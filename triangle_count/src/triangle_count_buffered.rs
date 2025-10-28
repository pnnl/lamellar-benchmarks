use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use lamellar_graph::{Graph, GraphData, GraphType};

use std::path::PathBuf;
use std::time::Instant;

// === Structured benchmark logging ===
use benchmark_record::BenchmarkInformation;

#[lamellar::AmLocalData]
struct LaunchAm {
    graph: Graph,
    start: u32,
    end: u32,
    final_cnt: AtomicArray<usize>, // count per-PE (one slot per PE)
    buf_size: usize,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec() {
        let task_group = LamellarTaskGroup::new(lamellar::world.clone());
        let graph_data = self.graph.data();
        let mut buffer = vec![];
        let mut cur_len = 0;

        for node_0 in (self.start..self.end).filter(|n| self.graph.node_is_local(n)) {
            let neighs = graph_data
                .neighbors_iter(&node_0)
                .take_while(|n| n < &&node_0)
                .map(|n| *n)
                .collect::<Vec<u32>>();
            cur_len += neighs.len();
            buffer.push((node_0, neighs)); // pack node + neighbors
            if cur_len > self.buf_size {
                let _ = task_group
                    .exec_am_all(BufferedTcAm {
                        graph: graph_data.clone(),
                        data: buffer,
                        final_cnt: self.final_cnt.clone(),
                    })
                    .spawn();
                buffer = vec![];
                cur_len = 0;
            }
        }

        if cur_len > 0 {
            // send remaining
            let _ = task_group
                .exec_am_all(BufferedTcAm {
                    graph: graph_data.clone(),
                    data: buffer,
                    final_cnt: self.final_cnt.clone(),
                })
                .spawn();
        }

        task_group.await_all().await;
    }
}

#[lamellar::AmData]
struct BufferedTcAm {
    graph: Darc<GraphData>,
    data: Vec<(u32, Vec<u32>)>,
    final_cnt: AtomicArray<usize>,
}

impl BufferedTcAm {
    fn sorted_intersection_count<'a>(
        set0: impl Iterator<Item = &'a u32> + Clone,
        mut set1: impl Iterator<Item = &'a u32> + Clone,
    ) -> usize {
        let mut count = 0;
        if let Some(mut node_1) = set1.next() {
            for node_0 in set0 {
                while node_1 < node_0 {
                    node_1 = match set1.next() {
                        Some(node_1) => node_1,
                        None => return count,
                    };
                }
                if node_0 == node_1 {
                    count += 1;
                }
            }
        }
        count
    }
}

#[lamellar::am]
impl LamellarAM for BufferedTcAm {
    async fn exec() {
        let mut cnt = 0;
        for (_node_0, neighbors) in &self.data {
            // loop over neighbors that are local to this PE
            for node_1 in neighbors.iter().filter(|n| self.graph.node_is_local(n)) {
                let neighs_1 = self
                    .graph
                    .neighbors_iter(node_1)
                    .take_while(|n| n < &node_1);
                cnt += BufferedTcAm::sorted_intersection_count(neighbors.iter(), neighs_1);
            }
        }
        // one element per PE: update local slot
        self.final_cnt.local_data().at(0).fetch_add(cnt);
    }
}

fn main() {
    // --- args / world -------------------------------------------------------
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];
    let launch_threads = if args.len() > 2 {
        match &args[2].parse::<usize>() {
            Ok(x) => *x,
            Err(_) => 2,
        }
    } else {
        2
    };

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    // --- graph & counters ---------------------------------------------------
    let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());
    let final_cnt = AtomicArray::new(world.team(), world.num_pes(), Distribution::Block).block();

    if my_pe == 0 {
        println!("num nodes {:?}", graph.num_nodes());
    }

    // --- output path with git short hash (once per run) ---------------------
    // Build it once; we'll append one JSONL line per buf_size.
    let out_path: PathBuf = {
        let mut base = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

        // create a temp bench just to read git; or you can build it outside and reuse
        let temp_bench = BenchmarkInformation::with_name("triangle_count_buffered");
        let short_hash = temp_bench
            .git
            .get("short_hash")
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        let safe_hash: String = short_hash.chars().filter(|c| c.is_ascii_alphanumeric()).collect();

        base.push(format!("triangle_count_buffered_{}.jsonl", safe_hash));
        base
    };
    if my_pe == 0 {
        if let Some(parent) = out_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
    }

    // --- per-thread batch size (same as original) ---------------------------
    let batch_size = (graph.num_nodes() as f32) / (launch_threads as f32);

    // --- main loop over buffer sizes (preserved) ----------------------------
    for buf_size in [10usize, 100, 1000, 10000, 100000, 1000000].iter().copied() {
        if my_pe == 0 {
            println!("using buf_size: {:?}", buf_size);
        }

        // fresh record per buf_size (one JSON line per iteration)
        let mut bench = BenchmarkInformation::with_name("triangle_count_buffered");
        bench.parameters = args.clone();
        bench.output.insert("num_pes".into(), num_pes.to_string());
        bench
            .output
            .insert("launch_threads".into(), launch_threads.to_string());
        bench.output.insert("buf_size".into(), buf_size.to_string());
        bench
            .output
            .insert("num_nodes".into(), graph.num_nodes().to_string());

        world.barrier();
        let timer = Instant::now();

        // spawn LaunchAMs
        let mut reqs = vec![];
        for tid in 0..launch_threads {
            let start = (tid as f32 * batch_size).round() as u32;
            let end = ((tid + 1) as f32 * batch_size).round() as u32;
            reqs.push(
                world
                    .exec_am_local(LaunchAm {
                        graph: graph.clone(),
                        start,
                        end,
                        final_cnt: final_cnt.clone(),
                        buf_size,
                    })
                    .spawn(),
            );
        }

        // wait for LaunchAMs to finish (issue time)
        world.block_on(async move {
            for req in reqs {
                req.await;
            }
        });

        let issue_secs = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!("issue time: {:.6}", issue_secs);
        }
        bench
            .output
            .insert("issue_time_secs".into(), format!("{:.6}", issue_secs));

        // wait for local completion
        world.wait_all();
        let local_secs = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!("local time: {:.6}", local_secs);
        }
        bench
            .output
            .insert("local_time_secs".into(), format!("{:.6}", local_secs));

        // global completion
        world.barrier();
        let final_cnt_sum = world.block_on(final_cnt.sum()); // reduce count across PEs

        let global_secs = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!(
                "triangles counted: {:?}\nglobal time: {:.6}",
                final_cnt_sum, global_secs
            );
        }
        bench
            .output
            .insert("global_time_secs".into(), format!("{:.6}", global_secs));

        // record triangle count (or null)
        if let Some(sum) = final_cnt_sum {
            bench
                .output
                .insert("triangles_counted".into(), (sum as u64).to_string());
        } else {
            bench.output.insert("triangles_counted".into(), "null".into());
        }

        // network stats
        let mb_sent = world.MB_sent();
        bench
            .output
            .insert("MB_sent".into(), format!("{:.6}", mb_sent));
        bench.output.insert(
            "MB_per_sec".into(),
            format!("{:.6}", mb_sent / global_secs.max(1e-12)),
        );

        // write one JSONL line (only PE 0)
        if my_pe == 0 {
            bench.write(&out_path);
            println!();
        }

        // reset the counter array for the next buf_size
        world.block_on(final_cnt.dist_iter().for_each(|x| x.store(0)));
    }
}
