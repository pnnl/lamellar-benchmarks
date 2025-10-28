use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;

/* edits:
- removed json crate (use BenchmarkInformation crate only
- import BenchmarkInformation 
- add result_record into main 
- removed original json file-writing logic 
- Added function to retrieve short git hash */

use rand::prelude::*;
use std::future::Future;
use std::time::Instant;
use std::path::PathBuf;

// === Benchmark metadata utility ===
use benchmark_record::BenchmarkInformation;

const COUNTS_LOCAL_LEN: usize = 1_000_000; // this will be 800MB on each PE

// ===== INDEX_GATHER (Buffered AM) =====

#[lamellar::AmData(Clone, Debug)]
struct IndexGatherBufferedAM {
    buff: std::vec::Vec<usize>,
    counts: SharedMemoryRegion<usize>,
}

#[lamellar::am]
impl LamellarAM for IndexGatherBufferedAM {
    async fn exec(self) -> Vec<usize> {
        let counts_slice = unsafe { self.counts.as_slice().unwrap() };
        self.buff
            .iter()
            .map(|i| counts_slice[*i])
            .collect::<Vec<usize>>()
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: OneSidedMemoryRegion<usize>,
    counts: SharedMemoryRegion<usize>,
    buffer_amt: usize,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let num_pes = lamellar::num_pes;
        let mut buffs: std::vec::Vec<std::vec::Vec<usize>> =
            vec![Vec::with_capacity(self.buffer_amt); num_pes];
        let task_group = LamellarTaskGroup::new(lamellar::team.clone());

        for idx in unsafe { self.rand_index.as_slice().unwrap() } {
            let rank = idx % num_pes;
            let offset = idx / num_pes;

            buffs[rank].push(offset);
            if buffs[rank].len() >= self.buffer_amt {
                let buff = buffs[rank].clone();
                task_group
                    .exec_am_pe(
                        rank,
                        IndexGatherBufferedAM {
                            buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .await;
                buffs[rank].clear();
            }
        }

        // send any remaining buffered updates
        for rank in 0..num_pes {
            let buff = buffs[rank].clone();
            if !buff.is_empty() {
                task_group
                    .exec_am_pe(
                        rank,
                        IndexGatherBufferedAM {
                            buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .await;
            }
        }
    }
}

fn histo(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    rand_index: &OneSidedMemoryRegion<usize>,
    counts: &SharedMemoryRegion<usize>,
    buffer_amt: usize,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.sub_region(start..end),
            counts: counts.clone(),
            buffer_amt,
        }));
    }
    launch_tasks
}

// ===== MAIN =====

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let counts = world.alloc_shared_mem_region(COUNTS_LOCAL_LEN);
    let global_count = COUNTS_LOCAL_LEN * num_pes;

    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let buffer_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let num_threads = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });

    // === Initialize Benchmark Record ===
    let mut bench = BenchmarkInformation::with_name("index_gather_buffered_am");
    bench.parameters = args.clone();
    bench
        .output
        .insert("updates_total".into(), (l_num_updates * num_pes).to_string());
    bench
        .output
        .insert("updates_per_pe".into(), l_num_updates.to_string());
    bench
        .output
        .insert("table_size_per_pe".into(), COUNTS_LOCAL_LEN.to_string());

    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    let counts = counts.block();

    unsafe {
        for elem in counts.as_mut_slice().unwrap().iter_mut() {
            *elem = 0;
        }
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            // NOTE: rand 0.8+ API
            *elem = rng.gen_range(0, global_count);
        }
    }

    // === Execute benchmark ===
    world.barrier();
    let now = Instant::now();

    let launch_tasks = histo(
        l_num_updates,
        num_threads,
        &world,
        &rand_index,
        &counts,
        buffer_amt,
    );

    world.block_on(async move {
        for task in launch_tasks {
            task.await;
        }
    });

    world.wait_all();
    world.barrier();

    let global_time = now.elapsed().as_secs_f64();

    // === Collect metrics ===
    bench
        .output
        .insert("num_pes".into(), num_pes.to_string());
    bench
        .output
        .insert("num_threads".into(), num_threads.to_string());
    bench
        .output
        .insert("global_execution_time_secs".into(), format!("{:.6}", global_time));

    let global_mups = ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time;
    bench
        .output
        .insert("MUPS".into(), format!("{:.6}", global_mups));

    let mb_sent = world.MB_sent();
    bench
        .output
        .insert("MB_sent".into(), format!("{:.6}", mb_sent));
    bench.output.insert(
        "MB_per_sec".into(),
        format!("{:.6}", mb_sent / global_time),
    );
    bench.output.insert(
        "gb_s_injection_rate".into(),
        format!("{:.6}", (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time),
    );

    // Optional: sanity metric (sum of counts)
    let pe_sum: u64 = unsafe { counts.as_slice().unwrap().iter().sum::<usize>() as u64 };
    bench.output.insert("pe_sum".into(), pe_sum.to_string());

    // === Build output path with git short hash ===
    let out_path: PathBuf = {
        let mut base = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

        let short_hash = bench
            .git
            .get("short_hash")
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());

        let safe_hash: String = short_hash
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();

        let file_name = format!("index_gather_buffered_am_{}.jsonl", safe_hash);
        base.push(file_name);
        base
    };

    if my_pe == 0 {
        if let Some(parent) = out_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        println!(
            "PE {my_pe}: Global time: {:.6}s, MUPS: {:.6} -> {:?}",
            global_time, global_mups, out_path
        );
        bench.write(&out_path);
    }
}
