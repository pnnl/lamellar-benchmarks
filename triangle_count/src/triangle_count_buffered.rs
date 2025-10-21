use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use lamellar_graph::{Graph, GraphData, GraphType};

// ===== IMPORTS FOR JSON OUTPUT =====
use json::{object, JsonValue};
use std::fs::{self, OpenOptions}; //Only import what we need - conserving memory is critical
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::env;

// ===== HELPER FUNCTIONS - OUTPUT TO JSON =====
// Function to auto-detect version of Lamellar in Cargo.toml histo project and place the outputted JSON in that directory (call in append_json_line)
// returns it as a string
fn lamellar_version() -> Option<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let lock_path = format!("{}/Cargo.lock", manifest_dir);

    if let Ok(contents) = fs::read_to_string(lock_path) {
        for line in contents.lines() {
            if line.trim_start().starts_with("name = \"lamellar\"") {
                // next line will have version
                if let Some(version_line) = contents.lines().skip_while(|l| !l.contains("name = \"lamellar\"")).nth(1) {
                    if let Some(version) = version_line.split('=').nth(1) {
                        return Some(version.trim().trim_matches('"').to_string());
                    }
                }
            }
        }
    }
    None
}

// Create directory we are putting the JSON output in
// No input
// function that returns a PathBuf, a flexible object in Rust specifically made to store file paths, containing the outputs directory
fn one_level_up() -> PathBuf {
    let exe_dir = env::current_exe()
        // return option
        .ok()
        // if option returned (we have path), 
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    // define base as one directory behind the executable, then add the "outputs" folder
    // .parent() is the function which actually goes one directory behind
    let base = exe_dir.parent().unwrap_or(&exe_dir).to_path_buf();
    if let Some(ver) = lamellar_version() {
        base.join(format!("Outputs/{}", ver))
    } else {
        base.join("Outputs")
    }
}

// Function to append output to target file as JSON
// takes as input the name of the script as a mutable string and a mutable JsonValue object from the json crate
fn append_json_line(script_stem: &str, obj: &JsonValue) {
    let dir = one_level_up();
    // creating specific file for output directory as a variable that may or may not be used (_)
    let _ = fs::create_dir_all(&dir);
    // actually naming the output file
    let path = dir.join(format!("{script_stem}.json"));
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
        // DON'T WORRY - stringify is from the JSON crate, which converts it into valid JSON syntax- not a string!
        // EDIT: we need to clone this so that it works for json::stringify and (implements Into<JsonValue>) and &JsonValue doesn't
        let _ = writeln!(f, "{}", json::stringify(obj.clone()));
    }
 }


#[lamellar::AmLocalData]
struct LaunchAm {
    graph: Graph,
    start: u32,
    end: u32,
    final_cnt: AtomicArray<usize>, //Instead of Darc<AtomicUsize> (as in the non buffered version), we can also use a atomic array to keep track of the counts.
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
            buffer.push((node_0, neighs)); // pack the node and neighbors into the buffer
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
            //send the remaining data
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
            // this loop is not present in the non-buffered version
            for node_1 in neighbors.iter().filter(|n| self.graph.node_is_local(n)) {
                //check to make sure node_1 is local to this pe
                let neighs_1 = self
                    .graph
                    .neighbors_iter(node_1)
                    .take_while(|n| n < &node_1);
                cnt += BufferedTcAm::sorted_intersection_count(neighbors.iter(), neighs_1);
            }
        }
        self.final_cnt.local_data().at(0).fetch_add(cnt); //we only need to update our local portion of the count, and we know each pe only has a single element of the cnt array
    }
}

fn main() {
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

    // JSON accumulator
    let mut out = object! {
        "binary": "triangle_count_buffered",
        "my_pe": my_pe,
        "num_pes": num_pes
    };
    //this loads, reorders, and distributes the graph to all PEs
    let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());

    let final_cnt = AtomicArray::new(world.team(), world.num_pes(), Distribution::Block).block(); // convert it to an atomic array (which is accessible to all PEs)

    if my_pe == 0 {
        println!("num nodes {:?}", graph.num_nodes())
    };
    // this section of code creates and executes a number of "LaunchAMs" so that we
    // can use multiple threads to initiate the triangle counting active message.
    let batch_size = (graph.num_nodes() as f32) / (launch_threads as f32);

    for buf_size in [10, 100, 1000, 10000, 100000, 1000000].iter() {
        // for buf_size in [100000].iter() {
        if my_pe == 0 {
            println!("using buf_size: {:?}", buf_size);
        }
        world.barrier();
        let timer = std::time::Instant::now();
        let mut reqs = vec![];
        for tid in 0..launch_threads {
            let start = (tid as f32 * batch_size).round() as u32;
            let end = ((tid + 1) as f32 * batch_size).round() as u32;
            reqs.push(
                world
                    .exec_am_local(LaunchAm {
                        graph: graph.clone(),
                        start: start,
                        end: end,
                        final_cnt: final_cnt.clone(),
                        buf_size: *buf_size,
                    })
                    .spawn(),
            );
        }

        //we explicitly wait for all the LaunchAMs to finish so we can explicity calculate the issue time.
        // calling wait_all() here will block until all the AMs including the LaunchAMs and the TcAMs have finished.
        world.block_on(async move {
            for req in reqs {
                req.await;
            }
        });
        if my_pe == 0 {
            println!("issue time: {:?}", timer.elapsed().as_secs_f64())
        };
        // at this point all the triangle counting active messages have been initiated.

        world.wait_all(); //wait for all the triangle counting active messages to finish locally
        if my_pe == 0 {
            println!("local time: {:?}", timer.elapsed().as_secs_f64())
        };

        world.barrier(); //wait for all the triangle counting active messages to finish on all PEs

        let final_cnt_sum = world.block_on(final_cnt.sum()); //reduce the final count across all PEs
        if my_pe == 0 {
            let global_secs = timer.elapsed().as_secs_f64();
            println!(
                "triangles counted: {:?}\nglobal time: {:?}",
                final_cnt_sum,
                global_secs
            );
            // populate JSON and append
            out["buf_size"] = (*buf_size as u64).into();
            if let Some(sum) = final_cnt_sum {
                out["triangles_counted"] = (sum as u64).into();
            } else {
                out["triangles_counted"] = JsonValue::Null;
            }
            out["global_time_secs"] = global_secs.into();
            println!("{}", json::stringify(out.clone()));
            append_json_line("triangle_count_buffered", &out);
            println!();
        }
        world.block_on(final_cnt.dist_iter().for_each(|x| x.store(0))); //reset the final count array
    }
}
