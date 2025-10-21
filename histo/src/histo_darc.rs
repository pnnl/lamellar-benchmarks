// this code sets up a distributed counter array - a 'histogram' (like all other codes)
// It builds a vector of atomic counters that’s shared across all processes/PEs using Lamellar’s Darc (a distributed, ref-counted container).
// Then, each PE creates a list of random indices into the histogram.
// For each random index, the program sends a Lamellar active message to the PE responsible for that bin, 
// asking it to atomically increment the counter at that offset. This simulates a typical irregular, communication-heavy HPC workload.
// Then it measures the local run time, global run time, and MUPs.
// Tests: communication between nodes, synchronization, and throughput. 

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use rand::prelude::*;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ===== IMPORTS FOR JSON OUTPUT =====
use json::{object, JsonValue};
use std::fs::{self, OpenOptions}; //Only import what we need - conserving memory is critical
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::env;


const COUNTS_LOCAL_LEN: usize = 10000000;

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


//===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct HistoAM {
    offset: usize,
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::am]
impl LamellarAM for HistoAM {
    async fn exec(self) {
        self.counts[self.offset].fetch_add(1, Ordering::Relaxed);
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: Vec<usize>,
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        for idx in &self.rand_index {
            let rank = idx % lamellar::num_pes;
            let offset = idx / lamellar::num_pes;
            let _ = lamellar::world
                .exec_am_pe(
                    rank,
                    HistoAM {
                        offset: offset,
                        counts: self.counts.clone(),
                    },
                )
                .spawn();
        }
    }
}

fn histo(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    mut rand_index: Vec<usize>,
    counts: &Darc<Vec<AtomicUsize>>,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        let split_index = rand_index.len() - (end - start);
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.split_off(split_index),
            counts: counts.clone(),
        }));
    }
    launch_tasks
}

//===== HISTO END ======

fn main() {

    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

// Accumulator object for JSON
// our bucket we keep the json in
    let mut out = object! {
        "binary": "histo_darc",
        "my_pe": my_pe,
        "num_pes": num_pes
    };
    let global_count = COUNTS_LOCAL_LEN * num_pes;
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let mut counts_data = Vec::with_capacity(COUNTS_LOCAL_LEN);
    for _ in 0..COUNTS_LOCAL_LEN {
        counts_data.push(AtomicUsize::new(0));
    }
    let counts = Darc::new(&world, counts_data)
        .block()
        .expect("unable to create darc");
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let rand_index = (0..l_num_updates)
        .into_iter()
        .map(|_| rng.gen_range(0, global_count))
        .collect::<Vec<usize>>();

    //create multiple launch tasks, that iterated through portions of rand_index in parallel
    let num_threads = match std::env::var("LAMELLAR_THREADS") {
        Ok(n) => n.parse::<usize>().unwrap(),
        Err(_) => 1,
    };
    let num_threads = std::cmp::max(num_threads / 2, 1);
    world.barrier();
    let now = Instant::now();
    let launch_tasks = histo(l_num_updates, num_threads, &world, rand_index, &counts);

    if my_pe == 0 {
        //println!("{:?} issue time {:?} ", my_pe, now.elapsed());
        out["issue_time"] = format!("{:?}", now.elapsed()).into();
        // If we also want to store the pe for this output: out["my_pe"] = my_pe.into();
    }
    world.block_on(async move {
        for task in launch_tasks {
            task.await;
        }
    });
    if my_pe == 0 {
        //println!("{:?} launch task time {:?} ", my_pe, now.elapsed(),);
        out["issue_time"] = format!("{:?}", now.elapsed()).into();
    }
    world.wait_all();

    if my_pe == 0 {
        /*println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()

        );*/
    // return local run time in seconds:
    out["local_run_time"] = now.elapsed().as_secs_f64().into();


    }
    world.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        /*println!(
            "MUPS: {:?}",
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
        );*/

        let total_updates = (l_num_updates * num_pes) as f64;
        let global_mups = (total_updates / 1_000_000.0) / global_time;
        out["MUPS"] = global_mups.into();  
    
    }
    if my_pe == 0 {
        /*println!(
            "{:?} global time {:?} MB {:?} MB/s: {:?} global mups: {:?} ",
            my_pe,
            global_time,
            world.MB_sent(),
            world.MB_sent() / global_time,
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
        );*/

        // MB_sent() is assumed to be f64-like; cast if needed
        let mb_sent = world.MB_sent();
        let mb_per_sec = mb_sent / global_time;

        out["my_pe"] = my_pe.into();
        out["MB_sent"] = mb_sent.into();
        out["MB_per_sec"] = mb_per_sec.into();

        // Re-state global_mups under the combined “global line” if you want it grouped
        // (Remove if redundant with above.)
        let total_updates = (l_num_updates * num_pes) as f64;
        out["global_mups_line"] = ((total_updates / 1_000_000.0) / global_time).into();
    }

    println!(
        "pe {:?} sum {:?}",
        my_pe,
        counts
            .iter()
            .map(|e| e.load(Ordering::Relaxed))
            .sum::<usize>()
    );

    // Append to json file
    if my_pe == 0 {
        println!("{}", json::stringify(out.clone()));
        append_json_line("histo_darc", &out);
    }

}
