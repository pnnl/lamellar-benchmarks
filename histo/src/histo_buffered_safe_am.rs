use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;

use rand::prelude::*;
use std::time::Instant;

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

// ===== IMPORTS FOR JSON OUTPUT =====
use json::{object, JsonValue};
use std::fs::{self, OpenOptions}; //Only import what we need - conserving memory is critical
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::env;
use std::process::Command;

const COUNTS_LOCAL_LEN: usize = 1000000; //100_000_000; //this will be 800MB on each pe

// Helper functions for JSON output - can be used across all executable .rs scripts

// get commit from lamellar
/*
IMPORTANT: When using this for a specific version of lamellar, open the Cargo.toml file and set the lamellar dependency to:

lamellar = { git = "https://github.com/pnnl/lamellar-runtime", rev = "[COMMIT HASH]" }

This will set the version of lamellar that our benchmarks are running off of to the codebase specifically at that commit.

It returns it as a string, so we can add it to the end of our .json file for each commit.
 */
fn get_lamellar_commit() -> String {
    // For build.rs --> we can set the specific commit in a build.rs file as well. In case we do this in the future, set is as LAMELLAR_COMMIT in this
    // build.rs file.
    if let Ok(s) = std::env::var("LAMELLAR_COMMIT") {
        return s;
    }

    // find metadata for the version of lamellar we need (contains commit hash)
    // this spawns a subprocess that runs: cargo metadata --format-version=1
    // (metadata pulled from the cargo.toml & cargo.lock, which has the specific rev)
    let out = Command::new("cargo")
        .args(["metadata", "--format-version=1"])
        .output();

    // parse metadata
    let Ok(out) = out else { return "unknown".to_string() };
    let stdout = String::from_utf8_lossy(&out.stdout);
    let Ok(meta) = json::parse(&stdout) else { return "unknown".to_string() };

    // Find the lamellar package and parse its "source":"git+...#<sha>" in the metadata
    for pkg in meta["packages"].members() {
        if pkg["name"] == "lamellar" {
            // extract hash - each dependency's source field cotains something like: "git+https://github.com/pnnl/lamellar-runtime#4b3d2f1"
            if let Some(src) = pkg["source"].as_str() {
                // split along the hash # so we get the number 
                if let Some(hash) = src.split('#').last() {
                    // sanitize to hex-only (defensive)
                    let cleaned: String = hash.chars()
                        .filter(|c| c.is_ascii_hexdigit())
                        .collect();
                    if !cleaned.is_empty() { return cleaned; }
                }
            }
        }
    }
    // fallback in case we don't find anything
    "unknown".to_string()
}



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
    let lamellar_commit = get_lamellar_commit();
    let path = dir.join(format!("{script_stem}_{}.json", lamellar_commit));
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) {
        // DON'T WORRY - stringify is from the JSON crate, which converts it into valid JSON syntax- not a string!
        // EDIT: we need to clone this so that it works for json::stringify and (implements Into<JsonValue>) and &JsonValue doesn't
        let _ = writeln!(f, "{}", json::stringify(obj.clone()));
    }
 }




// ===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct HistoBufferedAM {
    buff: std::vec::Vec<usize>,
    counts: SharedMemoryRegion<usize>,
}

#[lamellar::am]
impl LamellarAM for HistoBufferedAM {
    async fn exec(self) {
        // cast the shared memory region from usize to atomicusize
        let slice = unsafe {
            let slice = self.counts.as_mut_slice().unwrap();
            let slice_ptr = slice.as_mut_ptr() as *mut AtomicUsize;
            std::slice::from_raw_parts_mut(slice_ptr, slice.len())
        };
        for o in &self.buff {
            slice[*o].fetch_add(1, Ordering::Relaxed);
        }
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
        // let now = Instant::now();
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
                let _ = task_group
                    .exec_am_pe(
                        rank,
                        HistoBufferedAM {
                            buff: buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .spawn();
                buffs[rank].clear();
            }
        }
        //send any remaining buffered updates
        for rank in 0..num_pes {
            let buff = buffs[rank].clone();
            if buff.len() > 0 {
                let _ = task_group
                    .exec_am_pe(
                        rank,
                        HistoBufferedAM {
                            buff: buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .spawn();
            }
        }

        task_group.await_all().await;
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
            buffer_amt: buffer_amt,
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
        "binary": "histo_buffered_safe_am",
        "my_pe": my_pe,
        "num_pes": num_pes
    };

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

    if my_pe == 0 {
        // println!("updates total {}", l_num_updates * num_pes);
        out["updates_total"] = (l_num_updates * num_pes).into();
        // println!("updates per pe {}", l_num_updates);
        out["updates_per_pe"] = l_num_updates.into();
        //println!("table size per pe{}", COUNTS_LOCAL_LEN);
        out["table_size_per_pe"] = COUNTS_LOCAL_LEN.into();
    }

    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let counts = counts.block();

    unsafe {
        for elem in counts.as_mut_slice().unwrap().iter_mut() {
            *elem = 0;
        }
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
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

    if my_pe == 0 {
        //println!("{:?} issue time {:?} ", my_pe, now.elapsed(),);
        out["issue_time_secs"] = now.elapsed().as_secs_f64().into();
    }
    world.block_on(async move {
        for task in launch_tasks {
            task.await;
        }
    });
    if my_pe == 0 {
        println!("{:?} launch task time {:?} ", my_pe, now.elapsed(),);
        out["launch_task_time_secs"] = now.elapsed().as_secs_f64().into();
    }
    world.wait_all();
    if my_pe == 0 {
        /*println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        ); */
        out["local_run_time_secs"] = now.elapsed().as_secs_f64().into();
        out["local_mups"] = ((l_num_updates as f64 / 1_000_000.0) / now.elapsed().as_secs_f64()).into();
    }
    world.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        /*println!(
            "MUPS: {:?}",
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
        );*/
        out["MUPS"] = (((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time).into();

        //println!("Secs: {:?}", global_time,);
        out["secs"] = global_time.into();

        /*println!(
            "GB/s Injection rate: {:?}",
            (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time,
        );*/
        out["gb_per_s_injection_rate"] = ((8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time).into();

    }

    if my_pe == 0 {
        /*println!(
            "{:?} global time {:?} MB {:?} MB/s: {:?} global mups: {:?} (({l_num_updates}*{num_pes})/1_000_000) ",
            my_pe,
            global_time,
            world.MB_sent(),
            world.MB_sent() / global_time,
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time,

        );*/
        out["global_time_secs"] = global_time.into();
        out["MB_sent"] = world.MB_sent().into();
        out["MB_per_s"] = (world.MB_sent() / global_time).into();
        out["global_mups"] = (((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time).into();

    }

    /*println!("pe {:?} sum {:?}", my_pe, unsafe {
        counts.as_slice().unwrap().iter().sum::<usize>()
    });*/

    out["pe_sum"] = (unsafe { counts.as_slice().unwrap().iter().sum::<usize>() } as u64).into();

    // append to our JSON file
    if my_pe == 0 {
        println!("{}", json::stringify(out.clone()));
        append_json_line("histo_buffered_safe_am", &out);
    }

    // use this if you want to track where the benchmark data was stored:
    // final_filepath = one_level_up();
    // println!("Recorded in: {final_filepath}");

}

