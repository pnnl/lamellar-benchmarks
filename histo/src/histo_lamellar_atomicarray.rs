use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;

use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;

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


//===== HISTO BEGIN ======

fn histo(counts: &AtomicArray<usize>, rand_index: &ReadOnlyArray<usize>) {
    let _ = counts.batch_add(rand_index.local_data(), 1).spawn();
}

//===== HISTO END ======

const COUNTS_LOCAL_LEN: usize = 1000000; //100_000_000; //this will be 800MB on each pe
                                         // srun -N <num nodes> target/release/histo_lamellar_array <num updates>
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_count = COUNTS_LOCAL_LEN * num_pes;
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    // Accumulator object for JSON
    // our bucket we keep the json in
    let mut out = object! {
        "binary": "histo_lamellar_atomicarray",
        "my_pe": my_pe,
        "num_pes": num_pes
    };

    if my_pe == 0 {
        out["updates_total"] = (l_num_updates * num_pes).into();
        out["updates_per_pe"] = l_num_updates.into();
        out["table_size_per_pe"] = COUNTS_LOCAL_LEN.into();
    }

    let unsafe_counts = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Cyclic,
    );

    let rand_index = UnsafeArray::<usize>::new(
        world.team(),
        l_num_updates * num_pes,
        lamellar::array::Distribution::Block,
    );
    let rng: Arc<Mutex<StdRng>> = Arc::new(Mutex::new(SeedableRng::seed_from_u64(my_pe as u64)));

    let unsafe_counts = unsafe_counts.block();

    // initialize arrays
    let counts_init = unsafe { unsafe_counts.dist_iter_mut().for_each(|x| *x = 0) };
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...

    let rand_index = rand_index.block();
    unsafe {
        let mut rng = rng.lock();
        for elem in rand_index.local_as_mut_slice().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(counts_init);
    let counts = unsafe_counts.into_atomic().block();
    //counts.wait_all(); equivalent in this case to the above statement
    let rand_index = rand_index.into_read_only().block();
    world.barrier();

    let now = Instant::now();
    histo(&counts, &rand_index);
    if my_pe == 0 {
        out["issue_time"] = format!("{:?}", now.elapsed()).into();
    }
    counts.wait_all();
    let local_run = now.elapsed();
    if my_pe == 0 {
        out["local_run_time_secs"] = local_run.as_secs_f64().into();
        out["local_mups"] = ((l_num_updates as f64 / 1_000_000.0) / local_run.as_secs_f64()).into();
    }
    counts.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        let mb_sent = world.MB_sent();
        out["global_time_secs"] = global_time.into();
        out["MB_sent"] = mb_sent.into();
        out["MB_per_sec"] = (mb_sent / global_time).into();
        out["MUPS"] = (((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time).into();
        out["secs"] = global_time.into();
        out["gb_per_s_injection_rate"] = ((8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time).into();
    }
    // println!("pe {:?} sum {:?}", my_pe, world.block_on(counts.sum()));
    if my_pe == 0 {
        println!("{}", json::stringify(out.clone()));
        append_json_line("histo_lamellar_atomicarray", &out);
    }
}
