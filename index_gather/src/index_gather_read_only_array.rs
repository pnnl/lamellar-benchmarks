use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use rand::prelude::*;
use std::time::Instant;

// ===== IMPORTS FOR JSON OUTPUT =====
use json::{object, JsonValue};
use std::fs::{self, OpenOptions}; //Only import what we need - conserving memory is critical
use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::env;

fn index_gather(array: &ReadOnlyArray<usize>, rand_index: OneSidedMemoryRegion<usize>) {
    let rand_slice = unsafe { rand_index.as_slice().expect("PE on world team") }; // Safe as we are the only consumer of this mem region
    array.batch_load(rand_slice).block();
}

// ===== LAMELLAR VERSIONS =====
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

const COUNTS_LOCAL_LEN: usize = 1000000; //this will be 800MBB on each pe
                                         // srun -N <num nodes> target/release/histo_lamellar_array <num updates>
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_count = COUNTS_LOCAL_LEN * num_pes;
    // Accumulator object for JSON
    // our bucket we keep the json in 

    let mut out = object! {
        "binary": "index_gather_read_only_array",
        "my_pe": my_pe,
        "num_pes": num_pes
    };

    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    if my_pe == 0 {
        /* println!("updates total {}", l_num_updates * num_pes);
        println!("updates per pe {}", l_num_updates);
        println!("table size per pe{}", COUNTS_LOCAL_LEN); */
    }

    let unsafe_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Cyclic,
    )
    .block();
    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    // initialize arrays
    let array_init = unsafe {
        unsafe_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    };
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...
    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(array_init);
    let array = unsafe_array.into_read_only().block();
    // let rand_index = rand_index.into_read_only();
    world.barrier();

    if my_pe == 0 {
        println!("starting index gather");
    }

    let now = Instant::now();
    index_gather(&array, rand_index);

    if my_pe == 0 {
        out["issue_time"] = format!("{:?}", now.elapsed()).into();
        // println!("{:?} issue time {:?} ", my_pe, now.elapsed());
    }
    array.wait_all();
    if my_pe == 0 {
        /* println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        ); */
    }
    array.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        out["my_pe"] = my_pe.into();
        out["global_time_secs"] = global_time.into();
        out["MB_sent"] = world.MB_sent().into();
        out["MB_per_sec"] = (world.MB_sent() / global_time).into();
        out["global_mups_line"] = (((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time).into();
        out["secs"] = global_time.into();
        out["gb_s_injection_rate"] = ((8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time).into();

        // print and append JSON line
        println!("{}", json::stringify(out.clone()));
        append_json_line("index_gather_read_only_array", &out);

        /* 
        println!(
            "global time {:?} MB {:?} MB/s: {:?}",
            global_time,
            (world.MB_sent()),
            (world.MB_sent()) / global_time,
        );
        println!(
            "MUPS: {:?}",
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time,
        );
        println!("Secs: {:?}", global_time,);
        println!(
            "GB/s Injection rate: {:?}",
            (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time,
        ); */
    }
}
