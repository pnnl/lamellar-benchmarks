use lamellar::array::prelude::*;
use rand::prelude::*;
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

fn main() {
    // Accumulator object for JSON will be created after world and PE info is known

    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    // Accumulator object for JSON
    // our bucket we keep the json in
    let mut out = object! {
        "binary": "randperm",
        "my_pe": my_pe,
        "num_pes": num_pes
    };
    let global_count = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000); //size of permuted array
    let target_factor = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10); //multiplication factor for target array

    if my_pe == 0 {
        /*println!("array size {}", global_count);
        println!("target array size {}", global_count * target_factor);*/
    }

    // start with unsafe because they are faster to initialize than AtomicArrays
    let darts_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Block,
    );
    let target_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count * target_factor,
        lamellar::array::Distribution::Block,
    );
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    //Ensure all arrays finish building...
    let darts_array = darts_array.block();
    let target_array = target_array.block();

    // initialize arrays
    let darts_init = unsafe {
        darts_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    }; // each PE some slice in [0..global_count]
    let target_init = unsafe { target_array.dist_iter_mut().for_each(|x| *x = usize::MAX) };
    world.block_on(darts_init);
    world.block_on(target_init);
    world.wait_all();

    let darts_array = darts_array.into_read_only().block();
    let target_array = target_array.into_atomic().block();
    let local_darts = darts_array.local_data(); //will use this slice for first iteration

    world.barrier();
    if my_pe == 0 {
        //println!("start");
    }
    let now = Instant::now();

    // ====== perform the actual random permute========//
    let rand_index = (0..local_darts.len())
        .map(|_| rng.gen_range(0, global_count * target_factor))
        .collect::<Vec<usize>>();

    // launch initial set of darts, and collect any that didnt stick
    let mut remaining_darts = world
        .block_on(target_array.batch_compare_exchange(&rand_index, usize::MAX, local_darts))
        .iter()
        .enumerate()
        .filter_map(|(i, elem)| {
            match elem {
                Ok(_val) => None,               //the dart stuck!
                Err(_) => Some(local_darts[i]), //something else was there, try again
            }
        })
        .collect::<Vec<usize>>();

    // continue launching remaining darts until they all stick
    while !remaining_darts.is_empty() {
        let rand_index = (0..remaining_darts.len())
            .map(|_| rng.gen_range(0, global_count * target_factor))
            .collect::<Vec<usize>>();
        remaining_darts = world
            .block_on(target_array.batch_compare_exchange(
                &rand_index,
                usize::MAX,
                remaining_darts.clone(),
            ))
            .iter()
            .enumerate()
            .filter_map(|(i, elem)| {
                match elem {
                    Ok(_val) => None,                   //the dart stuck!
                    Err(_) => Some(remaining_darts[i]), //something else was there, try again
                }
            })
            .collect::<Vec<usize>>();
    }
    world.wait_all(); //my work is done
    if my_pe == 0 {
        //println!("local run time {:?} ", now.elapsed(),);
    }
    world.barrier(); //all work is done
    if my_pe == 0 {
        //println!("permute time {:?} ", now.elapsed(),);
    }
    let collect_start = Instant::now();
    let the_array = world.block_on(
        target_array
            .dist_iter()
            .filter_map(|elem| {
                let elem = elem.load(); //elements are atomic so we cant just read directly
                if elem < usize::MAX {
                    Some(elem)
                } else {
                    None
                }
            })
            .collect::<ReadOnlyArray<usize>>(lamellar::array::Distribution::Block),
    ); //need to work on collect performance from within the runtime
       // =============================================================//

    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        /*println!("collect time: {:?}", collect_start.elapsed());
        println!(
            "global time {:?} MB {:?} MB/s: {:?} ",
            global_time,
            (world.MB_sent()),
            (world.MB_sent()) / global_time,
        );
        println!("Secs: {:?}", global_time,);
        if let Some(sum) = world.block_on(the_array.sum()) {
            println!(
                "reduced sum: {sum} calculated sum {} ",
                (global_count * (global_count + 1) / 2) - global_count
            );
            if sum != (global_count * (global_count + 1) / 2) - global_count {
                println!("Error! randperm not as expected");
            }
        } else {
            println!("Error! randperm computation failed");
        }*/
        // populate JSON output with metrics
    out["global_time_secs"] = global_time.into();
    let mb_sent = world.MB_sent();
    out["MB_sent"] = mb_sent.into();
    out["MB_per_sec"] = (mb_sent / global_time).into();
    // compute collect time and permute time explicitly
    let collect_secs = collect_start.elapsed().as_secs_f64();
    out["collect_time_secs"] = collect_secs.into();
    // permute time is the time spent before collection
    let permute_secs = global_time - collect_secs;
    out["permute_time_secs"] = permute_secs.into();

        if let Some(sum) = world.block_on(the_array.sum()) {
            out["reduced_sum"] = (sum as u64).into();
            let expected = (global_count * (global_count + 1) / 2) - global_count;
            out["expected_sum"] = (expected as u64).into();
            out["sum_match"] = (sum == expected).into();
        } else {
            out["reduced_sum"] = JsonValue::Null;
            out["expected_sum"] = JsonValue::Null;
            out["sum_match"] = false.into();
        }

        // print and append JSON line to Outputs directory
        println!("{}", json::stringify(out.clone()));
        append_json_line("randperm", &out);
    }
}
