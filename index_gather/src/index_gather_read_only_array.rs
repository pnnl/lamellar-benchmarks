use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use rand::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

// === Benchmark metadata utility ===
use benchmark_record::BenchmarkInformation;

const COUNTS_LOCAL_LEN: usize = 1_000_000;

// Load by random indices from a ReadOnlyArray
fn index_gather(array: &ReadOnlyArray<usize>, rand_index: OneSidedMemoryRegion<usize>) {
    let rand_slice = unsafe { rand_index.as_slice().expect("PE on world team") };
    array.batch_load(rand_slice).block();
}

fn main() {
    // --- world / args ---
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    // --- benchmark record ---
    let mut bench = BenchmarkInformation::with_name("index_gather_read_only_array");
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

    // --- array setup ---
    let global_count = COUNTS_LOCAL_LEN * num_pes;

    let unsafe_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Cyclic,
    )
    .block();

    // Force element type to avoid Range<usize> inference issues
    let rand_index = world.alloc_one_sided_mem_region::<usize>(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    // initialize arrays
    let array_init = unsafe {
        unsafe_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    };
    // Fill random indices (use rand 0.6-compatible signature)
    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(array_init);

    // Convert to read-only array
    let array = unsafe_array.into_read_only().block();
    world.barrier();

    if my_pe == 0 {
        println!("starting index gather (read-only array)");
    }

    // --- timing ---
    let now = Instant::now();
    index_gather(&array, rand_index);
    array.wait_all();
    world.barrier();

    let duration = now.elapsed().as_secs_f64();

    // --- metrics ---
    bench
        .output
        .insert("num_pes".into(), num_pes.to_string());
    bench
        .output
        .insert("global_time_secs".into(), format!("{:.6}", duration));

    let global_mups = ((l_num_updates * num_pes) as f64 / 1_000_000.0) / duration;
    bench
        .output
        .insert("MUPS".into(), format!("{:.6}", global_mups));

    let mb_sent = world.MB_sent();
    bench
        .output
        .insert("MB_sent".into(), format!("{:.6}", mb_sent));
    bench.output.insert(
        "MB_per_sec".into(),
        format!("{:.6}", mb_sent / duration),
    );
    bench.output.insert(
        "gb_s_injection_rate".into(),
        format!("{:.6}", (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / duration),
    );

    // --- build output path with git short hash ---
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

        // sanitize for filenames
        let safe_hash: String = short_hash.chars().filter(|c| c.is_ascii_alphanumeric()).collect();

        let file_name = format!("index_gather_read_only_array_{}.jsonl", safe_hash);
        base.push(file_name);
        base
    };

    if my_pe == 0 {
        if let Some(parent) = out_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        println!(
            "PE {my_pe}: Global time: {:.6}s, MUPS: {:.6} -> {:?}",
            duration, global_mups, out_path
        );
        bench.write(&out_path);
    }
}
