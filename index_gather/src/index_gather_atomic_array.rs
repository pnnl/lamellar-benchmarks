use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use rand::prelude::*;
use std::time::Instant;
use std::path::PathBuf;


use benchmark_record::BenchmarkInformation;

const COUNTS_LOCAL_LEN: usize = 1_000_000;

fn index_gather(array: &AtomicArray<usize>, rand_index: OneSidedMemoryRegion<usize>) {
    let rand_slice = unsafe { rand_index.as_slice().expect("PE on world team") };
    array.batch_load(rand_slice).block();
}

fn main() {
    // === Initialize Lamellar World ===
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    // === Initialize Benchmark Record ===
    let mut bench = BenchmarkInformation::new();
    bench.parameters = args.clone(); // no need to keep track of this here - use the object we already have to avoid cloning
    bench.output.insert(
        "updates_total".to_string(),
        (l_num_updates * num_pes).to_string(),
    );

    let global_count = COUNTS_LOCAL_LEN * num_pes;

    // === Initialize Arrays ===
    let unsafe_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Cyclic,
    )
    .block();

    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count); // was (0, global_count)
        }
    }

    let array_init = unsafe {
        unsafe_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    };
    world.block_on(array_init);

    let array = unsafe_array.into_atomic().block();
    world.barrier();

    if my_pe == 0 {
        println!("starting index gather");
    }

    // === Benchmark timing ===
    let now = Instant::now();
    index_gather(&array, rand_index);
    array.wait_all();
    world.barrier();

    let duration = now.elapsed().as_secs_f64();

    // === Collect Results ===
    bench.output.insert("num_pes".to_string(), num_pes.to_string());
    bench.output.insert("updates_per_pe".to_string(), l_num_updates.to_string());
    bench.output
        .insert("global_time_secs".to_string(), format!("{:.4}", duration));

    let global_mups =
        ((l_num_updates * num_pes) as f64 / 1_000_000.0) / duration;
    bench.output
        .insert("MUPS".to_string(), format!("{:.3}", global_mups));

    let mb_sent = world.MB_sent();
    bench.output
        .insert("MB_sent".to_string(), format!("{:.3}", mb_sent));
    bench.output.insert(
        "MB_per_sec".to_string(),
        format!("{:.3}", mb_sent / duration),
    );
    bench.output.insert(
        "gb_s_injection_rate".to_string(),
        format!("{:.3}", (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / duration), // just take the value and do it as a .tostring
// no need to format this 
    );

    // === Output and Save ===
    println!("Ouptutting to file");
    // === Building Output File ===
    let out_path: PathBuf = {
        // put the file next to the binary; tweak as you like
        let mut base = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    // make a new copy of the short_hash to avoid ownership issues
        let short_hash  = bench.git.get("short_hash").cloned().unwrap_or_default();
        let file_name = format!("index_gather_atomic_array_{}.jsonl", short_hash);
        base.push(&file_name);
        base
    };

    if my_pe == 0 {
        println!("Global time: {:.3}s, MUPS: {:.3}", duration, global_mups);
        //result_record.write(&benchmark_record::default_output_path()); 
        bench.write(&out_path);
    }
}