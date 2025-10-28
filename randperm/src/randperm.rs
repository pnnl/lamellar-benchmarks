use lamellar::array::prelude::*;
use rand::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

// === Benchmark metadata utility ===
use benchmark_record::BenchmarkInformation;

const DEFAULT_GLOBAL_COUNT: usize = 1000;
const DEFAULT_TARGET_FACTOR: usize = 10;

fn main() {
    // --- world / args ---
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    // args: <global_count> <target_factor>
    let global_count = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_GLOBAL_COUNT);

    let target_factor = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_TARGET_FACTOR);

    // --- benchmark record ---
    let mut bench = BenchmarkInformation::with_name("randperm");
    bench.parameters = args.clone();
    bench.output.insert("num_pes".into(), num_pes.to_string());
    bench.output.insert("global_count".into(), global_count.to_string());
    bench.output.insert("target_factor".into(), target_factor.to_string());
    bench.output.insert("table_size_total".into(), (global_count * target_factor).to_string());

    // --- array setup ---
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

    // Ensure arrays finish building
    let darts_array = darts_array.block();
    let target_array = target_array.block();

    // initialize arrays: darts holds 0..global_count, target is all MAX (empty)
    let darts_init = unsafe {
        darts_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    };
    let target_init = unsafe {
        target_array
            .dist_iter_mut()
            .for_each(|x| *x = usize::MAX)
    };
    world.block_on(darts_init);
    world.block_on(target_init);
    world.wait_all();

    // Convert to read-only for darts; atomic for target
    let darts_array = darts_array.into_read_only().block();
    let target_array = target_array.into_atomic().block();

    // local darts slice
    let local_darts = darts_array.local_data();

    world.barrier();
    let now = Instant::now();

    // ====== perform the actual random permute ====== //
    let rand_index = (0..local_darts.len())
        .map(|_| rng.gen_range(0, global_count * target_factor)) // rand 0.6 signature
        .collect::<Vec<usize>>();

    // launch initial set of darts, and collect any that didn't stick
    let mut remaining_darts = world
        .block_on(target_array.batch_compare_exchange(&rand_index, usize::MAX, local_darts))
        .iter()
        .enumerate()
        .filter_map(|(i, elem)| match elem {
            Ok(_val) => None,                   // dart stuck
            Err(_prev) => Some(local_darts[i]), // collision: try again
        })
        .collect::<Vec<usize>>();

    // continue launching remaining darts until they all stick
    while !remaining_darts.is_empty() {
        let rand_index = (0..remaining_darts.len())
            .map(|_| rng.gen_range(0, global_count * target_factor))
            .collect::<Vec<usize>>();

        remaining_darts = world
            .block_on(target_array.batch_compare_exchange(&rand_index, usize::MAX, &remaining_darts))
            .iter()
            .enumerate()
            .filter_map(|(i, elem)| match elem {
                Ok(_val) => None,
                Err(_prev) => Some(remaining_darts[i]),
            })
            .collect::<Vec<usize>>();
    }

    world.wait_all();
    world.barrier();

    let permute_secs = now.elapsed().as_secs_f64();

    // Collect: filter out MAX entries and gather to a ReadOnlyArray
    let collect_start = Instant::now();
    let the_array = world.block_on(
        target_array
            .dist_iter()
            .filter_map(|elem| {
                let elem = elem.load();
                if elem < usize::MAX { Some(elem) } else { None }
            })
            .collect::<ReadOnlyArray<usize>>(lamellar::array::Distribution::Block),
    );
    let collect_secs = collect_start.elapsed().as_secs_f64();

    // Global metrics
    let global_time = permute_secs; // total permute time measured above
    bench.output.insert("permute_time_secs".into(), format!("{:.6}", permute_secs));
    bench.output.insert("collect_time_secs".into(), format!("{:.6}", collect_secs));
    bench.output.insert("global_time_secs".into(), format!("{:.6}", global_time));

    let total_updates = global_count; // one write per element across PEs
    let mups = (total_updates as f64 / 1_000_000.0) / global_time.max(1e-12);
    bench.output.insert("MUPS".into(), format!("{:.6}", mups));

    let mb_sent = world.MB_sent();
    bench.output.insert("MB_sent".into(), format!("{:.6}", mb_sent));
    bench.output.insert("MB_per_sec".into(), format!("{:.6}", mb_sent / global_time.max(1e-12)));

    // optional correctness check
    if my_pe == 0 {
        if let Some(sum) = world.block_on(the_array.sum()) {
            let expected = (global_count * (global_count + 1) / 2) - global_count; // n(n-1)/2
            bench.output.insert("reduced_sum".into(), (sum as u64).to_string());
            bench.output.insert("expected_sum".into(), (expected as u64).to_string());
            bench.output.insert("sum_match".into(), (sum == expected).to_string());
        } else {
            bench.output.insert("reduced_sum".into(), "null".into());
            bench.output.insert("expected_sum".into(), "null".into());
            bench.output.insert("sum_match".into(), "false".into());
        }
    }

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

        let safe_hash: String = short_hash.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
        let file_name = format!("randperm_{}.jsonl", safe_hash);
        base.push(file_name);
        base
    };

    if my_pe == 0 {
        if let Some(parent) = out_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        println!(
            "PE {my_pe}: permute {:.6}s, collect {:.6}s, MUPS {:.6} -> {:?}",
            permute_secs, collect_secs, mups, out_path
        );
        bench.write(&out_path);
    }
}
