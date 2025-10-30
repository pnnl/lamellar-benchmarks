use lamellar::array::prelude::*;
use rand::prelude::*;
use std::time::Instant;
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
    let mut bench = BenchmarkInformation::new();
    bench.with_output("num_pes", num_pes.to_string());
    bench.with_output("global_count", global_count.to_string());
    bench.with_output("target_factor", target_factor.to_string());
    bench.with_output("table_size_total", (global_count * target_factor).to_string());

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
    bench.with_output("permute_time_secs", format!("{:.6}", permute_secs));
    bench.with_output("collect_time_secs", format!("{:.6}", collect_secs));
    bench.with_output("global_time_secs", format!("{:.6}", global_time));

    let total_updates = global_count; // one write per element across PEs
    let mups = (total_updates as f64 / 1_000_000.0) / global_time.max(1e-12);
    bench.with_output("MUPS", format!("{:.6}", mups));

    let mb_sent = world.MB_sent();
    bench.with_output("MB_sent", format!("{:.6}", mb_sent));
    bench.with_output("MB_per_sec", format!("{:.6}", mb_sent / global_time.max(1e-12)));

    // optional correctness check
    if my_pe == 0 {
        if let Some(sum) = world.block_on(the_array.sum()) {
            let expected = (global_count * (global_count + 1) / 2) - global_count; // n(n-1)/2
            bench.with_output("reduced_sum", (sum as u64).to_string());
            bench.with_output("expected_sum", (expected as u64).to_string());
            bench.with_output("sum_match", (sum == expected).to_string());
        } else {
            bench.with_output("reduced_sum", "null".into());
            bench.with_output("expected_sum", "null".into());
            bench.with_output("sum_match", "false".into());
        }
    }

    if my_pe == 0 {
        let result_path = benchmark_record::default_output_path("benchmarking");
        println!(
            "PE {my_pe}: permute {:.6}s, collect {:.6}s, MUPS {:.6} -> {:?}",
            permute_secs, collect_secs, mups, result_path
        );
        bench.write(&result_path);
        println!("Benchmark Results");
        bench.display(Some(3));
    }
}
