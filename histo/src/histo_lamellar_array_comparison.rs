use lamellar::array::prelude::*;

use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;
use benchmark_record;

const COUNTS_LOCAL_LEN: usize = 100_000_000; //this will be 800MBB on each pe

// Small struct to return the metrics from histo()
struct HistoResult {
    global_time_secs: f64,
    local_run_time_secs: f64,
    mups: f64,
    mb_sent: f64,
}

fn histo<T: ElementArithmeticOps + std::fmt::Debug>(
    _array_type: &str,
    counts: LamellarWriteArray<T>,
    rand_index: &ReadOnlyArray<usize>,
    world: &LamellarWorld,
    my_pe: usize,
    num_pes: usize,
    l_num_updates: usize,
    one: T,
    prev_amt: f64,
) -> HistoResult {
    let now = Instant::now();

    //the actual histo
    let _ = counts.batch_add(rand_index.local_data(), one).spawn();

    //-----------------
    if my_pe == 0 {
        // issue time (kept as a human log)
        // println!("{:?} issue time {:?} ", my_pe, now.elapsed());
    }
    counts.wait_all();

    let local_run_time = now.elapsed().as_secs_f64();
    let _local_mups = (l_num_updates as f64 / 1_000_000.0) / local_run_time;

    counts.barrier();
    let global_time = now.elapsed().as_secs_f64();
    let mb_sent = world.MB_sent() - prev_amt;
    let global_mups = ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time;

    if my_pe == 0 {
        /*println!(
            "global time {:?} MB {:?} MB/s: {:?}",
            global_time,
            mb_sent,
            mb_sent / global_time,
        );
        println!("MUPS: {:?}, {:?}", global_mups, _array_type);
        */
    }

    // ensure global barriers same as before
    counts.barrier();

    HistoResult {
        global_time_secs: global_time,
        local_run_time_secs: local_run_time,
        mups: global_mups,
        mb_sent,
    }
}

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

    let counts = UnsafeArray::<usize>::new(
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
    let counts = counts.block();

    // initialize arrays
    let counts_init = unsafe { counts.dist_iter_mut().for_each(|x| *x = 0).spawn() };
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...
    let rand_index = rand_index.block();

    unsafe {
        let mut rng = rng.lock();
        for elem in rand_index.local_as_mut_slice().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(counts_init);

    let rand_index = rand_index.into_read_only().block();
    world.barrier();

    let mut result_record = benchmark_record::BenchmarkInformation::new();
    let results_file = &result_record.default_output_path("benchmarking");

    let res_unsafe = histo(
        "unsafe",
        counts.clone().into(),
        &rand_index,
        &world,
        my_pe,
        num_pes,
        l_num_updates,
        1,
        0.0,
    );

    result_record.with_output("run_mode", "unsafe".into());
    result_record.with_output("updates_total", (l_num_updates * num_pes).to_string());
    result_record.with_output("updates_per_pe", l_num_updates.to_string());
    result_record.with_output("table_size_per_pe", COUNTS_LOCAL_LEN.to_string());
    result_record.with_output("local_run_time (secs)", res_unsafe.local_run_time_secs.to_string());
    result_record.with_output("local_mups", res_unsafe.mups.to_string());
    result_record.with_output("global_run_time (secs)", res_unsafe.global_time_secs.to_string());
    result_record.with_output("MB_sent", res_unsafe.mb_sent.to_string());
    result_record.with_output("MB_per_sec", (res_unsafe.mb_sent / res_unsafe.global_time_secs).to_string());
    result_record.with_output("array_type", "unsafe".into());
    if my_pe == 0 {
        result_record.write(results_file);
        println!("Finished 'unsafe' run mode");
    }

    world.block_on(unsafe { counts.dist_iter_mut().for_each(|x| *x = 0).spawn() });
    counts.barrier();

    let mut result_record = benchmark_record::BenchmarkInformation::new();
    let counts = counts.into_local_lock().block();
    let res_local_lock = histo(
        "local_lock",
        counts.clone().into(),
        &rand_index,
        &world,
        my_pe,
        num_pes,
        l_num_updates,
        1,
        res_unsafe.mb_sent,
    );

    result_record.with_output("run_mode", "local_lock".into());
    result_record.with_output("local_run_time (secs)", res_local_lock.local_run_time_secs.to_string());
    result_record.with_output("local_mups", res_local_lock.mups.to_string());
    result_record.with_output("global_run_time (secs)", res_local_lock.global_time_secs.to_string());
    result_record.with_output("MB_sent", res_local_lock.mb_sent.to_string());
    result_record.with_output("MB_per_sec", (res_local_lock.mb_sent / res_local_lock.global_time_secs).to_string());
    result_record.with_output("array_type", "local_lock".into());

    if my_pe == 0 {
        result_record.write(results_file);
        println!("Finished 'local_lock' run mode");
    }
    world.block_on(counts.dist_iter_mut().for_each(|x| *x = 0));
    counts.barrier();

    let mut result_record = benchmark_record::BenchmarkInformation::new();
    let counts = counts.into_atomic().block();
    let res_atomic = histo(
        "atomic",
        counts.clone().into(),
        &rand_index,
        &world,
        my_pe,
        num_pes,
        l_num_updates,
        1,
        res_local_lock.mb_sent,
    );
    
    result_record.with_output("run_mode", "atomic".into());
    result_record.with_output("local_run_time (secs)", res_atomic.local_run_time_secs.to_string());
    result_record.with_output("local_mups", res_atomic.mups.to_string());
    result_record.with_output("global_run_time (secs)", res_atomic.global_time_secs.to_string());
    result_record.with_output("MB_sent", res_atomic.mb_sent.to_string());
    result_record.with_output("MB_per_sec", (res_atomic.mb_sent / res_atomic.global_time_secs).to_string());
    result_record.with_output("array_type", "atomic".into());


    if my_pe == 0 {
        result_record.write(results_file);
        println!("Finished 'atomic' run mode");
    }
}
