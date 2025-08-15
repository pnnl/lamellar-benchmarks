use lamellar::array::prelude::*;

use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;

const COUNTS_LOCAL_LEN: usize = 100_000_000; //this will be 800MBB on each pe

fn histo<T: ElementArithmeticOps + std::fmt::Debug>(
    array_type: &str,
    counts: LamellarWriteArray<T>,
    rand_index: &ReadOnlyArray<usize>,
    world: &LamellarWorld,
    my_pe: usize,
    num_pes: usize,
    l_num_updates: usize,
    one: T,
    prev_amt: f64,
) -> f64 {
    let now = Instant::now();

    //the actual histo
    let _ = counts.batch_add(rand_index.local_data(), one).spawn();

    //-----------------
    if my_pe == 0 {
        println!("{:?} issue time {:?} ", my_pe, now.elapsed());
    }
    counts.wait_all();
    if my_pe == 0 {
        println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    counts.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        println!(
            "global time {:?} MB {:?} MB/s: {:?}",
            global_time,
            (world.MB_sent() - prev_amt),
            (world.MB_sent() - prev_amt) / global_time,
        );
        println!(
            "MUPS: {:?}, {:?}",
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time,
            array_type
        );
    }

    if my_pe == 0 {
        println!("{{\"array_type\":\"{}\",\"updates_per_pe\":{},\"num_pes\":{},\"total_updates\":{},\"table_size_per_pe\":{},\"execution_time_secs\":{:.6},\"mups\":{:.6},\"mb_sent\":{:.6},\"mb_per_sec\":{:.6}}}",
            array_type,
            l_num_updates,
            num_pes,
            l_num_updates * num_pes,
            COUNTS_LOCAL_LEN,
            global_time,
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time,
            (world.MB_sent() - prev_amt),
            (world.MB_sent() - prev_amt) / global_time
        );
    }

    // println!("pe {:?} sum {:?}", my_pe, world.block_on(counts.as_slice().iter().sum()));
    counts.barrier();
    world.MB_sent()
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
    //counts.wait_all(); equivalent in this case to the above statement

    let rand_index = rand_index.into_read_only().block();
    world.barrier();

    if my_pe == 0 {
        println!("unsafe histo");
    }
    let prev_amt = histo(
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
    world.block_on(unsafe { counts.dist_iter_mut().for_each(|x| *x = 0) });
    counts.barrier();

    let counts = counts.into_local_lock().block();
    if my_pe == 0 {
        println!("local lock atomic histo");
    }
    let prev_amt = histo(
        "local_lock",
        counts.clone().into(),
        &rand_index,
        &world,
        my_pe,
        num_pes,
        l_num_updates,
        1,
        prev_amt,
    );
    world.block_on(counts.dist_iter_mut().for_each(|x| *x = 0));
    counts.barrier();

    let counts = counts.into_atomic().block();
    if my_pe == 0 {
        println!("atomic histo");
    }
    histo(
        "atomic",
        counts.clone().into(),
        &rand_index,
        &world,
        my_pe,
        num_pes,
        l_num_updates,
        1,
        prev_amt,
    );
}
