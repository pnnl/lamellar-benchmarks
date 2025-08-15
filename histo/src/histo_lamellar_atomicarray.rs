use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;

use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;

//===== HISTO BEGIN ======

fn histo(counts: &AtomicArray<usize>, rand_index: &ReadOnlyArray<usize>) {
    let _ = counts.batch_add(rand_index.local_data(), 1).spawn();
}

//===== HISTO END ======

const COUNTS_LOCAL_LEN: usize = 1000000; //100_000_000; //this will be 800MB on each pe
                                         // srun --cpu-bind=ldoms --mpi=pmi2 --cpus-per-task=4 --tasks-per-node=2  -N 2 target/debug/histo_lamellar_array_comparison 2
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

    if my_pe == 0 {
        println!("updates total {}", l_num_updates * num_pes);
        println!("updates per pe {}", l_num_updates);
        println!("table size per pe{}", COUNTS_LOCAL_LEN);
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
    let counts_init = unsafe { unsafe_counts.dist_iter_mut().for_each(|x| *x = 0).spawn() };
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
    println!("My pe {my_pe}: Counts awaited", );

    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        println!(
            "global time {:?} MB {:?} MB/s: {:?} ",
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
        );
        println!("(({l_num_updates}*{num_pes})/1_000_000) ");
    }
    // println!("pe {:?} sum {:?}", my_pe, world.block_on(counts.sum()));
}
