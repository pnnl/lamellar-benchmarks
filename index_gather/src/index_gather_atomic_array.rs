use lamellar::array::{
    AccessOps, ReadOnlyOps, AtomicArray, DistributedIterator, Distribution, ReadOnlyArray, UnsafeArray,
};
use lamellar::{LocalMemoryRegion,RemoteMemoryRegion};
use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;

fn index_gather(array: &AtomicArray<usize>, rand_index: &LocalMemoryRegion<usize>) {
    array.batch_load(rand_index);
}

const COUNTS_LOCAL_LEN: usize = 1000000; //this will be 800MBB on each pe
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
        .unwrap_or_else(|| 1000);

        if my_pe == 0 {
            println!("updates total {}", l_num_updates * num_pes);
            println!("updates per pe {}", l_num_updates);
            println!("table size per pe{}", COUNTS_LOCAL_LEN);
        }

    let unsafe_array = UnsafeArray::<usize>::new(world.team(), global_count, Distribution::Cyclic);
    // let rand_index =
    //     UnsafeArray::<usize>::new(world.team(), l_num_updates * num_pes, Distribution::Block);
    let rand_index = world.alloc_local_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    // initialize arrays
    let array_init = unsafe_array
        .dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i);
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...
    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(array_init);
    let array = unsafe_array.into_atomic();
    // let rand_index = rand_index.into_read_only();
    world.barrier();

    if my_pe == 0 {
        println!("starting index gather");
    }

    let now = Instant::now();
    index_gather(&array, &rand_index);

    if my_pe == 0 {
        println!("{:?} issue time {:?} ", my_pe, now.elapsed());
    }
    array.wait_all();
    if my_pe == 0 {
        println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    array.barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
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
        println!(
            "Secs: {:?}",
             global_time,
        );
        println!(
            "GB/s Injection rate: {:?}",
            (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time,
        );
    }
}
