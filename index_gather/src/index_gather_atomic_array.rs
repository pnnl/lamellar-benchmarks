use lamellar::array::{
    AccessOps, AtomicArray, DistributedIterator, Distribution, ReadOnlyArray, UnsafeArray,
};
use parking_lot::Mutex;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Instant;

fn index_gather(array: &AtomicArray<usize>, rand_index: &ReadOnlyArray<usize>) {
    array.batch_load(rand_index);
}

const COUNTS_LOCAL_LEN: usize = 100_000_000; //this will be 800MBB on each pe
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

    let unsafe_array = UnsafeArray::<usize>::new(world.team(), global_count, Distribution::Cyclic);
    let rand_index =
        UnsafeArray::<usize>::new(world.team(), l_num_updates * num_pes, Distribution::Block);
    let rng: Arc<Mutex<StdRng>> = Arc::new(Mutex::new(SeedableRng::seed_from_u64(my_pe as u64)));

    // initialize arrays
    let array_init = unsafe_array
        .dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i);
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...
    unsafe {
        let mut rng = rng.lock();
        for elem in rand_index.local_as_mut_slice().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(array_init);
    let array = unsafe_array.into_atomic();
    let rand_index = rand_index.into_read_only();
    world.barrier();

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
    }
}
