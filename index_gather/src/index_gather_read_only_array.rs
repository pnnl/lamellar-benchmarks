use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use rand::prelude::*;
use std::time::Instant;

fn index_gather(array: &ReadOnlyArray<usize>, rand_index: OneSidedMemoryRegion<usize>) {
    let rand_slice = unsafe {rand_index.as_slice().expect("PE on world team")}; // Safe as we are the only consumer of this mem region
    array.batch_load(rand_slice);
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

    let unsafe_array = UnsafeArray::<usize>::new(world.team(), global_count, lamellar::array::Distribution::Cyclic);
    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    // initialize arrays
    let array_init = unsafe {unsafe_array
        .dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i)};
    // rand_index.dist_iter_mut().for_each(move |x| *x = rng.lock().gen_range(0,global_count)).wait(); //this is slow because of the lock on the rng so we will do unsafe slice version instead...
    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    world.block_on(array_init);
    let array = unsafe_array.into_read_only();
    // let rand_index = rand_index.into_read_only();
    world.barrier();

    if my_pe == 0 {
        println!("starting index gather");
    }

    let now = Instant::now();
    index_gather(&array, rand_index);

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

    if my_pe == 0 {
        println!("{{\"updates_per_pe\":{},\"num_pes\":{},\"total_updates\":{},\"table_size_per_pe\":{},\"execution_time_secs\":{:.6},\"mups\":{:.6},\"gb_per_sec_injection_rate\":{:.6},\"mb_sent\":{:.6},\"mb_per_sec\":{:.6}}}",
            l_num_updates,
            num_pes,
            l_num_updates * num_pes,
            COUNTS_LOCAL_LEN,
            global_time,
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time,
            (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time,
            world.MB_sent(),
            world.MB_sent() / global_time
        );
    }
}
