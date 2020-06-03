use rand::prelude::*;
use std::time::Instant;

const COUNTS_LOCAL_LEN: usize = 10000000;

// srun -N <num nodes> target/release/histo <num updates>
fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (my_pe, num_pes) = lamellar::init();
    let counts = lamellar::alloc_mem_region(COUNTS_LOCAL_LEN);
    let global_count = COUNTS_LOCAL_LEN * num_pes;

    //zero initialize counts
    unsafe {
        for elem in counts.as_mut_slice().iter_mut(){
            *elem = 0;
        }
    }
    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let index: Vec<usize> = (0..l_num_updates)
        .map(|_| rng.gen_range(0, global_count))
        .collect();

    println!("my_pe: {:?}", my_pe);

    lamellar::barrier();
    let now = Instant::now();
    for idx in index {
        let rank = idx % num_pes;
        let offset = idx / num_pes;

        lamellar::exec_on_pe(
            rank,
            lamellar::FnOnce!([offset,counts] move || {
                unsafe { counts.as_mut_slice()[offset]+=1 }; //this is currently unsafe and has potential for races / dropped updates
            }),
        );
    }
    if my_pe == 0 {
        println!("{:?} issue time {:?} ", my_pe, now.elapsed());
    }
    lamellar::wait_all();

    if my_pe == 0 {
        println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    lamellar::barrier();
    let global_time = now.elapsed().as_secs_f64();
    if my_pe == 0 {
        println!(
            "MUPS: {:?}",
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
        );
    }
    if my_pe == 0 {
        println!(
            "{:?} global time {:?} MB {:?} MB/s: {:?} global mups: {:?} ",
            my_pe,
            global_time,
            lamellar::MB_sent(),
            lamellar::MB_sent().iter().sum::<f64>() / global_time,
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) /global_time
        );
    }

    println!("pe {:?} sum {:?}",my_pe,counts.as_slice().iter().sum::<usize>());
    counts.delete();
    lamellar::finit();
}
