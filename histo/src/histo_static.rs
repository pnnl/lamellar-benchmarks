// #![feature(duration_float)]
// extern crate lamellar;
use rand::prelude::*;

#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

static COUNTS_LOCAL_LEN: usize = 10000000;
lazy_static! {
    static ref COUNTS: Mutex<Vec<u64>> = Mutex::new(vec![0; COUNTS_LOCAL_LEN]);
}

fn init_counts2() -> Vec<AtomicUsize> {
    let mut temp: Vec<AtomicUsize> = Vec::new();
    for i in 0..COUNTS_LOCAL_LEN {
        temp.push(AtomicUsize::new(0));
    }
    temp
}

lazy_static! {
    static ref COUNTS2: Vec<AtomicUsize> = init_counts2();
}

#[inline(never)]
fn update_index(index: usize) {
    // COUNTS2[index].fetch_add(1,Ordering::SeqCst);
    COUNTS.lock().unwrap()[index] += 1;
}
// srun -N <num nodes> target/release/histo <num updates>
fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (my_rank, num_ranks) = lamellar::init();

    let global_count = COUNTS_LOCAL_LEN * num_ranks;

    //initialize counts...
    let c = COUNTS.lock().unwrap();
    drop(c);

    let c2 = &COUNTS2;
    drop(c2);

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_rank as u64);
    let index: Vec<usize> = (0..l_num_updates)
        .map(|_| rng.gen_range(0, global_count))
        .collect();

    println!("my_rank: {:?}", my_rank);

    lamellar::barrier();
    let now = Instant::now();
    for idx in index {
        let rank = idx % num_ranks;
        let offset = idx / num_ranks;

        lamellar::exec_on_pe(
            rank,
            lamellar::FnOnce!([offset] move || {
                COUNTS2[offset].fetch_add(1,Ordering::SeqCst);
                // COUNTS.lock().unwrap()[offset]+=1;
                // update_index(offset);
            }),
        );
    }
    if (my_rank == 0) {
        println!("{:?} issue time {:?} ", my_rank, now.elapsed());
    }
    lamellar::wait_all();

    if (my_rank == 0) {
        println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    lamellar::barrier();

    if (my_rank == 0) {
        println!(
            "MUPS: {:?}",
            ((l_num_updates * num_ranks) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    if (my_rank == 0) {
        println!(
            "{:?} global time {:?} MB {:?} MB/s: {:?} global mups: {:?} ",
            my_rank,
            now.elapsed(),
            lamellar::MB_sent()[0] * 1_000_000.0,
            lamellar::MB_sent().iter().sum::<f64>() / now.elapsed().as_secs_f64(),
            ((l_num_updates * num_ranks) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }

    // println!("rank {:?} {:?}", my_rank, COUNTS.lock().unwrap());
    // let l_sum: u64 = COUNTS2().iter().sum();
    // if (my_rank == 0) {
    //     println!("rank {:?} {:?}", my_rank, l_sum);
    // }

    lamellar::finit();
}
