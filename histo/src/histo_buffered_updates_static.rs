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
    static ref COUNTS2: Vec<AtomicUsize> = {
        let mut temp: Vec<AtomicUsize> = Vec::new();
        for i in 0..COUNTS_LOCAL_LEN {
            temp.push(AtomicUsize::new(0));
        }
        temp
    };
}

fn update_index(index: usize) {
    COUNTS2[index].fetch_add(1, Ordering::SeqCst);
    // COUNTS.lock().unwrap()[index]+=1;
}

// srun -N <num nodes> target/release/histo_buffered <num updates> <num buffered>
fn main() {
    let args: Vec<String> = std::env::args().collect();

    let (my_rank, num_ranks) = lamellar::init();
    let global_count = COUNTS_LOCAL_LEN * num_ranks;
    let c = COUNTS.lock().unwrap();
    drop(c);

    let c2 = &COUNTS2;
    drop(c2);

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let buffer_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_rank as u64);
    let index: Vec<usize> = (0..l_num_updates)
        .map(|_| rng.gen_range(0, global_count))
        .collect();

    println!("my_rank: {:?}", my_rank);

    lamellar::barrier();
    let now = Instant::now();
    let mut buffs: std::vec::Vec<std::vec::Vec<usize>> =
        vec![Vec::with_capacity(buffer_amt); num_ranks];
    for idx in index {
        let rank = idx % num_ranks;
        let offset = idx / num_ranks;

        buffs[rank].push(offset);
        if buffs[rank].len() >= buffer_amt {
            let buff = buffs[rank].clone();
            lamellar::exec_on_pe(
                rank,
                lamellar::FnOnce!([buff] move || {
                    let mut c= COUNTS.lock().unwrap();
                    for o in buff{
                        // COUNTS2[o].fetch_add(1,Ordering::SeqCst);
                        // COUNTS.lock().unwrap()[o]+=1;
                        c[o]+=1;
                        // update_index(o);
                    }
                }),
            );
            buffs[rank].clear();
        }
    }
    for rank in 0..num_ranks {
        let buff = buffs[rank].clone();
        if buff.len() > 0 {
            lamellar::exec_on_pe(
                rank,
                lamellar::FnOnce!([buff] move || {
                    // let mut c= COUNTS.lock().unwrap();
                    for o in buff{
                        COUNTS2[o].fetch_add(1,Ordering::SeqCst);
                        // c[o]+=1;
                        // COUNTS.lock().unwrap()[o]+=1;
                        // update_index(o)
                    }
                }),
            );
        }
    }
    if (my_rank == 0) {
        println!("{:?} issue time {:?} ", my_rank, now.elapsed(),);
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
            lamellar::MB_sent(),
            lamellar::MB_sent().iter().sum::<f64>() / now.elapsed().as_secs_f64(),
            ((l_num_updates * num_ranks) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }

    // println!("rank {:?} {:?}", my_rank, COUNTS.lock().unwrap());
    let l_sum: u64 = COUNTS.lock().unwrap().iter().sum();
    if (my_rank == 0) {
        println!("rank {:?} {:?}", my_rank, l_sum);
    }
    lamellar::barrier();
    lamellar::finit();
}
