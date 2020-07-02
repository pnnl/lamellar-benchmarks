use lamellar::{ActiveMessaging, LamellarAM};
use rand::prelude::*;

#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

static COUNTS_LOCAL_LEN: usize = 10000000;

lazy_static! {
    static ref COUNTS: Vec<AtomicUsize> = {
        let mut temp: Vec<AtomicUsize> = Vec::new();
        for _i in 0..COUNTS_LOCAL_LEN {
            temp.push(AtomicUsize::new(0));
        }
        temp
    };
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct HistoBufferedAM {
    buff: std::vec::Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for HistoBufferedAM {
    fn exec(self) {
        for o in &self.buff {
            COUNTS[*o].fetch_add(1, Ordering::SeqCst);
        }
    }
}

// srun -N <num nodes> target/release/histo_buffered <num updates> <num buffered>
fn main() {
    let args: Vec<String> = std::env::args().collect();

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let global_count = COUNTS_LOCAL_LEN * num_pes;
    assert!(
        COUNTS.len() == COUNTS_LOCAL_LEN,
        "error initializing counts"
    );

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let buffer_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let rand_index = (0..l_num_updates)
    .map(|_| rng.gen_range(0, global_count));

    println!("my_pe: {:?}", my_pe);

    world.barrier();
    let now = Instant::now();
    let mut buffs: std::vec::Vec<std::vec::Vec<usize>> =
        vec![Vec::with_capacity(buffer_amt); num_pes];
    for idx in rand_index {
        let rank = idx % num_pes;
        let offset = idx / num_pes;

        buffs[rank].push(offset);
        if buffs[rank].len() >= buffer_amt {
            let buff = buffs[rank].clone();
            world.exec_am_pe(rank, HistoBufferedAM { buff: buff });
            buffs[rank].clear();
        }
    }
    for rank in 0..num_pes {
        let buff = buffs[rank].clone();
        if buff.len() > 0 {
            world.exec_am_pe(rank, HistoBufferedAM { buff: buff });
        }
    }
    if my_pe == 0 {
        println!("{:?} issue time {:?} ", my_pe, now.elapsed(),);
    }
    world.wait_all();
    if my_pe == 0 {
        println!(
            "local run time {:?} local mups: {:?}",
            now.elapsed(),
            (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
    world.barrier();

    if my_pe == 0 {
        println!(
            "MUPS: {:?}",
            ((l_num_updates * num_pes) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }

    if my_pe == 0 {
        println!(
            "{:?} global time {:?} MB {:?} MB/s: {:?} global mups: {:?} ",
            my_pe,
            now.elapsed(),
            world.MB_sent(),
            world.MB_sent().iter().sum::<f64>() / now.elapsed().as_secs_f64(),
            ((l_num_updates * num_pes) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }

    // println!("rank {:?} {:?}", my_pe, COUNTS.lock().unwrap());
    let l_sum: usize = COUNTS.iter().map(|elem| elem.load(Ordering::SeqCst)).sum();
    if my_pe == 0 {
        println!("rank {:?} {:?}", my_pe, l_sum);
    }
    world.barrier();
}
