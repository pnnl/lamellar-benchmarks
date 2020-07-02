use lamellar::{ActiveMessaging, LamellarAM};
use rand::prelude::*;

#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

static COUNTS_LOCAL_LEN: usize = 10000000;

fn init_counts() -> Vec<AtomicUsize> {
    let mut temp: Vec<AtomicUsize> = Vec::new();
    for _i in 0..COUNTS_LOCAL_LEN {
        temp.push(AtomicUsize::new(0));
    }
    temp
}

lazy_static! {
    static ref COUNTS: Vec<AtomicUsize> = init_counts();
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct HistoAM {
    offset: usize,
}

#[lamellar::am]
impl LamellarAM for HistoAM {
    fn exec(self) {
        COUNTS[self.offset].fetch_add(1, Ordering::Relaxed);
    }
}

// srun -N <num nodes> target/release/histo <num updates>
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let global_count = COUNTS_LOCAL_LEN * num_pes;

    //initialize counts...
    assert!(
        COUNTS.len() == COUNTS_LOCAL_LEN,
        "error initializing counts"
    );

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let rand_index = (0..l_num_updates)
    .map(|_| rng.gen_range(0, global_count));

    println!("my_pe: {:?}", my_pe);

    world.barrier();
    let now = Instant::now();
    for idx in rand_index {
        let rank = idx % num_pes;
        let offset = idx / num_pes;
        world.exec_am_pe(rank, HistoAM { offset: offset });
    }
    if my_pe == 0 {
        println!("{:?} issue time {:?} ", my_pe, now.elapsed());
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
            world.MB_sent()[0] * 1_000_000.0,
            world.MB_sent().iter().sum::<f64>() / now.elapsed().as_secs_f64(),
            ((l_num_updates * num_pes) as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
        );
    }
}
