mod options;
use clap::Parser;

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

//===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct HistoAM {
    offset: usize,
    #[AmGroup(static)]
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::am]
impl LamellarAM for HistoAM {
    async fn exec(self) {
        self.counts[self.offset as usize].fetch_add(1, Ordering::Relaxed);
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: Vec<usize>,
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let mut tg = typed_am_group!(HistoAM, lamellar::world.clone());
        for idx in &self.rand_index {
            let rank = idx % lamellar::num_pes;
            let offset = idx / lamellar::num_pes;
            tg.add_am_pe(
                rank,
                HistoAM {
                    offset: offset as usize,
                    counts: self.counts.clone(),
                },
            );
        }
        tg.exec().await;
    }
}

//===== HISTO END ======

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::HistoCli::parse();

    let global_count = cli.global_size;
    let local_count = global_count / num_pes;
    let g_num_updates = cli.global_updates;
    let l_num_updates = g_num_updates / num_pes;
    let iterations = cli.iterations;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let mut counts_data = Vec::with_capacity(local_count);
    for _ in 0..local_count {
        counts_data.push(AtomicUsize::new(0));
    }
    let counts = Darc::new(&world, counts_data).expect("unable to create darc");
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let rand_index = (0..l_num_updates)
        .into_iter()
        .map(|_| rng.gen_range(0, global_count))
        .collect::<Vec<usize>>();

    for _i in 0..iterations {
        world.barrier();
        let now = Instant::now();

        let mut tg = typed_am_group!(HistoAM, world.clone());
        for idx in &rand_index {
            let rank = idx % num_pes;
            let offset = idx / num_pes;
            tg.add_am_pe(
                rank,
                HistoAM {
                    offset: offset as usize,
                    counts: counts.clone(),
                },
            );
        }

        if my_pe == 0 {
            println!("{:?} issue time {:?} ", my_pe, now.elapsed());
        }
        let res = tg.exec();
        if my_pe == 0 {
            println!("{:?} launch task time {:?} ", my_pe, now.elapsed(),);
        }
        world.block_on(res);
        world.wait_all();

        if my_pe == 0 {
            println!(
                "local run time {:?} local mups: {:?}",
                now.elapsed(),
                (l_num_updates as f32 / 1_000_000.0) / now.elapsed().as_secs_f32()
            );
        }
        world.barrier();
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
                world.MB_sent(),
                world.MB_sent() / global_time,
                ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
            );
        }

        if my_pe == 0 {
            println!(
                "pe {:?} sum {:?}",
                my_pe,
                counts
                    .iter()
                    .map(|e| e.load(Ordering::Relaxed))
                    .sum::<usize>()
            );
        }

        for elem in counts.iter() {
            elem.store(0, Ordering::SeqCst);
        }
    }
}
