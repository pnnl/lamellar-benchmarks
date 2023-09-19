mod options;
use clap::Parser;

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use lamellar::memregion::prelude::*;

use rand::prelude::*;
use std::time::Instant;

//===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct IndexGatherAM {
    offset: u32,
    #[AmGroup(static)]
    counts: Darc<Vec<usize>>,
}

#[lamellar::am]
impl LamellarAM for IndexGatherAM {
    async fn exec(self) -> usize {
        self.counts[self.offset as usize]
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: OneSidedMemoryRegion<usize>,
    counts: Darc<Vec<usize>>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let mut tg = typed_am_group!(IndexGatherAM, lamellar::world.clone());
        for idx in unsafe { self.rand_index.as_slice().unwrap() } {
            let rank = idx % lamellar::num_pes;
            let offset = idx / lamellar::num_pes;
            tg.add_am_pe(
                rank,
                IndexGatherAM {
                    offset: offset as u32,
                    counts: self.counts.clone(),
                },
            );
        }
        tg.exec().await;
    }
}

async fn run_ig(
    world: &LamellarWorld,
    num_pes: usize,
    rand_index: &[usize],
    counts: &Darc<Vec<usize>>,
) {
    let mut tg = typed_am_group!(IndexGatherAM, world.clone());
    for idx in rand_index {
        let rank = idx % num_pes;
        let offset = idx / num_pes;
        tg.add_am_pe(
            rank,
            IndexGatherAM {
                offset: offset as u32,
                counts: counts.clone(),
            },
        );
    }
    tg.exec().await;
}

//===== HISTO END ======

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::IndexGatherCli::parse();

    let global_count = cli.global_size;
    let local_count = global_count / num_pes;
    let g_num_updates = cli.global_updates;
    let l_num_updates = g_num_updates / num_pes;
    let iterations = cli.iterations;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let counts_data = vec![0; local_count];
    let counts = Darc::new(&world, counts_data).expect("unable to create darc");

    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    unsafe {
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }
    for _i in 0..iterations {
        world.barrier();
        let now = Instant::now();
        // let launch_tasks = index_gather(l_num_updates, launch_threads, &world, &rand_index, &counts);
        let launch_tasks = run_ig(
            &world,
            num_pes,
            unsafe { rand_index.as_slice().unwrap() },
            &counts,
        );

        if my_pe == 0 {
            println!("{:?} issue time {:?} ", my_pe, now.elapsed(),);
        }
        world.block_on(async move {
            // for task in launch_tasks {
            //     task.await;
            // }
            launch_tasks.await;
        });
        if my_pe == 0 {
            println!("{:?} launch task time {:?} ", my_pe, now.elapsed(),);
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
        let global_time = now.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!(
                "MUPS: {:?}",
                ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time
            );
            println!("Secs: {:?}", global_time,);
            println!(
                "GB/s Injection rate: {:?}",
                (8.0 * (l_num_updates * 2) as f64 * 1.0E-9) / global_time,
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
    }

    // println!(
    //     "pe {:?} sum {:?}",
    //     my_pe,
    //     counts.as_slice().unwrap().iter().sum::<usize>()
    // );
}
