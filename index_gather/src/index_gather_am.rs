use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use lamellar::memregion::prelude::*;

use rand::prelude::*;
use std::future::Future;
use std::time::Instant;

const COUNTS_LOCAL_LEN: usize = 1000000; //100_000_000; //this will be 800MB on each pe
                                         //===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct IndexGatherAM {
    offset: usize,
    counts: Darc<Vec<usize>>,
}

#[lamellar::am]
impl LamellarAM for IndexGatherAM {
    async fn exec(self) -> usize {
        self.counts[self.offset]
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
        for idx in unsafe { self.rand_index.as_slice().unwrap() } {
            let rank = idx % lamellar::num_pes;
            let offset = idx / lamellar::num_pes;
            lamellar::world.exec_am_pe(
                rank,
                IndexGatherAM {
                    offset: offset,
                    counts: self.counts.clone(),
                },
            );
        }
    }
}

fn index_gather(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    rand_index: &OneSidedMemoryRegion<usize>,
    counts: &Darc<Vec<usize>>,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.sub_region(start..end),
            counts: counts.clone(),
        }));
    }
    launch_tasks
}

//===== HISTO END ======

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

    let num_threads = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });
    let iterations = args
        .get(4)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });

    if my_pe == 0 {
        println!("updates total {}", l_num_updates * num_pes);
        println!("updates per pe {}", l_num_updates);
        println!("table size per pe{}", COUNTS_LOCAL_LEN);
    }

    let  counts_data = vec![0; COUNTS_LOCAL_LEN];
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
        let launch_tasks = index_gather(l_num_updates, num_threads, &world, &rand_index, &counts);

        if my_pe == 0 {
            println!("{:?} issue time {:?} ", my_pe, now.elapsed(),);
        }
        world.block_on(async move {
            for task in launch_tasks {
                task.await;
            }
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
