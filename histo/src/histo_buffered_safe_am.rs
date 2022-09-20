use lamellar::{
    ActiveMessaging, LamellarRequest, LamellarTaskGroup, LamellarWorld, LocalMemoryRegion,
    RemoteMemoryRegion, SharedMemoryRegion,
};

use rand::prelude::*;
use std::time::Instant;

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};

const COUNTS_LOCAL_LEN: usize = 100_000_000; //this will be 800MB on each pe

//===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct HistoBufferedAM {
    buff: std::vec::Vec<usize>,
    counts: SharedMemoryRegion<usize>,
}

#[lamellar::am]
impl LamellarAM for HistoBufferedAM {
    async fn exec(self) {
        // cast the shared memory region from usize to atomicusize
        let slice = unsafe {
            let slice = self.counts.as_mut_slice().unwrap();
            let slice_ptr = slice.as_mut_ptr() as *mut AtomicUsize;
            std::slice::from_raw_parts_mut(slice_ptr, slice.len())
        };
        for o in &self.buff {
            slice[*o].fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: LocalMemoryRegion<usize>,
    counts: SharedMemoryRegion<usize>,
    buffer_amt: usize,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let now = Instant::now();
        let num_pes = lamellar::num_pes;
        let mut buffs: std::vec::Vec<std::vec::Vec<usize>> =
            vec![Vec::with_capacity(self.buffer_amt); num_pes];
        let task_group = LamellarTaskGroup::new(lamellar::team.clone());
        for idx in self.rand_index.as_slice().unwrap() {
            let rank = idx % num_pes;
            let offset = idx / num_pes;

            buffs[rank].push(offset);
            if buffs[rank].len() >= self.buffer_amt {
                let buff = buffs[rank].clone();
                task_group.exec_am_pe(
                    rank,
                    HistoBufferedAM {
                        buff: buff,
                        counts: self.counts.clone(),
                    },
                );
                buffs[rank].clear();
            }
        }
        //send any remaining buffered updates
        for rank in 0..num_pes {
            let buff = buffs[rank].clone();
            if buff.len() > 0 {
                task_group.exec_am_pe(
                    rank,
                    HistoBufferedAM {
                        buff: buff,
                        counts: self.counts.clone(),
                    },
                );
            }
        }
    }
}

fn histo(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    rand_index: &LocalMemoryRegion<usize>,
    counts: &SharedMemoryRegion<usize>,
    buffer_amt: usize,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.sub_region(start..end),
            counts: counts.clone(),
            buffer_amt: buffer_amt,
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
    let counts = world.alloc_shared_mem_region(COUNTS_LOCAL_LEN);
    let global_count = COUNTS_LOCAL_LEN * num_pes;
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let buffer_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);
    let num_threads = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });
    let rand_index = world.alloc_local_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    unsafe {
        for elem in counts.as_mut_slice().unwrap().iter_mut() {
            *elem = 0;
        }
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            *elem = rng.gen_range(0, global_count);
        }
    }

    //get number of updates to perform from first command line argument otherwise set to 1000 updates
    world.barrier();
    let now = Instant::now();
    let launch_tasks = histo(
        l_num_updates,
        num_threads,
        &world,
        &rand_index,
        &counts,
        buffer_amt,
    );

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

    println!(
        "pe {:?} sum {:?}",
        my_pe,
        counts.as_slice().unwrap().iter().sum::<usize>()
    );
}
