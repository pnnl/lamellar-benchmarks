use lamellar::{ActiveMessaging, LocalMemoryRegion, SharedMemoryRegion, RemoteMemoryRegion};

use rand::prelude::*;
use std::time::Instant;

const COUNTS_LOCAL_LEN: usize = 10000000;

#[lamellar::AmData(Clone, Debug)]
struct HistoAM {
    offset: usize,
    counts: SharedMemoryRegion<usize>
}

#[lamellar::am]
impl LamellarAM for HistoAM {
    fn exec(self) {
        unsafe { self.counts.as_mut_slice().unwrap()[self.offset]+=1 }; //this is unsafe and has potential for races / dropped updates
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: LocalMemoryRegion<usize>,
    counts: SharedMemoryRegion<usize>
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    fn exec(self) {
        for idx in self.rand_index.as_slice().unwrap() {
            let rank = idx % lamellar::num_pes;
            let offset = idx /  lamellar::num_pes;
    
            lamellar::world.exec_am_pe(
                rank,
                HistoAM{
                    offset: offset,
                    counts: self.counts.clone(),
                },
            );
        }
        
    }
}


// srun -N <num nodes> target/release/histo <num updates>
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

    let counts = world.alloc_shared_mem_region(COUNTS_LOCAL_LEN);    
    let rand_index = world.alloc_local_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    //initialize arrays
    unsafe {
        for elem in counts.as_mut_slice().unwrap().iter_mut(){
            *elem = 0;
        }
        for elem in rand_index.as_mut_slice().unwrap().iter_mut(){
            *elem =  rng.gen_range(0, global_count);
        }
    }

    //create multiple launch tasks, that iterated through portions of rand_index in parallel
    let num_threads = match std::env::var("LAMELLAR_THREADS") {
        Ok(n) => n.parse::<usize>().unwrap(),
        Err(_) => 1,
    };
    let num_threads = std::cmp::max(num_threads/2,1);
    let slice_size = l_num_updates as f32/num_threads as f32;
    world.barrier();
    let now = Instant::now();
    for tid in 0..num_threads{
        let start = (tid as f32*slice_size).round() as usize;
        let end = ((tid+1) as f32 * slice_size).round() as usize;
        world.exec_am_local(
            LaunchAm{
                rand_index: rand_index.sub_region(start..end),
                counts: counts.clone(),
            }
        );
    }
    // for idx in rand_index.as_slice().unwrap() {
    //     let rank = idx % num_pes;
    //     let offset = idx / num_pes;

    //     world.exec_am_pe(
    //         rank,
    //         HistoAM{
    //             offset: offset,
    //             counts: counts.clone(),
    //         },
    //     );
    // }
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
            ((l_num_updates * num_pes) as f64 / 1_000_000.0) /global_time
        );
    }

    println!("pe {:?} sum {:?}",my_pe,counts.as_slice().unwrap().iter().sum::<usize>());
}
