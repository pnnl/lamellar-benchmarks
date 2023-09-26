
use clap::Parser;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    indices: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    val: usize,
}

#[lamellar::am]
impl LamellarAm for DartAm {
    async fn exec(self) -> Result<usize, usize> {
        //create a random index less than the length of indices
        let mut thread_rng = thread_rng();
        let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
        let mut index = rng.gen_range(0, self.indices.0.len());

        //while compare and exhange using the index where to origial val is 0 and the new val is 1 fails
        let mut res = self.indices.0[index].compare_exchange(
            usize::MAX,
            self.val,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        // println!("{:?} {}",res,self.indices.1.load(Ordering::Relaxed));
        while res.is_err() && self.indices.1.load(Ordering::Relaxed) < self.indices.0.len() {
            index = rng.gen_range(0, self.indices.0.len());
            res = self.indices.0[index].compare_exchange(
                usize::MAX,
                self.val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        if res.is_ok() {
            self.indices.1.fetch_add(1, Ordering::Relaxed);
            res
        } else {
            Err(self.val)
        }
    }
}

#[lamellar::AmData]
struct FillAm {
    target: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    the_array: LocalLockArray<usize>,
    start_index: usize,
}

#[lamellar::am]
impl LamellarAm for FillAm {
    async fn exec(self) {
        let data = self
            .target
            .0
            .iter()
            .filter_map(|x| {
                let x = x.load(Ordering::Relaxed);
                if x == usize::MAX {
                    None
                } else {
                    Some(x)
                }
            })
            .collect::<Vec<_>>();
        if lamellar::current_pe < lamellar::num_pes - 1 {
            let req = lamellar::world.exec_am_pe(
                lamellar::current_pe + 1,
                FillAm {
                    target: self.target.clone(),
                    the_array: self.the_array.clone(),
                    start_index: self.start_index + data.len(),
                },
            );
            unsafe {
                self.the_array.put(self.start_index, &data).await;
            };
            req.await;
        } else {
            unsafe { self.the_array.put(self.start_index, &data).await };
        }
    }
}


 
pub fn random_permutation(
        world: LamellarWorld, 
        global_count: usize, // size of permuted array
        target_factor: usize, // multiplication factor for target array -- defualt to 10
        iterations: usize, // -- default to 1
        launch_threads: usize, // -- default to 1?
        seed: usize, 
        verbose: bool, 
    )  
    -> ReadOnlyArray<usize>       
{
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let local_count = global_count / num_pes;

    let target_local_count = local_count * target_factor;

    let mut targets: Vec<AtomicUsize> = Vec::with_capacity(target_local_count);
    //initialize targets with 0
    for _ in 0..target_local_count {
        targets.push(AtomicUsize::new(usize::MAX));
    }

    let targets = Darc::new(&world, (targets, AtomicUsize::new(0))).unwrap();
    let the_array = LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let local_lens = AtomicArray::new(&world, num_pes, lamellar::Distribution::Block);

    for _ in 0..iterations {
        let world2 = world.clone();
        let targets2 = targets.clone();
        world.barrier();
        let start = Instant::now();
        world.block_on(
            the_array
                .local_iter()
                .enumerate()
                .chunks(local_count / launch_threads)
                .map(move |chunk| {
                    // let timer = Instant::now();
                    let mut thread_rng = thread_rng();
                    let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
                    let mut darts = FuturesUnordered::new();
                    for (i, _) in chunk.clone() {
                        let dart = DartAm {
                            indices: targets2.clone(),
                            val: i + my_pe * local_count,
                        };
                        darts.push(world2.exec_am_pe(rng.gen_range(0, num_pes), dart));
                    }
                    // println!("launch time {:?}s ", timer.elapsed().as_secs_f64());
                    let world2 = world2.clone();
                    let targets2 = targets2.clone();
                    async move {
                        while darts.len() > 0 {
                            darts = darts
                                .collect::<Vec<_>>()
                                .await
                                .iter()
                                .filter_map(|req| match req {
                                    Ok(_) => None,
                                    Err(i) => {
                                        let dart = DartAm {
                                            indices: targets2.clone(),
                                            val: *i,
                                        };
                                        Some(world2.exec_am_pe(rng.gen_range(0, num_pes), dart))
                                    }
                                })
                                .collect::<FuturesUnordered<_>>();
                        }
                    }
                })
                .for_each_async(move |future| async move { future.await }),
        );
        world.wait_all();
        if verbose && (my_pe == 0) {
            println!("local run time {:?} ", start.elapsed(),);
        }
        world.barrier(); //all work is done
        if verbose && (my_pe == 0) {
            println!("permute time {:?}s ", start.elapsed().as_secs_f64());
        }

        let collect_start = Instant::now();
        let data = targets
            .0
            .iter()
            .filter_map(|x| {
                let x = x.load(Ordering::Relaxed);
                if x == usize::MAX {
                    None
                } else {
                    Some(x)
                }
            })
            .collect::<Vec<_>>();
        local_lens.local_data().at(0).store(data.len());
        world.barrier();
        let start_index = local_lens
            .buffered_onesided_iter(num_pes)
            .into_iter()
            .take(my_pe)
            .sum::<usize>();
        world.block_on(unsafe { the_array.put(start_index, &data) });
        world.barrier();
        let global_time = start.elapsed().as_secs_f64();
        if verbose && (my_pe == 0) {
            println!("collect time: {:?}s", collect_start.elapsed().as_secs_f64());
            println!(
                "global time {:?} MB {:?} MB/s: {:?} ",
                global_time,
                (world.MB_sent()),
                (world.MB_sent()) / global_time,
            );
            println!("Secs: {:?}", global_time,);
            let sum = world.block_on(the_array.sum());
            println!(
                "reduced sum: {sum} calculated sum {} ",
                (global_count * (global_count + 1) / 2) - global_count
            );
            if sum != (global_count * (global_count + 1) / 2) - global_count {
                println!("Error! randperm not as expected");
            }
        }

        for i in targets.0.iter() {
            i.store(usize::MAX, Ordering::Relaxed);
        }
        targets.1.store(0, Ordering::Relaxed);
        world.barrier();
    }

    the_array.into_read_only()
}
