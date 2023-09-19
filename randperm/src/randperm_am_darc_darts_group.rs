mod options;
use clap::Parser;

use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    #[AmGroup(static)]
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

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::RandpermCli::parse();

    let global_count = cli.global_size;
    let local_count = global_count / num_pes;
    let iterations = cli.iterations;
    let target_factor = cli.target_factor;
    let launch_threads = cli.launch_threads;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let target_local_count = local_count * target_factor;
    //initialize targets with max
    let mut targets: Vec<AtomicUsize> = Vec::with_capacity(target_local_count);
    for _ in 0..target_local_count {
        targets.push(AtomicUsize::new(usize::MAX));
    }

    let targets = Darc::new(&world, (targets, AtomicUsize::new(0))).unwrap();
    let the_array = LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let local_lens = AtomicArray::new(&world, num_pes, lamellar::Distribution::Block);

    for _ in 0..iterations {
        world.barrier();
        let world2 = world.clone();
        let targets2 = targets.clone();
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
                    let mut darts = typed_am_group!(DartAm, &world2);
                    // let mut darts = FuturesUnordered::new();
                    for (i, _) in chunk.clone() {
                        let dart = DartAm {
                            indices: targets2.clone(),
                            val: i + my_pe * local_count,
                        };
                        darts.add_am_pe(rng.gen_range(0, num_pes), dart);
                    }
                    // println!("launch time {:?}s ", timer.elapsed().as_secs_f64());

                    let world2 = world2.clone();
                    let targets2 = targets2.clone();
                    async move {
                        let mut reqs = darts.exec().await;
                        // println!("initial permute time {:?}", timer.elapsed());
                        while reqs.len() > 0 {
                            let mut darts = typed_am_group!(DartAm, &world2);
                            for req in reqs.iter() {
                                if let AmGroupResult::Pe(_, res) = req {
                                    if let Err(i) = res {
                                        let dart = DartAm {
                                            indices: targets2.clone(),
                                            val: *i,
                                        };
                                        darts.add_am_pe(rng.gen_range(0, num_pes), dart);
                                    }
                                }
                            }
                            reqs = darts.exec().await;
                        }
                    }
                })
                .for_each_async(move |future| async move { future.await }),
        );
        world.wait_all();
        if my_pe == 0 {
            println!("local run time {:?} ", start.elapsed(),);
        }
        world.barrier(); //all work is done
        if my_pe == 0 {
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
        if my_pe == 0 {
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
}
