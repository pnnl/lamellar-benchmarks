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
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_count = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000); //size of permuted array
    let target_factor = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 10); //multiplication factor for target array
    let iterations = args
        .get(4)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1);

    if my_pe == 0 {
        println!("array size {}", global_count);
        println!("target array size {}", global_count * target_factor);
    }

    let local_count = (global_count * target_factor) / num_pes;

    let the_array = LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let mut targets: Vec<AtomicUsize> = Vec::with_capacity(local_count);
    //initialize targets with 0
    for _ in 0..local_count {
        targets.push(AtomicUsize::new(usize::MAX));
    }

    let targets = Darc::new(&world, (targets, AtomicUsize::new(0))).unwrap();

    for _ in 0..iterations {
        world.barrier();
        let world2 = world.clone();
        let targets2 = targets.clone();
        let throw_darts = async move {
            let mut thread_rng = thread_rng();
            let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
            let mut dart_cnt = 0;
            let mut reqs = (my_pe..global_count)
                .step_by(num_pes)
                .map(|i| {
                    dart_cnt += 1;
                    let dart = DartAm {
                        indices: targets2.clone(),
                        val: i,
                    };
                    world2.exec_am_pe(rng.gen_range(0, num_pes), dart)
                })
                .collect::<FuturesUnordered<_>>();

            while reqs.len() > 0 {
                // println!("{}", reqs.len());
                reqs = reqs
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
        };
        //time throw darts
        let start = Instant::now();
        world.block_on(throw_darts);
        if my_pe == 0 {
            println!("local run time {:?} ", start.elapsed(),);
        }
        world.barrier(); //all work is done
        if my_pe == 0 {
            println!("permute time {:?}s ", start.elapsed().as_secs_f64());
        }

        let collect_start = Instant::now();
        if my_pe == 0 {
            world.block_on(world.exec_am_local(FillAm {
                target: targets.clone(),
                the_array: the_array.clone(),
                start_index: 0,
            }));
        }
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
