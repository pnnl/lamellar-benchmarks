mod options;
use clap::Parser;

use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    target: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    vals: Vec<usize>,
}

#[lamellar::am]
impl LamellarAm for DartAm {
    async fn exec(self) -> Vec<usize> {
        //create a random index less than the length of indices
        let mut thread_rng = thread_rng();
        let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
        let mut index = rng.gen_range(0, self.target.0.len());

        let mut results = vec![];
        for val in self.vals.iter() {
            //while compare and exhange using the index where to origial val is 0 and the new val is 1 fails
            let mut res = self.target.0[index].compare_exchange(
                usize::MAX,
                *val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            while res.is_err() && self.target.1.load(Ordering::Relaxed) < self.target.0.len() {
                index = rng.gen_range(0, self.target.0.len());
                res = self.target.0[index].compare_exchange(
                    usize::MAX,
                    *val,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            if res.is_ok() {
                self.target.1.fetch_add(1, Ordering::Relaxed);
            } else {
                results.push(*val);
            }
        }
        results
    }
}

fn send_buffer(
    world: &lamellar::LamellarWorld,
    target: &Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    buffer_size: usize,
    buffer: &mut Vec<usize>,
    pe: usize,
) -> impl Future<Output = Vec<usize>> {
    let mut new_vec = Vec::with_capacity(buffer_size);
    std::mem::swap(buffer, &mut new_vec);
    let dart = DartAm {
        target: target.clone(),
        vals: new_vec,
    };
    world.exec_am_pe(pe, dart)
}

fn launch_darts(
    world: &lamellar::LamellarWorld,
    target: &Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    buffer_size: usize,
    buffers: &mut Vec<Vec<usize>>,
    rng: &mut SmallRng,
    num_pes: usize,
    indices: Box<dyn Iterator<Item = usize>>,
) -> FuturesUnordered<impl Future<Output = std::vec::Vec<usize>>> {
    let reqs = FuturesUnordered::new();
    for i in indices {
        let pe = rng.gen_range(0, num_pes);
        buffers[pe].push(i);
        if buffers[pe].len() == buffer_size {
            reqs.push(send_buffer(
                world,
                target,
                buffer_size,
                &mut buffers[pe],
                pe,
            ));
        }
    }
    //check if any data remaining in buffers and launch a dart am
    for pe in 0..num_pes {
        if buffers[pe].len() > 0 {
            reqs.push(send_buffer(
                world,
                target,
                buffer_size,
                &mut buffers[pe],
                pe,
            ));
        }
    }
    reqs
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::RandpermCli::parse();

    let global_count = cli.global_size;
    let local_count = global_count / num_pes;
    let iterations = cli.iterations;
    let buffer_size = cli.buffer_size;
    let target_factor = cli.target_factor;
    let launch_threads = cli.launch_threads;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let target_local_count = local_count * target_factor;

    let mut targets: Vec<AtomicUsize> = Vec::with_capacity(target_local_count);
    //initialize targets with max
    for _ in 0..target_local_count {
        targets.push(AtomicUsize::new(usize::MAX));
    }

    let targets = Darc::new(&world, (targets, AtomicUsize::new(0))).unwrap();
    let the_array = LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let local_lens = AtomicArray::new(&world, num_pes, lamellar::Distribution::Block);

    for _ in 0..iterations {
        let world2 = world.clone();
        let target2 = targets.clone();
        world.barrier();
        let start = Instant::now();
        world.block_on(
            the_array
                .local_iter()
                .enumerate()
                .chunks(local_count / launch_threads)
                .map(move |chunk| {
                    // let timer = Instant::now();
                    let target = target2.clone();
                    let world = world2.clone();
                    let mut thread_rng = thread_rng();
                    let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();

                    async move {
                        let mut buffers = vec![Vec::with_capacity(buffer_size); num_pes];
                        let mut reqs = launch_darts(
                            &world,
                            &target,
                            buffer_size,
                            &mut buffers,
                            &mut rng,
                            num_pes,
                            Box::new(chunk.map(move |(i, _)| i + my_pe * local_count)),
                        );

                        while reqs.len() > 0 {
                            let reqs_iter =
                                Box::new(reqs.collect::<Vec<_>>().await.into_iter().flatten());
                            reqs = launch_darts(
                                &world,
                                &target,
                                buffer_size,
                                &mut buffers,
                                &mut rng,
                                num_pes,
                                reqs_iter,
                            );
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
        // the_array.print();
    }
}
