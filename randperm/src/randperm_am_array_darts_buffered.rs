mod options;
use clap::Parser;

use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;

use rand::prelude::*;

use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    #[AmGroup(static)]
    target: AtomicArray<usize>,
    vals: Vec<usize>,
}

#[lamellar::am]
impl LamellarAm for DartAm {
    async fn exec(self) -> Vec<usize> {
        //create a random index less than the length of indices
        let thread_rng = thread_rng();
        let mut rng = thread_rng;
        // let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
        let local_target = self.target.local_data();
        let mut index = rng.gen_range(0, local_target.len());

        let mut results = vec![];

        for val in self.vals.iter() {
            let mut max_retry = 5;
            //while compare and exhange using the index where to origial val is 0 and the new val is 1 fails
            let mut res = local_target.at(index).compare_exchange(usize::MAX, *val);
            while res.is_err() && max_retry > 0 {
                index = rng.gen_range(0, local_target.len());
                res = local_target.at(index).compare_exchange(usize::MAX, *val);
                max_retry -= 1;
            }
            if res.is_err() {
                results.push(*val);
            }
        }
        results
    }
}

fn send_buffer(
    world: &lamellar::LamellarWorld,
    target: &AtomicArray<usize>,
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
    target: &AtomicArray<usize>,
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
    let launch_threads = cli.launch_threads;
    let buffer_size = cli.buffer_size;
    let target_factor = cli.target_factor;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    if my_pe == 0 {
        println!("array size {}", global_count);
        println!("target array size {}", global_count * target_factor);
    }

    let target = AtomicArray::new(
        &world,
        global_count * target_factor,
        lamellar::Distribution::Block,
    );

    let init_array = world.block_on(
        target
            .dist_iter()
            .enumerate()
            .filter_map(move |(i, e)| {
                e.store(usize::MAX);
                if i % target_factor == 0 {
                    Some(i / target_factor)
                } else {
                    None
                }
            })
            .collect::<ReadOnlyArray<usize>>(lamellar::Distribution::Block),
    );

    for _ in 0..iterations {
        let world2 = world.clone();
        let target2 = target.clone();
        world.barrier();
        let start = Instant::now();
        world.block_on(
            init_array
                .local_iter()
                .chunks(local_count / launch_threads)
                .map(move |chunk| {
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
                            Box::new(chunk.map(|i| *i)),
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
                .for_each_async(|req| async { req.await }),
        );
        world.wait_all();
        if my_pe == 0 {
            println!(
                "PE:{my_pe}  local run time {:?} ",
                start.elapsed().as_secs_f64()
            );
        }
        world.barrier(); //all work is done
        if my_pe == 0 {
            println!("permute time {:?}s ", start.elapsed().as_secs_f64());
        }

        let collect_start = Instant::now();
        let the_array = world.block_on(
            target
                .dist_iter()
                .filter_map(|elem| {
                    let elem = elem.load(); //elements are atomic so we cant just read directly
                    if elem < usize::MAX {
                        Some(elem)
                    } else {
                        None
                    }
                })
                .collect::<ReadOnlyArray<usize>>(lamellar::array::Distribution::Block),
        );
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

        //reset target array
        world.block_on(target.dist_iter_mut().for_each(|elem| {
            elem.store(usize::MAX);
        }));
        world.barrier();
    }
}
