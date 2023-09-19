mod options;
use clap::Parser;

use futures::stream::FuturesUnordered;
use futures::Future;
use futures::StreamExt;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use rand::prelude::*;

use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    target: LocalRwDarc<Vec<usize>>,
    vals: Vec<usize>,
}

#[lamellar::am]
impl LamellarAm for DartAm {
    async fn exec(self) {
        self.target.write().extend_from_slice(&self.vals);
    }
}

fn send_buffer(
    world: &lamellar::LamellarWorld,
    target: &LocalRwDarc<Vec<usize>>,
    buffer_size: usize,
    buffer: &mut Vec<usize>,
    pe: usize,
) -> impl Future<Output = ()> {
    let mut new_vec = Vec::with_capacity(buffer_size);
    std::mem::swap(buffer, &mut new_vec);
    let dart = DartAm {
        target: target.clone(),
        vals: new_vec,
    };
    world.exec_am_pe(pe, dart)
}

fn launch_darts<I: Iterator<Item = usize>>(
    world: &lamellar::LamellarWorld,
    target: &LocalRwDarc<Vec<usize>>,
    buffer_size: usize,
    buffers: &mut Vec<Vec<usize>>,
    rng: &mut SmallRng,
    num_pes: usize,
    indices: I,
    chunk_size: usize,
) -> FuturesUnordered<impl Future<Output = ()>> {
    let reqs = FuturesUnordered::new();
    for i in indices.choose_multiple(rng, chunk_size).iter().map(|&i| i) {
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
        // println!("target array size {}", global_count * target_factor);
    }

    let target_local_count = local_count * target_factor;

    let the_array: LocalLockArray<usize> =
        LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let local_lens = AtomicArray::new(&world, num_pes, lamellar::Distribution::Block);

    let target = LocalRwDarc::new(&world, Vec::with_capacity(target_local_count)).unwrap();

    for _ in 0..iterations {
        //reset the array and target array
        world.block_on(
            the_array
                .dist_iter_mut()
                .enumerate()
                .for_each(|(i, e)| *e = i),
        );
        target.write().clear();

        let world2 = world.clone();
        let target2 = target.clone();
        world.barrier();
        let start = Instant::now();
        world.block_on(
            the_array
                .local_iter()
                .chunks(std::cmp::max(1, local_count / launch_threads))
                .map(move |chunk| {
                    let mut thread_rng = thread_rng();
                    let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
                    let mut buffers = vec![Vec::with_capacity(buffer_size); num_pes];
                    let reqs = launch_darts(
                        &world2,
                        &target2,
                        buffer_size,
                        &mut buffers,
                        &mut rng,
                        num_pes,
                        chunk.map(|&i| i),
                        std::cmp::max(1, local_count / launch_threads),
                    );
                    reqs.collect::<Vec<_>>()
                })
                .for_each_async(|req| async {
                    req.await;
                }),
        );
        world.wait_all();

        if my_pe == 0 {
            println!("PE:{my_pe}  local run time {:?} ", start.elapsed(),);
        }
        world.barrier(); //all work is done
        if my_pe == 0 {
            println!("permute time {:?}s ", start.elapsed().as_secs_f64());
        }
        let collect_start = Instant::now();
        target.write().shuffle(&mut thread_rng());

        local_lens.local_data().at(0).store(target.read().len());
        world.barrier();
        let start_index = local_lens
            .buffered_onesided_iter(num_pes)
            .into_iter()
            .take(my_pe)
            .sum::<usize>();
        world.block_on(unsafe { the_array.put(start_index, target.read().as_ref()) });
        world.barrier(); //all work is done
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
        world.barrier();
    }
}
