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

#[lamellar::AmData]
struct FillAm {
    target: LocalRwDarc<Vec<usize>>,
    the_array: LocalLockArray<usize>,
    start_index: usize,
}

#[lamellar::am]
impl LamellarAm for FillAm {
    async fn exec(self) {
        let target = self.target.read();
        // let put_req = unsafe { self.the_array.put(self.start_index, target.as_ref()) };
        if lamellar::current_pe < lamellar::num_pes - 1 {
            let req = lamellar::world.exec_am_pe(
                lamellar::current_pe + 1,
                FillAm {
                    target: self.target.clone(),
                    the_array: self.the_array.clone(),
                    start_index: self.start_index + target.len(),
                },
            );
            // .await;
            unsafe {
                self.the_array.put(self.start_index, target.as_ref()).await;
            };
            req.await;
        } else {
            unsafe { self.the_array.put(self.start_index, target.as_ref()).await };
        }
        // put_req.await;
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
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_count = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000); //size of permuted array
                                  // let target_factor = args
                                  //     .get(2)
                                  //     .and_then(|s| s.parse::<usize>().ok())
                                  //     .unwrap_or_else(|| 10); //multiplication factor for target array
    let target_factor = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 10); //multiplication factor for target array
    let buffer_size = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000); //multiplication factor for target array
    let iterations = args
        .get(4)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1);
    let num_threads = args
        .get(5)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });

    if my_pe == 0 {
        println!("array size {}", global_count);
        // println!("target array size {}", global_count * target_factor);
    }

    let local_count = (global_count) / num_pes;

    let the_array: LocalLockArray<usize> =
        LocalLockArray::new(&world, global_count, lamellar::Distribution::Block);
    let local_lens = AtomicArray::new(&world, num_pes, lamellar::Distribution::Block);

    let target = LocalRwDarc::new(&world, Vec::with_capacity(local_count * target_factor)).unwrap();

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
                .chunks(std::cmp::max(1, local_count / num_threads))
                .map(move |chunk| {
                    // println!("{} {:?}",local_count/num_threads, std::thread::current().id());
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
                        chunk.map(|&i| {
                            // println!("{:?} {} ", std::thread::current().id(), i);
                            i
                        }),
                        std::cmp::max(1, local_count / num_threads),
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
