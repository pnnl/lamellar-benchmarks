mod options;
use clap::Parser;

use futures::Future;
use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use rand::prelude::*;

use std::time::Instant;

#[lamellar::AmData]
struct DartAm {
    #[AmGroup(static)]
    target: LocalRwDarc<Vec<usize>>,
    val: usize,
}

#[lamellar::am]
impl LamellarAm for DartAm {
    async fn exec(self) {
        self.target.write().push(self.val);
    }
}

fn launch_darts<I: Iterator<Item = usize>>(
    world: &lamellar::LamellarWorld,
    target: &LocalRwDarc<Vec<usize>>,
    rng: &mut SmallRng,
    num_pes: usize,
    indices: I,
    chunk_size: usize,
) -> impl Future<Output = TypedAmGroupResult<()>> {
    let mut darts = typed_am_group!(DartAm, world);
    for i in indices.choose_multiple(rng, chunk_size).iter().map(|&i| i) {
        let pe = rng.gen_range(0, num_pes);
        darts.add_am_pe(
            pe,
            DartAm {
                target: target.clone(),
                val: i,
            },
        );
    }
    darts.exec()
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
    let target_factor = cli.target_factor;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    if my_pe == 0 {
        println!("array size {}", global_count);
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
            // the_array.local_iter()
            the_array
                .local_iter()
                .chunks(std::cmp::max(1, local_count / launch_threads))
                .map(move |chunk| {
                    // println!("{} {:?}",local_count/launch_threads, std::thread::current().id());
                    let mut thread_rng = thread_rng();
                    let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
                    launch_darts(
                        &world2,
                        &target2,
                        &mut rng,
                        num_pes,
                        chunk.map(|&i| i),
                        std::cmp::max(1, local_count / launch_threads),
                    )
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
