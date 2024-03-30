use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;

use crate::options::RandPermCli;

use rand::prelude::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

// In this variant, we randomly select a remote PE to launch the dart
// and then once we arrive to the PE we randomly select the index,
// repeating this process until the dart as landed, or this PE is full

//------ Safe AMs -----------
#[lamellar::AmData]
struct CasDartAm {
    target: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
    val: usize,
}

#[lamellar::am]
impl LamellarAM for CasDartAm {
    async fn exec(self) {
        let mut thread_rng = thread_rng();
        let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
        let local_len = self.target.0.len();

        let mut res: Result<usize, usize> = Err(0); // incase this pe is already full.
        while res.is_err() && self.target.1.load(Ordering::Relaxed) < local_len {
            let index = rng.gen_range(0, local_len);
            res = self.target.0[index].compare_exchange(
                usize::MAX,
                self.val,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        if res.is_err() {
            //this PE is full but we still have a dart
            let pe = rng.gen_range(0, lamellar::num_pes);
            let _ = lamellar::world.exec_am_pe(
                pe,
                CasDartAm {
                    target: self.target.clone(),
                    val: self.val,
                },
            );
        } else {
            self.target.1.fetch_add(1, Ordering::Relaxed);
        }
    }
}

//--------------------------

// We likely want to issue updates from multiple threads to improve performance
// we can use a local Active Messages to do this.

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    val_start: usize,
    val_end: usize,
    target: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let mut thread_rng = thread_rng();
        let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();
        for val in self.val_start..self.val_end {
            let pe = rng.gen_range(0, lamellar::num_pes);
            let _ = lamellar::world.exec_am_pe(
                pe,
                CasDartAm {
                    target: self.target.clone(),
                    val,
                },
            );
        }
    }
}

fn launch_ams(
    world: &LamellarWorld,
    rand_perm_config: &RandPermCli,
    target: Darc<(Vec<AtomicUsize>, AtomicUsize)>,
) -> Pin<Box<dyn Future<Output = Vec<()>>>> {
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let slice_size =
        rand_perm_config.pe_table_size(num_pes) as f32 / rand_perm_config.launch_threads as f32;
    let mut launch_tasks = vec![];

    for tid in 0..rand_perm_config.launch_threads {
        let start = (tid as f32 * slice_size).round() as usize
            + (my_pe * rand_perm_config.pe_table_size(num_pes));
        let end = (tid as f32 * slice_size + slice_size).round() as usize
            + (my_pe * rand_perm_config.pe_table_size(num_pes));
        launch_tasks.push(world.exec_am_local(LaunchAm {
            val_start: start,
            val_end: end,
            target: target.clone(),
        }));
    }
    Box::pin(futures::future::join_all(launch_tasks))
}

pub fn rand_perm<'a>(
    world: &lamellar::LamellarWorld,
    rand_perm_config: &RandPermCli,
) -> (Duration, Duration, Duration, usize) {
    let num_pes = world.num_pes();
    let local_lens = AtomicArray::new(world, world.num_pes(), lamellar::Distribution::Block);
    let the_array =
        LocalRwDarc::new(world, vec![0; rand_perm_config.pe_table_size(num_pes)]).unwrap();
    world.barrier();
    let mut timer = Instant::now();
    // let (_init_time, launch_tasks) = if safe {
    let target_size = rand_perm_config.pe_table_size(num_pes) * rand_perm_config.target_factor;
    let mut target_inner = Vec::with_capacity(target_size);
    for _ in 0..target_size {
        target_inner.push(AtomicUsize::new(usize::MAX));
    }
    let target =
        Darc::new(world, (target_inner, AtomicUsize::new(0))).expect("darc should be created");
    world.barrier();
    let _init_time = timer.elapsed();
    timer = Instant::now();
    let launch_tasks = launch_ams(world, rand_perm_config, target.clone());
    world.block_on(launch_tasks);
    world.wait_all();
    world.barrier();
    let perm_time = timer.elapsed();

    let collect_timer = Instant::now();
    let data = target
        .0
        .iter()
        .map(|x| x.load(Ordering::Relaxed))
        .filter(|x| *x != usize::MAX)
        .collect::<Vec<_>>();
    super::collect_perm(world, rand_perm_config, data, &the_array, &local_lens);
    let collect_time = collect_timer.elapsed();

    let global_finish_time = timer.elapsed();

    let sum = Darc::new(world, AtomicUsize::new(0)).expect("darc should be created");
    let local_sum = world.block_on(the_array.read()).iter().sum::<usize>();

    let _ = world.exec_am_pe(
        0,
        super::SumAm {
            sum: sum.clone(),
            amt: local_sum,
        },
    );
    world.wait_all();
    world.barrier();

    (
        perm_time,
        collect_time,
        global_finish_time,
        sum.load(Ordering::Relaxed),
    )
}
