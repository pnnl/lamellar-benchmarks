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
// and then once we arrive we simply append it to the end of the vector

//------ Safe AMs -----------
#[lamellar::AmData]
struct PushDartAmGroup {
    #[AmGroup(static)]
    target: LocalRwDarc<Vec<usize>>,
    val: usize,
}

#[lamellar::am]
impl LamellarAM for PushDartAmGroup {
    async fn exec(self) {
        self.target.write().await.push(self.val);
    }
}

//--------------------------

// We likely want to issue updates from multiple threads to improve performance
// we can use a local Active Messages to do this.

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    val_start: usize,
    val_end: usize,
    target: LocalRwDarc<Vec<usize>>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let mut am_group = typed_am_group! {PushDartAmGroup, self.target.team()};
        {
            //need to scope so we drop rng
            let mut thread_rng = thread_rng();
            let mut rng = SmallRng::from_rng(&mut thread_rng).unwrap();

            for val in (self.val_start..self.val_end)
                .choose_multiple(&mut rng, self.val_end - self.val_start)
                .iter()
            {
                let pe = rng.gen_range(0, lamellar::num_pes);
                am_group.add_am_pe(
                    pe,
                    PushDartAmGroup {
                        target: self.target.clone(),
                        val: *val,
                    },
                );
            }
        }
        am_group.exec().await;
    }
}

fn launch_ams(
    world: &LamellarWorld,
    rand_perm_config: &RandPermCli,
    target: LocalRwDarc<Vec<usize>>,
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
    std::env::set_var(
        "LAMELLAR_BATCH_OP_SIZE",
        format!("{}", rand_perm_config.buffer_size),
    );
    world.barrier();
    let mut timer = Instant::now();
    let target = LocalRwDarc::new(
        world,
        Vec::with_capacity(rand_perm_config.pe_table_size(num_pes)),
    )
    .expect("darc should be created");
    world.barrier();
    let _init_time = timer.elapsed();
    timer = Instant::now();
    let launch_tasks = launch_ams(world, rand_perm_config, target.clone());
    world.block_on(launch_tasks);
    world.wait_all();
    world.barrier();
    let perm_time = timer.elapsed();
    let target = target.blocking_into_darc();
    let collect_timer = Instant::now();
    let mut data = Vec::with_capacity(target.len());
    data.extend_from_slice(&target);
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
