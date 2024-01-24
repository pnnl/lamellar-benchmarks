use crate::options::IndexGatherCli;
use lamellar::array::prelude::*;

use clap::ValueEnum;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub enum ArrayType {
    Unsafe,
    Atomic,
    LocalLock,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum ArrayDistribution {
    Block,
    Cyclic,
}
impl Into<lamellar::Distribution> for &ArrayDistribution {
    fn into(self) -> lamellar::Distribution {
        match self {
            ArrayDistribution::Block => lamellar::Distribution::Block,
            ArrayDistribution::Cyclic => lamellar::Distribution::Cyclic,
        }
    }
}
pub fn index_gather<'a>(
    world: &lamellar::LamellarWorld,
    ig_config: &IndexGatherCli,
    rand_indices: &Arc<Vec<usize>>,
    array_type: ArrayType,
    distribution: &ArrayDistribution,
) -> (Duration, Duration, Duration, Duration) {
    let num_pes = world.num_pes();
    std::env::set_var(
        "LAMELLAR_BATCH_OP_THREADS",
        format!("{}", ig_config.launch_threads),
    );
    std::env::set_var("LAMELLAR_OP_BATCH", format!("{}", ig_config.buffer_size));
    world.barrier();
    let mut timer = Instant::now();

    let index_gather_request = match array_type {
        ArrayType::Unsafe => {
            let array = UnsafeArray::<usize>::new(
                world,
                ig_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();
            //the actual index_gather operation
            array.batch_load(rand_indices.as_slice())
        }
        ArrayType::Atomic => {
            let array = AtomicArray::<usize>::new(
                world,
                ig_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();

            //the actual index_gather operation
            array.batch_load(rand_indices.as_slice())
        }
        ArrayType::LocalLock => {
            let array = LocalLockArray::<usize>::new(
                world,
                ig_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();

            //the actual index_gather operation
            array.batch_load(rand_indices.as_slice())
        }
    };

    let launch_issue_time = timer.elapsed();
    let launch_finish_time = timer.elapsed();
    world.block_on(index_gather_request);
    let local_finish_time = timer.elapsed();
    world.barrier();
    let global_finish_time = timer.elapsed();
    (
        launch_issue_time,
        launch_finish_time,
        local_finish_time,
        global_finish_time,
    )
}
