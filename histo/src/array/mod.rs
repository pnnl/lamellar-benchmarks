use crate::options::HistoCli;
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
pub fn histo<'a>(
    world: &lamellar::LamellarWorld,
    histo_config: &HistoCli,
    rand_indices: &Arc<Vec<usize>>,
    array_type: ArrayType,
    distribution: &ArrayDistribution,
) -> (Duration, Duration, Duration, Duration) {
    let num_pes = world.num_pes();
    std::env::set_var(
        "LAMELLAR_BATCH_OP_THREADS",
        format!("{}", histo_config.launch_threads),
    );
    std::env::set_var("LAMELLAR_BATCH_OP_SIZE", format!("{}", histo_config.buffer_size));
    world.barrier();
    let mut timer = Instant::now();

    //TODO specify that we want usize arrays
    let histo_request = match array_type {
        ArrayType::Unsafe => {
            let array: UnsafeArray<usize> = UnsafeArray::new(
                world,
                histo_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();
            //the actual histo operation
            array.batch_add(rand_indices.as_slice(), 1)
        }
        ArrayType::Atomic => {
            let array: AtomicArray<usize> = AtomicArray::new(
                world,
                histo_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();

            //the actual histo operation
            array.batch_add(rand_indices.as_slice(), 1)
        }
        ArrayType::LocalLock => {
            let array: LocalLockArray<usize> = LocalLockArray::new(
                world,
                histo_config.total_table_size(num_pes),
                distribution.into(),
            );
            let _init_time = timer.elapsed();
            timer = Instant::now();

            //the actual histo operation
            array.batch_add(rand_indices.as_slice(), 1)
        }
    };

    let launch_issue_time = timer.elapsed();
    let launch_finish_time = timer.elapsed();
    world.block_on(histo_request);
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
