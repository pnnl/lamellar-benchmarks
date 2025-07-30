// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeWAXPBY_ref.cpp

mod vector;
mod utils;

use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::LamellarTaskGroup;
use vector::{Vector, LocalLockVector};


#[lamellar::AmData(Clone, Debug)]
struct WAXBYAm {
    i: usize,
    w: LocalLockArray<f64>,
    alpha: f64,
    x: LocalLockArray<f64>,
    beta: f64,
    y: LocalLockArray<f64>
}

#[lamellar::am]
impl lamellar::LamellarAM for WAXBYAm {
    async fn exec(self) {
        let value = self.alpha * self.x.at(self.i).await + self.beta * self.y.at(self.i).await;
        self.w.store(self.i, value).await;
    }
}

async fn waxby_timed(world: &LamellarWorld, w:&LocalLockVector, alpha: f64, x: &LocalLockVector, beta: f64, y: &LocalLockVector) -> utils::Timing {
    let timing = utils::Timing::start("WAXBY");
    let _ = waxby(world, w, alpha, x, beta, y).await;
    let timing = timing.end();
    timing
}

async fn waxby(world: &LamellarWorld, w:&LocalLockVector, alpha:f64, x: &LocalLockVector, beta:f64, y: &LocalLockVector) {
    let start = x.values.first_global_index_for_pe(world.my_pe());
    let task_group = LamellarTaskGroup::new(world);

    if let Some(start) = start {
        let end = start + x.values.num_elems_local();
        for i in start..end {
            let (target_pe, _) = x.values.pe_and_offset_for_global_index(i).unwrap(); //Panic accepted because should be part of the global range by construction
            let _ = task_group.exec_am_pe(
                target_pe, 
                WAXBYAm {
                    i:i, 
                    w: w.values.clone(), 
                    alpha: alpha, 
                    x: x.values.clone(), 
                    beta:beta, 
                    y:y.values.clone()
                });
        }
    }

    task_group.await_all().await;
    world.barrier();
}


async fn async_main(world: &LamellarWorld) -> utils::Timing {
    let args: Vec<String> = std::env::args().collect();

    let values_magnitude:u32 = args.get(1).unwrap_or(&("3".to_owned())).parse().expect("Must supply an positive integer for values-magnitude (actual values-per-pe is 10**<supplied value>, default 3).");
    let alpha:f64 = args.get(1).unwrap_or(&("1.0".to_owned())).parse().expect("Must supply a float for alpha (default 1).");
    let beta:f64 = args.get(1).unwrap_or(&("1.0".to_owned())).parse().expect("Must supply a float for beta (default 1).");
    
    let values_per_pe = 10_usize.pow(values_magnitude);
    let num_pes = world.num_pes();

    let vector_size = num_pes * values_per_pe ;

    let mut w = LocalLockVector::new(world, vector_size).await;
    let x = LocalLockVector::new(world, vector_size).await;
    let y = LocalLockVector::new(world, vector_size).await;

    w.zero().await;
    x.fill_random().await;
    y.fill_random().await;

    waxby_timed(world, &mut w, alpha, &x, beta,  &y).await   
}


pub fn main() {
    let world = LamellarWorldBuilder::new().build();
    let w= async_main(&world);
    let timing = world.block_on(w);

    let my_pe = world.my_pe();
    if my_pe == 0 {
        println!("{timing}")
    }
}