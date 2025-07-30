// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeWAXPBY_ref.cpp

mod vector;
mod utils;

use lamellar::array::prelude::*;
use vector::{Vector, LocalLockVector};

async fn waxby_timed(world: &LamellarWorld, w:&LocalLockVector, alpha: f64, x: &LocalLockVector, beta: f64, y: &LocalLockVector) -> utils::Timing {
    let timing = utils::Timing::start("WAXBY");
    waxby(world, w, alpha, x, beta, y).await;
    let timing = timing.end();
    timing
}

async fn waxby(world: &LamellarWorld, w:&LocalLockVector, alpha: f64, x: &LocalLockVector, beta: f64, y: &LocalLockVector) {    
    let my_pe = world.my_pe();

    for i in 0..x.len() {
        let (pe, _) = x.values.pe_and_offset_for_global_index(i).unwrap(); //Panic unlikely because should be part of the global range by construction
        if pe == my_pe {
            let value = alpha * x.values.at(i).await + beta * y.values.at(i).await;
            let _ = w.values.store(i, value).await;
        }
    }
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

    w.zero(world).await;
    x.fill_random().await;
    y.fill_random().await;

    waxby_timed(world, &mut w, alpha, &x, beta, &y).await   
}


pub fn main() {
    let world = LamellarWorldBuilder::new().build();
    let work = async_main(&world);
    let timing = world.block_on(work);

    let my_pe = world.my_pe();
    if my_pe == 0 {
        // println!("Result: {result}");
        println!("{timing}")
    }
}