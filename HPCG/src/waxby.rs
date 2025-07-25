// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeWAXPBY_ref.cpp

mod vector;
mod utils;

use lamellar::array::prelude::*;
use vector::{Vector, LamellarVector};

async fn waxby_timed(world: &LamellarWorld, w:&mut impl Vector, alpha: f64, x: &impl Vector, beta: f64, y: &impl Vector) -> utils::Timing {
    let timing = utils::Timing::start("WAXBY");
    waxby(world, w, alpha, x, beta, y).await;
    let timing = timing.end();
    timing
}

async fn waxby(world: &LamellarWorld, w:&mut impl Vector, alpha: f64, x: &impl Vector, beta: f64, y: &impl Vector) {
    // TODO: If it took LamellarVector instead of Vector could skip the local_valus game
    // and just do w.values.store(i, alpha*x.values.at(i) + beta * y.values.at(i))
    // WHY am I using 'Vector'?  Is there a proper generic LamellarArray trait that gives 'at' and 'store'?
    
    let mut w_local = w.mut_local_values().await;
    let x_local = x.local_values().await;
    let y_local = y.local_values().await;

    for i in 0..w_local.len() {
        w_local[i] = alpha * x_local[i] + beta * y_local[i];
    }

    world.barrier();
}

async fn async_main(world: &LamellarWorld) -> utils::Timing {
    let args: Vec<String> = std::env::args().collect();

    let values_magnitude:u32 = args.get(1).unwrap_or(&("3".to_owned())).parse().expect("Must supply an positive integer for values-magnitude (actual values-per-pe is 10**<supplied value>, default 3).");
    let values_per_pe = 10_usize.pow(values_magnitude);
    let num_pes = world.num_pes();

    let vector_size = num_pes * values_per_pe ;

    let mut w = LamellarVector::new(world, vector_size).await;
    let x = LamellarVector::new(world, vector_size).await;
    let y = LamellarVector::new(world, vector_size).await;

    let alpha = 1.0;
    let beta = 1.0;

    w.zero().await;
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