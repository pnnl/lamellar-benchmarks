// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.cpp 
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.cpp

mod vector;
mod utils;

use lamellar::array::prelude::*;
use vector::{Vector, LamellarVector};

async fn compute_dot_product_timed(world: &LamellarWorld, x: &impl Vector, y: &impl Vector) -> (f64, utils::Timing) {
    let timing = utils::Timing::start("Dot Product");
    let result = compute_dot_product(world, x, y).await;
    let timing = timing.end();
    (result, timing)
}

async fn compute_dot_product(world: &LamellarWorld, x: &impl Vector, y: &impl Vector) -> f64 {
    let global_result = AtomicArray::new(world, 1, lamellar::array::Distribution::Block).await; //TODO: I suspect this should be a darc
    let local_x = x.local_values().await;
    let local_y = y.local_values().await;
    let local_result = local_x.iter().zip(local_y.iter()).map(|(a,b)| a*b).into_iter().fold(1.0, |a, e| a+e);
    global_result.add(0,local_result).await;
    world.barrier();
    global_result.at(0).await
}

async fn async_main(world: &LamellarWorld) -> (f64, utils::Timing) {
    let args: Vec<String> = std::env::args().collect();

    let values_magnitude:u32 = args.get(1).unwrap_or(&("3".to_owned())).parse().expect("Must supply an positive integer for values-magnitude (actual values-per-pe is 10**<supplied value>, default 3).");
    let values_per_pe = 10_usize.pow(values_magnitude);
    let num_pes = world.num_pes();

    let vector_size = num_pes * values_per_pe ;

    let x = LamellarVector::new(world, vector_size).await;
    let y = LamellarVector::new(world, vector_size).await;

    x.fill_random().await;
    y.fill_random().await;

    compute_dot_product_timed(world, &x, &y).await
}

pub fn main() {
    let world = LamellarWorldBuilder::new().build();
    let w= async_main(&world);
    let (result, timing) = world.block_on(w);

    let my_pe = world.my_pe();
    println!("{my_pe}");
    if my_pe == 0 {
        println!("Result: {result}");
        println!("{timing}")
    }
}