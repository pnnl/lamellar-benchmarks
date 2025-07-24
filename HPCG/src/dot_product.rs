// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.cpp 
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.cpp

mod vector;
mod utils;

use lamellar::array::prelude::*;
use vector::{Vector, LamellarVector};

async fn compute_dot_product(x: &impl Vector, y: &impl Vector) -> (f64, utils::Timing) {
    let timing = utils::Timing::start("Dot Product");
    let result = 0.0;
    let timing = timing.end();
    (result, timing)
}

async fn async_main(world: &LamellarWorld) -> (f64, utils::Timing) {
    let values_per_pe = 1000;
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let vector_size = num_pes * values_per_pe ;

    let x = LamellarVector::new(world, vector_size).await;
    let y = LamellarVector::new(world, vector_size).await;

    x.fill_random().await;
    y.fill_random().await;

    compute_dot_product(&x, &y).await
}

pub fn main() {
    let world = LamellarWorldBuilder::new().build();
    let w= async_main(&world);
    let (result, timing) = world.block_on(w);

    println!("Result: {result}");
    println!("{timing}")

}