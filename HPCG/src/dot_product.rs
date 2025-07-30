// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.cpp 
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.cpp

mod vector;
mod utils;

use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use vector::{Vector, LocalLockVector};


async fn compute_dot_product_timed(world: &LamellarWorld, x: &LocalLockVector, y: &LocalLockVector) -> (f64, utils::Timing) {
    let timing = utils::Timing::start("Dot Product");
    let result = compute_dot_product(world, x, y).await;
    let timing = timing.end();
    (result, timing)
}


async fn compute_dot_product(world: &LamellarWorld, x: &LocalLockVector, y: &LocalLockVector) -> f64 {
    let global_result = AtomicArray::new(world, 1, lamellar::array::Distribution::Block).await;

    let my_pe = world.my_pe();
    let mut local_sum = 0.0;
    for i in 0..x.len() {
        let (pe, _) = x.values.pe_and_offset_for_global_index(i).unwrap(); //Panic unlikely because should be part of the global range by construction
        if pe == my_pe {
            local_sum += x.values.at(i).await * y.values.at(i).await;
        }
    }
    global_result.add(0, local_sum).await;
    global_result.at(0).await
}

async fn async_main(world: &LamellarWorld) -> (f64, utils::Timing) {
    let args: Vec<String> = std::env::args().collect();

    let values_magnitude:u32 = args.get(1).unwrap_or(&("3".to_owned())).parse().expect("Must supply an positive integer for values-magnitude (actual values-per-pe is 10**<supplied value>, default 3).");
    let values_per_pe = 10_usize.pow(values_magnitude);
    let num_pes = world.num_pes();

    let vector_size = num_pes * values_per_pe ;

    let x = LocalLockVector::new(world, vector_size).await;
    let y = LocalLockVector::new(world, vector_size).await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::WORLD;

    fn test_dot_product(size: usize, av: f64, bv: f64, expected: f64, msg: &str) {
        let a = LocalLockVector::new_now(&WORLD, size);
        let task = a.fill(&WORLD, av);
        WORLD.block_on(task);

        let b = LocalLockVector::new_now(&WORLD, size);
        let task = b.fill(&WORLD, bv);
        WORLD.block_on(task);

        let task = compute_dot_product_timed(&WORLD, &a, &b);
        let (result, _time) = WORLD.block_on(task);
        assert_eq!(result, expected, "Dot Product: {}", msg);        
    }

    #[test]
    fn test_various_dot_products() {
        test_dot_product(100, 1.0, 1.0, 100.0, "Ones x100");
        test_dot_product(50, 1.0, 1.0, 50.0, "Ones x50");
        test_dot_product(100, 2.0, 2.0, 400.0, "Twos");
        test_dot_product(100, 1.0, 2.0, 200.0, "a=1, b=2");
    } 
}
