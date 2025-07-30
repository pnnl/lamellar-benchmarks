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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_utils::WORLD;

    fn do_test(xv: f64, alpha: f64, yv: f64, beta:f64, expected:f64, msg: &str) {
        let size = 100;
        let w = LocalLockVector::new_now(&WORLD, size);
        let task = w.fill(&WORLD, -1.0);
        WORLD.block_on(task);

        let x = LocalLockVector::new_now(&WORLD, size);
        let task = x.fill(&WORLD, xv);
        WORLD.block_on(task);

        let y = LocalLockVector::new_now(&WORLD, size);
        let task = y.fill(&WORLD, yv);
        WORLD.block_on(task);

        let task = waxby_timed(&WORLD,  &w, alpha, &x, beta, &y);
        let _time = WORLD.block_on(task);
        
        for e in w.values.onesided_iter().into_iter() {
            assert_eq!(*e, expected, "WAXBY test: {}", msg);
        }        
    }

    #[test]
    fn test_waxby_am_various() {
        do_test(1.0, 2.0, 1.0, 3.0, 5.0, "ones");
        do_test(0.0, 2.0, 1.0, 3.0, 3.0, "no x");
        do_test(1.0, 2.0, 0.0, 3.0, 2.0, "no y");
        do_test(0.0, 2.0, 0.0, 3.0, 0.0, "no x and no y");
    }
}
