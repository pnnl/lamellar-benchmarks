// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeWAXPBY_ref.cpp

mod vector;
mod utils;

use lamellar::array::prelude::*;
use vector::{Vector, LamellarVector};

async fn waxby_timed(world: &LamellarWorld, w:&mut impl Vector, alpha: f64, x: &impl Vector, beta: f64, y: &impl Vector) -> utils::Timing {
    let timing = utils::Timing::start("WAXBY");
    let result = waxby(world, w, alpha, x, beta, y).await;
    let timing = timing.end();
    timing
}

#[lamellar::AmData(Clone, Debug)]
struct WAXBY_AM {
    i: usize,
    w: &Vector,
    a: f64,
    x: &Vector,
    b: f64,
    y: &Vector,
}

impl lamellar::LamellarAM for WAXBY_AM {
    async fn exec(self) {
        let w_local = self.w.write_local_values();
        let x_local = self.x.local_values();
        let y_local = self.y.local_values();
        let local_i = self.i - self.offset;

        //NOTE: This local_i game implies that all of the arrays are identically distributed...which they may not be.
        w_local[local_i] = self.a * x_local[local_i] + self.b * y_local[local_i];
    }
}

async fn waxby(world: &LamellarWorld, w:&mut impl Vector, alpha:f64, x: &impl Vector, beta:f64, y: &impl Vector) {
    let my_pe = world.my_pe();
    if my_pe == 0 {
        for i in 0..w.len() {
            world.exec_am(WAXBY_AM {i, w, alpha, x, beta, y})
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

    let mut w = LamellarVector::new(world, vector_size).await;
    let x = LamellarVector::new(world, vector_size).await;
    let y = LamellarVector::new(world, vector_size).await;

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