// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.cpp 
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.hpp
// and https://github.com/hpcg-benchmark/hpcg/blob/master/src/ComputeDotProduct_ref.cpp

mod vector;
mod utils;

use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::array::LocalLockArray;
use lamellar::LamellarTaskGroup;
use vector::{Vector, LocalLockVector};


#[lamellar::AmData(Clone, Debug)]
struct DotProductAM {
    i: usize,
    x: LocalLockArray<f64>,
    y: LocalLockArray<f64>,
    global_result: AtomicArray<f64> 
}

#[lamellar::am]
impl lamellar::LamellarAM for DotProductAM {
    async fn exec(self) {
        let xv = self.x.at(self.i);
        let yv = self.y.at(self.i);
        let product = xv.await * yv.await;
        self.global_result.add(0, product).await;
    }
}

async fn compute_dot_product_timed(world: &LamellarWorld, x: &LocalLockVector, y: &LocalLockVector) -> (f64, utils::Timing) {
    let timing = utils::Timing::start("Dot Product");
    let result = compute_dot_product(world, x, y).await;
    let timing = timing.end();
    (result, timing)
}


async fn compute_dot_product(world: &LamellarWorld, x: &LocalLockVector, y: &LocalLockVector) -> f64 {
    let global_result = AtomicArray::new(world, 1, lamellar::array::Distribution::Block).await;
    let task_group = LamellarTaskGroup::new(world);

    let start = x.values.first_global_index_for_pe(world.my_pe());
    if let Some(start) = start {
        let end = start + x.values.num_elems_local();
        for i in start..end {
            let (target_pe, _) = x.values.pe_and_offset_for_global_index(i).unwrap(); //Panic accepted because should be part of the global range by construction
            let _ = task_group.exec_am_pe(
                target_pe,
                DotProductAM {
                    i: i,
                    x: x.values.clone(),
                    y: y.values.clone(),
                    global_result: global_result.clone()
                }
            );
        }
    }

    task_group.await_all().await;
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

    fn test_ones() {
        let world = LamellarWorldBuilder::new().build();

        let size = 100;
        let v1 = LocalLockVector::new_now(&world, size);
        let w = v1.ones();
        world.block_on(w);

        let v2 = LocalLockVector::new_now(&world, size);
        let w = v2.ones();
        world.block_on(w);

        let w = compute_dot_product_timed(&world, &v1, &v2);
        let (result, _time) = world.block_on(w);
        assert_eq!(result, 1.0);
    }
}
