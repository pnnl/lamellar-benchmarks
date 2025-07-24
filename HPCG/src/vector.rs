/// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/Vector.hpp

use lamellar::array::prelude::*;
use lamellar::array::LocalLockArray;
use rand::prelude::*;
use std::ops::{DerefMut, Deref};

/// Dense vector trait members
pub trait Vector {
    async fn mut_local_values(&mut self) -> impl DerefMut<Target=[f64]>;
    async fn local_values(&mut self) -> impl Deref<Target=[f64]>;

    /// Fill vector with zero values.  
    async fn zero(&self);

    /// Multiply a single index's value by the passed value
    async fn scale_value(&self, index:usize, value:f64);

    ///Fill a vector with psuedo-random values between 0 and 1
    async fn fill_random(&self);

    /// Copy the contents of a this (input) vector to the passed (target) vector.
    async fn copy(&self, target:&mut impl Vector);
}



pub struct LamellarVector {
    values: LocalLockArray<f64>,
    optimization_data: Option<usize> // placeholder for later use
}

impl LamellarVector {
    pub async fn new(world: &LamellarWorld, size: usize) -> LamellarVector {
        LamellarVector {
            values: LocalLockArray::new(world, size,
                lamellar::array::Distribution::Block,
                ).await,
            optimization_data: Option::None
        }
    }

    #[allow(dead_code)]
    pub fn new_now(world: &LamellarWorld, size: usize) -> LamellarVector {
        let w = Self::new(world, size);
        world.block_on(w)
    }

}

impl Vector for LamellarVector {
    async fn mut_local_values(&mut self) -> impl DerefMut<Target=[f64]>{
        self.values.write_local_data().await
    }

    async fn local_values(&mut self) -> impl Deref<Target=[f64]> {
        self.values.read_local_data().await
    }


    async fn zero(&self) {
        let mut local_data=self.values.write_local_data().await;
        local_data.iter_mut().for_each(|elem| *elem = 0.0);
    }

    async fn scale_value(&self, index:usize, scale:f64) {
        self.values.mul(index, scale).await;
    }

    async fn fill_random(&self) {
        let mut local_data=self.values.write_local_data().await;
        let mut rng = rand::rng();
        rng.fill(&mut *local_data);
    }

    async fn copy(&self, target: &mut impl Vector) {
        let source_slice= self.values.read_local_data().await;
        let mut target_slice = target.mut_local_values().await;

        if source_slice.len() != target_slice.len() {panic!("Could not copy to target with unequal local storage.")}
        target_slice.copy_from_slice(&source_slice);

        //TODO: If copy_from_slice doesn't work try: target_slice.clone_from(&source_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_copy() {
        let world = LamellarWorldBuilder::new().build();

        let size = 100;
        let v1 = LamellarVector::new_now(&world, size);
        let w = v1.fill_random();
        world.block_on(w);

        let mut v2 = LamellarVector::new_now(&world, size);
        let w = v2.zero();
        world.block_on(w);

        let w = v1.copy(&mut v2);
        world.block_on(w);

        let zipped = v1.values.onesided_iter().into_iter().zip(v2.values.onesided_iter().into_iter());
        for (a,b) in zipped {
            assert_eq!(a,b)
        }
    }

    #[test]
    fn test_scale_value() {
        let world = LamellarWorldBuilder::new().build();

        let size = 100;
        let v1 = LamellarVector::new_now(&world, size);
        let w = v1.fill_random();
        world.block_on(w);

        let mut v2 = LamellarVector::new_now(&world, size);        
        let w = v1.copy(&mut v2);
        world.block_on(w);

        for i in 0..v2.values.len() {
            let w = v2.scale_value(i, 2.0);
            world.block_on(w);
        }

        let zipped = v1.values.onesided_iter().into_iter().zip(v2.values.onesided_iter().into_iter());
        for (a,b) in zipped {
            assert_eq!(a*2.0, *b)
        }

    }


    #[test]
    fn test_fill_random() {
        let world = LamellarWorldBuilder::new().build();

        let size = 100;
        let v = LamellarVector::new_now(&world, size);
        let w = v.fill_random();
        world.block_on(w);
        let mut prior = -1.0;
        for e in v.values.onesided_iter().into_iter() {
            assert!(0.0 < *e);
            assert!(1.0 > *e);
            assert_ne!(prior, *e);
            prior = *e;
        }
    }


    #[test]
    fn test_zero() {
        let world = LamellarWorldBuilder::new().build();

        let size = 100;
        let v = LamellarVector::new_now(&world, size);
        let w = v.zero();
        world.block_on(w);
        for e in v.values.onesided_iter().into_iter() {
            assert_eq!(0.0, *e)
        }
    }

}
