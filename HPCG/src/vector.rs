/// Based on https://github.com/hpcg-benchmark/hpcg/blob/master/src/Vector.hpp

use lamellar::array::prelude::*;
use lamellar::array::LocalLockArray;
use rand::prelude::*;

/// Dense vector trait members
pub trait Vector {
    /// Fill vector with zero values.  
    async fn zero(&self);

    /// Multiply a single index's value by the passed value
    async fn scale_value(&self, index:usize, value:f64);

    ///Fill a vector with psuedo-random values between 0 and 1
    async fn fill_random(&self);

    /// Copy the contents of a this (input) vector to the passed (target) vector.
    async fn copy(&self, target:&mut LocalLockVector);

    /// How many elements are in the vector?
    fn len(&self) -> usize;
}



#[derive(Clone, Debug)]
pub struct LocalLockVector {
    pub values: LocalLockArray<f64>,
    pub optimization_data: Option<usize> // placeholder for later use
}

impl LocalLockVector {
    pub async fn new(world: &LamellarWorld, size: usize) -> LocalLockVector {
        LocalLockVector {
            values: LocalLockArray::new(world, size,
                lamellar::array::Distribution::Block,
                ).await,
            optimization_data: Option::None
        }
    }

    #[cfg(test)]
    pub fn new_now(world: &LamellarWorld, size: usize) -> LocalLockVector {
        let w = Self::new(world, size);
        world.block_on(w)
    }

    #[cfg(test)]
    pub async fn ones(&self) {
        let mut local_data=self.values.write_local_data().await;
        local_data.iter_mut().for_each(|elem| *elem = 1.0);
    }
}

impl Vector for LocalLockVector {
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

    async fn copy(&self, target: &mut LocalLockVector) { //TODO: Generalize away from LocalLockVector
        let source_slice= self.values.read_local_data().await;
        let mut target_slice = target.values.write_local_data().await;

        if source_slice.len() != target_slice.len() {
            panic!("Could not copy to target with unequal local storage.")
        }
        target_slice.copy_from_slice(&source_slice);

        //TODO: If copy_from_slice doesn't work try: target_slice.clone_from(&source_slice)
    }

    fn len(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::LazyLock;
     use lamellar::Backend;


    static WORLD: LazyLock<LamellarWorld> = LazyLock::new(
        || 
        LamellarWorldBuilder::new()
            .with_lamellae(Backend::Local)
            .build()
    );

    #[test]
    fn test_copy() {
        let world = &(*WORLD);

        let size = 100;
        let v1 = LocalLockVector::new_now(world, size);
        let w = v1.fill_random();
        world.block_on(w);

        let mut v2 = LocalLockVector::new_now(world, size);
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
        let world = &(*WORLD);

        let size = 100;
        let v1 = LocalLockVector::new_now(world, size);
        let w = v1.fill_random();
        world.block_on(w);

        let mut v2 = LocalLockVector::new_now(world, size);
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
        let world = &(*WORLD);

        let size = 100;
        let v = LocalLockVector::new_now(world, size);
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
        let world = &(*WORLD);

        let size = 100;
        let v = LocalLockVector::new_now(world, size);
        let w = v.zero();
        world.block_on(w);
        for e in v.values.onesided_iter().into_iter() {
            assert_eq!(0.0, *e)
        }
    }

    #[test]
    fn test_ones() {
        let world = &(*WORLD);

        let size = 100;
        let v = LocalLockVector::new_now(world, size);
        let w = v.ones();
        world.block_on(w);
        for e in v.values.onesided_iter().into_iter() {
            assert_eq!(1.0, *e)
        }
    }

}
