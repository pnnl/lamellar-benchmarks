//! Tools for permutations


// ============================================================================

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

use lamellar::LamellarWorld;
use lamellar::array::prelude::*;
use rand::prelude::*;
use std::time::Instant;

// ============================================================================

pub mod randperm_am_darc_darts;

// ============================================================================


pub struct Permutation{
    pub forward:    Vec<usize>,
    pub backward:   Vec<usize>,
}

impl Permutation {
    /// Returns the label assigned to an element
    pub fn get_forward( &self, original: usize ) -> usize { self.forward[original].clone() }

    /// Given a label, find the element it was attached to
    pub fn get_backward( &self, label: usize ) -> usize { self.backward[label].clone() }    

    /// Returns the label assigned to an element
    pub fn forward( &self ) -> &Vec<usize> { &self.forward }

    /// Given a label, find the element it was attached to
    pub fn backward( &self ) -> &Vec<usize> { &self.backward }   
    
    /// Generates a random permutation from a random seed
    pub fn random( length: usize, seed: usize ) -> Self {
        let mut rng = StdRng::seed_from_u64(seed as u64);
        let mut forward: Vec<_> = (0..length).collect(); 
        let mut backward    =   vec![0; forward.len()];       
        forward.shuffle(&mut rng); // Shuffle the elements to generate a random permutation
        for (original,label) in forward.iter().cloned().enumerate() {
            backward[label] = original;
        }
        return Permutation{ forward, backward }
    }
}


pub fn rand_perm( length: usize, seed: usize ) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(seed as u64);
    let mut forward: Vec<_> = (0..length).collect();    
    forward.shuffle(&mut rng); // Shuffle the elements to generate a random permutation
    forward
}


pub fn rand_perm_distributed( 
        world: LamellarWorld, 
        global_count: usize, // size of permuted array
        target_factor: usize, // multiplication factor for target array -- defualt to 10
        iterations: usize, // -- default to 1
        seed: usize, 
    )  
    -> ReadOnlyArray<usize>    
{

    let my_pe = world.my_pe();

    if my_pe == 0 {
        println!("array size {}", global_count);
        println!("target array size {}", global_count * target_factor);
    }

    // start with unsafe because they are faster to initialize than AtomicArrays
    let darts_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count,
        lamellar::array::Distribution::Block,
    );
    let target_array = UnsafeArray::<usize>::new(
        world.team(),
        global_count * target_factor,
        lamellar::array::Distribution::Block,
    );
    let mut rng: StdRng = SeedableRng::seed_from_u64( (seed + my_pe) as u64);

    // initialize arrays
    let darts_init = unsafe {
        darts_array
            .dist_iter_mut()
            .enumerate()
            .for_each(|(i, x)| *x = i)
    }; // each PE some slice in [0..global_count]
    let target_init = unsafe { target_array.dist_iter_mut().for_each(|x| *x = usize::MAX) };
    world.block_on(darts_init);
    world.block_on(target_init);
    world.wait_all();

    let darts_array = darts_array.into_read_only();
    let local_darts = darts_array.local_data(); //will use this slice for first iteration

    let target_array = target_array.into_atomic();
    world.barrier();
    if my_pe == 0 {
        println!("start");
    }

    let mut the_array   =   ReadOnlyArray::<usize>::new(
                                & world,
                                world.num_pes() * 2,
                                lamellar::array::Distribution::Block,
                            );

    for _ in 0..iterations {
        world.barrier();
        let now = Instant::now();
        // ====== perform the actual random permute========//
        let rand_index = (0..local_darts.len())
            .map(|_| rng.gen_range(0, global_count * target_factor))
            .collect::<Vec<usize>>();

        // launch initial set of darts, and collect any that didnt stick
        let mut remaining_darts = world
            .block_on(target_array.batch_compare_exchange(&rand_index, usize::MAX, local_darts))
            .iter()
            .enumerate()
            .filter_map(|(i, elem)| {
                match elem {
                    Ok(_val) => None,               //the dart stuck!
                    Err(_) => Some(local_darts[i]), //something else was there, try again
                }
            })
            .collect::<Vec<usize>>();

        // continue launching remaining darts until they all stick
        while remaining_darts.len() > 0 {
            let rand_index = (0..remaining_darts.len())
                .map(|_| rng.gen_range(0, global_count * target_factor))
                .collect::<Vec<usize>>();
            remaining_darts = world
                .block_on(target_array.batch_compare_exchange(
                    &rand_index,
                    usize::MAX,
                    remaining_darts.clone(),
                ))
                .iter()
                .enumerate()
                .filter_map(|(i, elem)| {
                    match elem {
                        Ok(_val) => None,                   //the dart stuck!
                        Err(_) => Some(remaining_darts[i]), //something else was there, try again
                    }
                })
                .collect::<Vec<usize>>();
        }
        world.wait_all(); //my work is done
        if my_pe == 0 {
            println!("local run time {:?} ", now.elapsed(),);
        }
        world.barrier(); //all work is done
        if my_pe == 0 {
            println!("permute time {:?}s ", now.elapsed().as_secs_f64(),);
        }
        let collect_start = Instant::now();
        the_array = world.block_on(
            target_array
                .dist_iter()
                .filter_map(|elem| {
                    let elem = elem.load(); //elements are atomic so we cant just read directly
                    if elem < usize::MAX {
                        Some(elem)
                    } else {
                        None
                    }
                })
                .collect::<ReadOnlyArray<usize>>(lamellar::array::Distribution::Block),
        );
        // =============================================================//
        world.barrier();
        let global_time = now.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!("collect time: {:?}s", collect_start.elapsed().as_secs_f64());
            println!(
                "global time {:?} MB {:?} MB/s: {:?} ",
                global_time,
                (world.MB_sent()),
                (world.MB_sent()) / global_time,
            );
            println!("Secs: {:?}", global_time,);
            let sum = world.block_on(the_array.sum());
            println!(
                "reduced sum: {sum} calculated sum {} ",
                (global_count * (global_count + 1) / 2) - global_count
            );
            if sum != (global_count * (global_count + 1) / 2) - global_count {
                println!("Error! randperm not as expected");
            }
        }
        world.block_on(
            target_array
                .dist_iter_mut()
                .for_each(|x| x.store(usize::MAX)),
        );
        world.barrier();
    }
    return the_array
}



//  ==========================================
//  DISTRIBUTED PERMUTATION
//  ==========================================

