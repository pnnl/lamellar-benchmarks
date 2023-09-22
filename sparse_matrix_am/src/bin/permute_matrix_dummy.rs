//! Matrix permutation
//!


//  ---------------------------------------------------------------------------

// use lamellar::array::{ReadOnlyArray, ReadOnlyOps };
// use lamellar::LamellarArray;
use lamellar::array::prelude::*;
use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::permutation::randperm_am_darc_darts::random_permutation;
use sparse_matrix_am::matrix_constructors::dart_uniform_rows;

use std::mem::replace;

//  ---------------------------------------------------------------------------


fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();
    
    let seed                    =   0;
    let rows_per_pe             =   1000; // number of permuted integers stored on each pe
    let target_factor           =   2; // multiplication factor for target array -- defualt to 10
    let iterations              =   3; // -- default to 1
    let nnz_per_row             =   10;
    let randperm_launch_threads =   1;
    let row_indices             =   ( world.my_pe() * rows_per_pe .. (world.my_pe()+1) * rows_per_pe ).collect::<Vec<usize>>();

    // define the row and column permutations
    // --------------------------------------    
    let permutation_row         =   random_permutation( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        randperm_launch_threads,
                                        seed, 
                                    );

    let permutation_col         =   random_permutation( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        randperm_launch_threads,
                                        seed + 1, 
                                    );                                    
    
}