//! Matrix permutation
//!


//  ---------------------------------------------------------------------------

// use lamellar::array::{ReadOnlyArray, ReadOnlyOps };
// use lamellar::LamellarArray;
use lamellar::array::prelude::*;
use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::permutation::rand_perm_distributed;
use sparse_matrix_am::matrix_constructors::dart_uniform_rows;

use std::mem::replace;

//  ---------------------------------------------------------------------------


fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();
    
    let seed                    =   0;
    let rows_per_pe             =   1000; // number of permuted integers stored on each pe
    let target_factor           =   10; // multiplication factor for target array -- defualt to 10
    let iterations              =   10; // -- default to 1
    let nnz_per_row             =   10;
    let row_indices             =   ( world.my_pe() * rows_per_pe .. (world.my_pe()+1) * rows_per_pe ).collect::<Vec<usize>>();

    // define the row and column permutations
    // --------------------------------------    
    let permutation_row         =   rand_perm_distributed( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        seed, 
                                    );

    let permutation_col         =   rand_perm_distributed( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        seed + 1, 
                                    );                                    

    // define the unpermuted matrix
    // ----------------------------    
    let dummy_indices           =   (0..rows_per_pe).collect::<Vec<usize>>();
    let mut matrix_data         =   dart_uniform_rows(
                                        seed + world.my_pe() + 2,
                                        rows_per_pe * world.num_pes(),
                                        nnz_per_row * rows_per_pe,
                                        &dummy_indices,
                                    );
    let mut matrix              =   vec![ vec![]; rows_per_pe ];
    matrix_data.0.drain(..)
        .zip( matrix_data.1.drain(..) )
        .map( |x| matrix[x.0].push(x.1) );

            
    // initialize the permuted matrix
    // ------------------------------    
    let matrix_permuted         =   vec![ vec![]; rows_per_pe ];
    let matrix_permuted         =   LocalRwDarc::new( world.team(), matrix_permuted ).unwrap();

    // place data in bins for distribution
    // -----------------------------------
    let mut bins_to_send        =   vec![ vec![] ; world.num_pes() ];
    
    // remove each row from the matrix, and place it into a bin based on its destination
    for (row_future, row_now) in permutation_row.local_data().iter().enumerate() {
        let pe_destination      =   row_future / rows_per_pe;
        let receiving_row_index =   row_future % rows_per_pe; // we reduct things MODULO the number of PE's, so that `receiving_row_index` is the exact index to which the row will be sent, on the remote PE
        let row_to_send         =   replace( &mut matrix[ *row_now ], vec![] ); // pull this row out of the matrix

        bins_to_send[ pe_destination ].push( (receiving_row_index, row_to_send) );
    }

    // send the bins
    // -------------
    for ( pe, bin_to_send ) in bins_to_send.drain(..).enumerate() {
        if ! bin_to_send.is_empty() {
            let am              =   SendRows{
                                        rows:               bin_to_send,
                                        receiver_of_rows:   matrix_permuted.clone(),
                                    };
            let _ = world.exec_am_pe( pe, am );            
        }
    }

    world.wait_all();
    world.barrier();

    // relabel the columns
    // -------------------

    for row in matrix_permuted.write().iter_mut() {
        * row     =   permutation_col.block_on( permutation_col.batch_load( row.clone() ) );
    }
    
}


//  ===========================================================================
//  ACTIVE MESSAGE
//  ===========================================================================


/// Allows each node to send a batch of rows to other nodes
#[lamellar::AmData(Debug, Clone)]
pub struct SendRows {
    pub rows:               Vec< (usize, Vec<usize>) >,               // diagonal_elements[p] is the set of (row,col) pairs added in epoch p    
    pub receiver_of_rows:   LocalRwDarc< Vec< Vec<usize > > >,             // number of rows/columns we have deleted in total
}

impl SendRows {
    // removes and returns all the rows contained in the am
    pub fn pop_rows( &mut self ) -> Vec< (usize, Vec<usize>) > {
        replace( &mut self.rows, vec![] )
    }
}

#[lamellar::am]
impl LamellarAM for SendRows {
    async fn exec(self) {
        let mut receiver_of_rows    =   self.receiver_of_rows.write();
        for (row_index, row_data) in self.rows.iter().cloned() {
            receiver_of_rows[ row_index ] = row_data;
        }
        // let rows                    =   (*self).pop_rows();
        // for (receiving_row,row_data) in rows.drain(..) {
        //     let _ = replace( &mut receiver_of_rows[receiving_row], row_data );
        // }
    }
}
