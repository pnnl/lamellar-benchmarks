//! Matrix permutation
//!
//! This file generates a random matrix and two permutations. It then applies the 
//! permutations to the matrix. 
//!
//! Some details on implementation:
//! - this implementation uses active messages
//! - we use random seeds to generate the relevant part of the matrix on each PE
//! - permutations are stored as vectors; each PE stores a complete copy of both
//!   permutations, which it generates for itself using a random seed
//! - the matrix is inintialized in vec-of-vec format, where each inner vector
//!   represents a row. we use active messages to send sets of row vectors between
//!   pe's.  each PE sends only one active message to any other PE. once all 
//!   messages have been received, the receiver assmbles them into a new matrix
//!   in vec-of-vec format, then converts to CSR format.  it then applies the
//!   column permutation to the column indices.


//  ---------------------------------------------------------------------------

use lamellar::array::prelude::*;
use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use clap::{Parser, Subcommand};

use sparse_matrix_am::permutation::randperm_am_darc_darts::random_permutation;
use sparse_matrix_am::matrix_constructors::dart_uniform_rows;

use std::mem::replace;
use std::time::Instant;

//  ---------------------------------------------------------------------------


fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();

    // parse command line inputs
    // -------------------------
    let cli                     =   Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   rows_per_thread_per_pe * world.num_threads_per_pe(); // number of matrix rows stored on this pe
    let num_rows_global         =   rows_per_pe * world.num_pes();    
    let avg_nnz_per_row         =   cli.avg_nnz_per_row;

    let seed_permutation_row    =   cli.random_seed; 
    let seed_permutation_col    =   cli.random_seed + 1; 
    let seed_matrix             =   cli.random_seed + 2 + world.my_pe();


    // start the initialization clock
    // ------------------------------

    let start_time_to_initialize=   Instant::now();


    // define the row and column permutations
    // --------------------------------------    
    let target_factor           =   2; // multiplication factor for target array -- defualt to 10
    let iterations              =   3; // -- default to 1
    let nnz_per_row             =   10;
    let randperm_launch_threads =   1;
    let verbose_rand_generator  =   false;    

    let permutation_row         =   random_permutation( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        randperm_launch_threads,
                                        seed_permutation_row, 
                                        verbose_rand_generator,
                                    );

    let permutation_col         =   random_permutation( 
                                        world.clone(), 
                                        rows_per_pe * world.num_pes(),
                                        target_factor,
                                        iterations, 
                                        randperm_launch_threads,
                                        seed_permutation_col, 
                                        verbose_rand_generator,
                                    );                                    

    // define the unpermuted matrix
    // ----------------------------    
    let dummy_indices           =   (0..rows_per_pe).collect::<Vec<usize>>();
    let mut matrix_data         =   dart_uniform_rows(
                                        seed_matrix,
                                        rows_per_pe * world.num_pes(),
                                        nnz_per_row * rows_per_pe,
                                        &dummy_indices,
                                    );  

    let mut matrix              =   vec![ vec![]; rows_per_pe ];

    for p in 0 .. matrix_data.0.len() {
        let row                 =   matrix_data.0[ p ];
        let col                 =   matrix_data.1[ p ];
        matrix[ row ].push( col );
    }

    let matrix_nnz: usize   =   matrix.iter().map(|x| x.len() ).sum();       

    // start the clock
    // ---------------

    let time_to_initialize      =   Instant::now().duration_since(start_time_to_initialize);    
    let start_time_to_permute   =   Instant::now();  

            
    // initialize the permuted matrix
    // ------------------------------    
    let matrix_permuted         =   vec![ vec![]; rows_per_pe ];
    let matrix_permuted         =   LocalRwDarc::new( world.team(), matrix_permuted ).unwrap();

    // place data in bins for distribution
    // -----------------------------------
    let mut bins_to_send        =   vec![ vec![] ; world.num_pes() ];
    
    // remove each row from the matrix, and place it into a bin based on its destination
    for ( row_now, row_future ) in permutation_row.local_data().iter().enumerate() {
        let pe_destination      =   row_future / rows_per_pe;
        let receiving_row_index =   row_future % rows_per_pe; // we reduct things MODULO the number of PE's, so that `receiving_row_index` is the exact index to which the row will be sent, on the remote PE
        let row_to_send         =   replace( &mut matrix[ row_now ], vec![] ); // pull this row out of the matrix

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

    // place the matrix in csr format
    let matrix_nnz_permuted: usize  =   world.block_on(matrix_permuted.read()).iter().map(|x| x.len() ).sum();          

    let mut csr_offset              =   vec![ 0; rows_per_pe + 1 ];
    let mut csr_col_indices         =   Vec::with_capacity(matrix_nnz_permuted);
    
    for (row_index, col_indices) in world.block_on(matrix_permuted.write()).iter().enumerate() {
        csr_offset[ row_index + 1 ] =   csr_offset[ row_index ] + col_indices.len();
        csr_col_indices.extend_from_slice( col_indices );
    }

    // re-index the row indices
    let _                           =   permutation_col.block_on( 
                                            permutation_col.batch_load( 
                                                csr_col_indices.clone() 
                                            ) 
                                        );


    world.wait_all();
    world.barrier();    

    let time_to_permute         =   Instant::now().duration_since(start_time_to_permute);   
    
    let mut number_of_zero_rows =   (0..rows_per_pe).map(|x| csr_offset[x]==csr_offset[x+1]).count();

    if world.my_pe() == 0 {

        

        println!("");
        println!("Finished");
        println!("");

        println!("Number of PE's:                     {:?}", world.num_pes() );  
        println!("Cores per PE:                       {:?}", world.num_threads_per_pe());        
        println!("Matrix size:                        {:?}", num_rows_global );
        println!("Rows per thread per PE:             {:?}", rows_per_thread_per_pe );        
        println!("Avg nnz per row:                    {:?}", matrix_nnz as f64 / rows_per_pe as f64 );
        println!("Avg nnz per row (permuted):         {:?}", matrix_nnz_permuted as f64 / rows_per_pe as f64 );        
        println!("Random seed:                        {:?}", cli.random_seed );
        println!("");          
        println!("Time to initialize matrix:          {:?}", time_to_initialize );
        println!("Time to permute:                    {:?}", time_to_permute );
        println!("");

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
        let mut receiver_of_rows    =   self.receiver_of_rows.write().await;
        for (row_index, row_data) in self.rows.iter().cloned() {
            receiver_of_rows[ row_index ] = row_data;
        }
        // let rows                    =   (*self).pop_rows();
        // for (receiving_row,row_data) in rows.drain(..) {
        //     let _ = replace( &mut receiver_of_rows[receiving_row], row_data );
        // }
    }
}


//  ===========================================================================
//  COMMAND LINE INTERFACE
//  ===========================================================================

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The number of rows owned by each PE
    #[arg(short, long, )]
    rows_per_thread_per_pe: usize,

    /// Desired average number of nonzero entries per row
    #[arg(short, long, )]
    avg_nnz_per_row: usize,

    /// Turn debugging information on
    #[arg(short, long, )]
    random_seed: usize,
}