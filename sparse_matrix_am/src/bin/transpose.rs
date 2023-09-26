//! Matrix permutation
//!


//  ---------------------------------------------------------------------------

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
    let num_pes                 =   world.num_pes();

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   rows_per_thread_per_pe * world.num_threads();
    let num_rows_global         =   rows_per_pe * world.num_pes();    
    let avg_nnz_per_row         =   cli.avg_nnz_per_row;
    let seed_matrix             =   cli.random_seed + world.my_pe(); 
    
    // start the initialization clock
    // ------------------------------

    let start_time_to_initialize=   Instant::now();    

    // define the owned portion of the matrix
    // ----------------------------    
    let dummy_indices           =   (0..rows_per_pe).collect::<Vec<usize>>();
    // returns a the locations of nonzero indices in the form of (list_of_row_indices, list_of_column_indices)
    let mut matrix_data         =   dart_uniform_rows(
                                        seed_matrix,
                                        num_rows_global,
                                        avg_nnz_per_row * rows_per_pe,
                                        &dummy_indices,
                                    );
    let nnz_in_this_pe          =   matrix_data.0.len();


    // ----------------------------------------------------------    
    // calculate the transpose of the owned portion of the matrix
    // ----------------------------------------------------------


    // start the main clock
    // --------------------

    let time_to_initialize      =   Instant::now().duration_since(start_time_to_initialize);    
    let start_time_to_permute   =   Instant::now();      

    // for each column submatrix of size `rows_per_pe`, define a vector V such that
    // V[i] = number of nonzero entries in the first i columns of the current block
    // -----------------------------------------------------------------------------
    
    // NB: this means that V[i] does *not* count the entries in column i

    let mut column_offset_binned     
                                =   vec![
                                        vec![ 0; rows_per_pe + 1 ]; // this is the vector V
                                        num_pes
                                    ];

    // calculate nnz per column
    for j in matrix_data.1.iter().cloned() {
        let pe                  =   j / rows_per_pe;
        let local_column_num    =   j % rows_per_pe;
        column_offset_binned[ pe ][ local_column_num + 1 ] += 1; // NB: we offset the histogram by shifting all entries to the right one place
    }

    // calculate the column offset vector for transpose of the row-submatrix owned by this PE (broken into chunks, according to destination PE)
    for pe in 0 .. num_pes {
        let mut column_offset_bin    
                                =   column_offset_binned.get_mut( pe ).unwrap();
        for j in 1 .. rows_per_pe + 1 {
            column_offset_bin[ j ] += column_offset_bin[ j-1 ]
        }
    }

    // pre-allocate space for row indices (for locally owned rows); space allocated separately for each destination PE
    // -----------------------------------------------------------

    let mut row_indices_binned  =    Vec::with_capacity( num_pes );
    for pe in 0 .. num_pes {
        let nnz                 =   column_offset_binned[ pe ][ rows_per_pe ].clone(); 
        let row_indices_bin     =   vec![ 0; nnz ];
        row_indices_binned.push( row_indices_bin );
    }    

    // 1) update `column_offset_binned` so that it becomes the column offset vector of the transpose
    //    of the *owned* portion of the matrix --WITH THE LEADING ZERO ENTRY DELETED--, and
    // 2) generate the (unsorted) row index vector of the transpose of the *owned* portion of
    //    the matrix
    // ---------------------------------------------------------------------------------------

    let mut pe;
    let mut col_local;
    let mut row_local;
    let mut linear_index_local;    
    let mut row_indices_bin;
    let mut col_offset_walker_bin;

    // check out a deep copy of the column offset array; this copy starts as a perfect column offset array,
    // and we will mutate it by incrementally increaing its entries; the result 
    // will no longer be a valid offset array
    let mut col_offset_walker_binned   
                                =   column_offset_binned.clone();
    for (row,col) in matrix_data.0.iter().cloned().zip( matrix_data.1.iter().cloned() ) {
        pe                      =   col / rows_per_pe;
        col_local               =   col % rows_per_pe;
        row_local               =   row + world.my_pe() * rows_per_pe;

        row_indices_bin         =   row_indices_binned.get_mut(        pe  ).unwrap();
        col_offset_walker_bin   =   col_offset_walker_binned.get_mut(  pe  ).unwrap(); 

        linear_index_local      =   col_offset_walker_bin[ col_local ];
        
        row_indices_bin[ linear_index_local ]     
                                =   row_local;
        
        col_offset_walker_bin[ col_local ] 
                                +=  1;
    }

    // NB: now that the for-loop has finished, we no longer need `col_offset_walker_binned`


    // ------------------------------------------------------------- 
    // send relevant pieces of the column offset, in active messages
    // -------------------------------------------------------------


    let destination_offset      =   LocalRwDarc::new( world.team(), vec![0; rows_per_pe + 1] ).unwrap();

    for pe in 0 .. world.num_pes() {
        let source_offset       =   column_offset_binned[ pe ].clone(); // pull out the set of column offset for this pe
        let am                  =   SendOffsets {
                                        source_offset,
                                        destination_offset: destination_offset.clone(),
                                    };
        let _                   =   world.exec_am_pe( pe, am );  // add these offset to the global count                
    }

    world.wait_all();
    world.barrier();


    // ------------------------------------------------------------- 
    // send relevant sections of row indices, in active messages
    // -------------------------------------------------------------    


    // get a deep copy of `destination_offset_walker`, which will be
    // used to "walk" the linear indices of row indices
    let mut destination_offset_walker
                                =   vec![ 0; rows_per_pe + 1 ];
    {
        let handle              =   destination_offset.read();
        for p in 0 .. rows_per_pe + 1 {
            
            destination_offset_walker[p]
                                =   handle[ p ];
        }
    }

    // wrap the deep copy / column offset walker in a LocalRwDarc
    let destination_offset_walker
                                =   LocalRwDarc::new( 
                                        world.team(),
                                        destination_offset_walker
                                    ).unwrap();

    // pre-allocate the vector of row indices
    let num_destination_row_indices  
                                =   destination_offset_walker
                                        .read()[ rows_per_pe ];
    let destination_row_indices =   LocalRwDarc::new( 
                                        world.team(),
                                        vec![ 0; num_destination_row_indices ],
                                    ).unwrap();

    // send an active messages containing the row indices
    for pe in 0 .. world.num_pes() {
        let source_offset       =   column_offset_binned[ pe ].clone(); // pull out the set of column offset for this pe
        let source_row_indices  =   row_indices_binned[ pe ].clone();
        let am                  =   SendRowIndices {
                                        source_offset,
                                        source_row_indices,
                                        destination_offset_walker: destination_offset_walker.clone(),
                                        destination_row_indices: destination_row_indices.clone(),
                                    };
        let _                   =   world.exec_am_pe( pe, am );  // add these offset to the global count                
    }

    world.wait_all();
    world.barrier();

    // stop timer and report results
    // -----------------------------

    let time_to_transpose       =   Instant::now().duration_since(start_time_to_permute);    
    
    let matrix_nnz_transposed   =   destination_row_indices.read().len();

    if world.my_pe() == 0 {
        println!("");
        println!("Finished");
        println!("");
    
        println!("Number of PE's:                     {:?}", world.num_pes() );  
        println!("Cores per PE:                       {:?}", world.num_threads());        
        println!("Matrix size:                        {:?}", num_rows_global );
        println!("Rows per thread per PE:             {:?}", rows_per_thread_per_pe );        
        println!("Avg nnz per row:                    {:?}", nnz_in_this_pe as f64 / rows_per_pe as f64 );
        println!("Avg nnz per row (permuted):         {:?}", matrix_nnz_transposed as f64 / rows_per_pe as f64 );        
        println!("Random seed:                        {:?}", cli.random_seed );
        println!("");          
        println!("Time to initialize matrix:          {:?}", time_to_initialize );
        println!("Time to transpose:                  {:?}", time_to_transpose );
        println!("");          
    }  
    
}


//  ===========================================================================
//  ACTIVE MESSAGE
//  ===========================================================================


/// Allows each node to transmit data about the number of nonzero entries it holds in each column.
#[lamellar::AmData(Debug, Clone)]
pub struct SendOffsets {
    pub source_offset:         Vec< usize >,            // source_offset[j] = number of nonzero entries stored in columns ( destination_pe * num_rows_per_pe ) .. ( destination_pe * num_rows_per_pe + j ) *excluding* `i * num_rows_per_pe + j`
    pub destination_offset:    LocalRwDarc< Vec< usize > >,    // the array to which we will add these offset
}

#[lamellar::am]
impl LamellarAM for SendOffsets {
    async fn exec(self) {
        let mut destination_offset    =   self.destination_offset.write();
        for ( local_column_number, offset ) in self.source_offset.iter().cloned().enumerate() {
            destination_offset[ local_column_number ] +=     offset;
        }
    }
}



/// Allows each node to transmit its row indices to a destination node
#[lamellar::AmData(Debug, Clone)]
pub struct SendRowIndices {
    pub source_offset:             Vec< usize >,                   
    pub source_row_indices:         Vec< usize >,                   // source_row_indices[ source_offset[i] .. source_offset[i+1] ] = the row indices for column i owned by the sending PE
    pub destination_offset_walker:  LocalRwDarc< Vec< usize > >,    
    pub destination_row_indices:    LocalRwDarc< Vec< usize > >,    
}

#[lamellar::am]
impl LamellarAM for SendRowIndices {
    async fn exec(self) {
        let mut destination_offset_walker   =   self.destination_offset_walker.write();
        let mut destination_row_indices     =   self.destination_row_indices.write();
        let source_offset                   =   & self.source_offset;
        let source_row_indices              =   & self.source_row_indices;                
        
        // for each column, add row indices from the source PE to the destination PE
        for col in 0 .. source_offset.len()-1 {

            let source_col_nnz              =   source_offset[ col + 1] - source_offset[ col ];
            let linear_index_start_source   =   source_offset[ col ].clone();
            let linear_index_start_destination
                                            =   destination_offset_walker[ col ].clone();
            // add each row index in the current column of the source PE to the destination PE
            for i in 0 .. source_col_nnz {
                let read_from               =   linear_index_start_source + i;
                let write_to                =   linear_index_start_destination + i;
                destination_row_indices[ write_to ]
                                            =   source_row_indices[ read_from ];
            }

            // update the column offset vector of the destination PE to reflect the new elements
            *(**destination_offset_walker).get_mut( col ).unwrap()
                                            +=  source_col_nnz; // update the 
        }
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