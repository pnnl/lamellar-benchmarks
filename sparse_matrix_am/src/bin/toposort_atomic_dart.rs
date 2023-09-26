//! Toposort
//!
//! Prompts the user for 
//!
//! - matrix size (= number of rows)
//! - edge_probability of adding each nonzero entry above the diagonal
//! - a random seed
//!
//! Then generates a random Erdos-Renyi upper unit triangular matrix with the specified number of rows,
//! permutes the matrix according to random row and column permutations, then calculates a permutation
//! the place the permuted matrix back in upper triangular form.  The program then prints the following
//! information:
//!
//! - Time to initialize = time to build the matrix
//! - Time to identify diagonal elements = time to identify the diagonal elements of the original matrix
//! - Time to pool = time to aggregate the diagonal elements identified on each node into node 0, then 
//!   concatenate them into a pair of row and column permutations
//! - Time to verify = time to verify that the new permutations are indeed permutations, and that they
//!   place the matrix in upper triangular form
//!
//! # Implementation details
//!
//! Each node stores a row-submatrix of the permuted matrix P; specifically, PE n stores 
//! rows n*k .. (n+1)*k; the last PE may store fewer rows.
//!
//! Each node stores a list of lists called 'diagonal_elements'.  We update this list
//! recursively in a way that gaurantees that every element of `diagonal_elements[ p ]`
//! has height `p` in the partial order represented by the matrix.

//  ---------------------------------------------------------------------------

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::matrix_constructors::dart_unit_triangular_rows;
use sparse_matrix_am::permutation::Permutation;

use clap::{Parser, Subcommand};

use sprs::{CsMat,TriMat};

use rand::prelude::*;
use rand::seq::SliceRandom;

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, Duration};

//  ---------------------------------------------------------------------------


/// Prompts the user for 
/// - matrix size (= number of rows)
/// - edge_probability of adding each nonzero entry above the diagonal
/// - a random seed
/// Then generates a random Erdos-Renyi upper unit triangular matrix with the specified number of rows,
/// permutes the matrix according to random row and column permutations, then calculates a permutation
/// the place the permuted matrix back in upper triangular form.  The program then prints the following
/// information:
/// - Time to initialize = time to build the matrix
/// - Time to identify diagonal elements = time to identify the diagonal elements of the original matrix
/// - Time to pool = time to aggregate the diagonal elements identified on each node into node 0, then 
///   concatenate them into a pair of row and column permutations
/// - Time to verify = time to verify that the new permutations are indeed permutations, and that they
///   place the matrix in upper triangular form
///
/// # Implementation details
///
/// Each node stores a row-submatrix of the permuted matrix P; specifically, PE n stores 
/// rows n*k .. (n+1)*k; the last PE may store fewer rows.
///
/// Each node stores a list of lists called 'diagonal_elements'.  We update this list
/// recursively in a way that gaurantees that every element of `diagonal_elements[ p ]`
/// has height `p` in the partial order represented by the matrix.
///
/// Edge probability is calculated as follows:
///     p: = edge probability
///     n: = number of rows
///     a: = desired number of nonzeros per row, average
///     t: = total number of nonzeros
///
///     t  = n + p * (number of entries strictly above the diagonal)
///        = n + p * (n^2 - n)/2
///     
///     a  = t / n 
///        = 1 + p * (n-1)/2
///     
///     p  = (a - 1) * 2 / (n-1)    <--- OUR FORMULA
fn main() {

    let start_make_world        =   Instant::now();       

    let world                   =   lamellar::LamellarWorldBuilder::new().build();   
    

    let make_world              =   Instant::now().duration_since(start_make_world);      

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   world.num_threads() * rows_per_thread_per_pe;
    let avg_nnz_per_row         =   cli.avg_nnz_per_row;
    let seed_permute            =   cli.random_seed; 
    let verify                  =   cli.verify.clone().unwrap_or(false);


    // initialize timer variables
    // --------------------------
    let mut time_to_initialize  =   Instant::now().duration_since(Instant::now());
    let mut time_to_loop        =   Instant::now().duration_since(Instant::now());
    let mut time_to_pool        =   Instant::now().duration_since(Instant::now());
    let mut time_to_verify      =   Instant::now().duration_since(Instant::now());


    // define parameters
    // -----------------

    let num_rows_global         =   rows_per_pe * world.num_pes();    
    let row_owned_first_in      =   rows_per_pe * world.my_pe();
    let row_owned_first_out     =   ( row_owned_first_in + rows_per_pe ).min( num_rows_global );
    let num_rows_owned          =   row_owned_first_out - row_owned_first_out;
    // let edge_probability        =   ( avg_nnz_per_row - 1.0 ) * 2.0 / ( num_rows_global - 1 ) as f64;

    let seed_matrix             =   seed_permute+2;

    // we will permute an Erdos Renyi random matrix by replacing each nonzero entry (row,col,val)
    // with (permutation_row.forward(row), permutation_col.forward(col), val)
    let start_time_to_permute   =   Instant::now();    
    let permutation_row         =   Permutation::random(num_rows_global, seed_permute   );
    let permutation_col         =   Permutation::random(num_rows_global, seed_permute+1 );
    let time_to_permute         =   Instant::now().duration_since(start_time_to_permute);        

    let start_time_to_hash      =   Instant::now();    
    let mut rows_owned: HashSet<usize>     // the indices of the rows owned by this PE     
                                =   (row_owned_first_in .. row_owned_first_out).collect();   
    let time_to_hash            =   Instant::now().duration_since(start_time_to_hash);        

    // initialize values
    // -----------------

    let start_time_initializing_values  
                                =   Instant::now();

    // // this function returns the `index_row`th row of the permuted matrix, 
    // // repersented by a pair of vectors (indices_row,indices_col)
    // let get_row                 =   | index_row: usize | -> (Vec<usize>, Vec<usize>) {
    //     let row_of_er           =   permutation_row.get_backward( index_row ); // NB: we have to use the backward permutation here
    //     let mut indices_col     =   bernoulli_upper_unit_triangular_row(
    //                                     seed_matrix + index_row,
    //                                     num_rows_global,
    //                                     edge_probability,
    //                                     row_of_er,     
    //                                 );
    //     for p in 0 .. indices_col.len() {
    //         indices_col[p]  =   permutation_col.get_forward( indices_col[p] ); 
    //     }        
    //     let indices_row     =   vec![ index_row; indices_col.len() ]; 
    //     (indices_row, indices_col)       
    // };

    // // generate the portion of the matrix owned by this PE
    let start_time_matrix_unpermuted_raw_entries 
                                =   Instant::now(); 
    // let mut indices_row         =   Vec::new();
    // let mut indices_col         =   Vec::new();
    // for index_row in row_owned_first_in .. row_owned_first_out {
    //     let (indices_row_new, indices_col_new)  =   get_row( index_row );
    //     indices_row.extend_from_slice( & indices_row_new );
    //     indices_col.extend_from_slice( & indices_col_new );                                                                            
    // }

    let get_rows_for_pe         =   | pe: usize | -> (Vec<usize>,Vec<usize>) {

        let (mut indices_row, mut indices_col)
                                =   dart_unit_triangular_rows(
                                        seed_matrix + pe,
                                        num_rows_global,
                                        avg_nnz_per_row * rows_per_pe,
                                        & permutation_row.backward[row_owned_first_in .. row_owned_first_out],
                                    );
        
        for p in 0 .. indices_row.len() {
            indices_row[p]          =   permutation_row.get_forward( indices_row[p] );
            indices_col[p]          =   permutation_col.get_forward( indices_col[p] );
        }  
        ( indices_row, indices_col )      
    };

    let (indices_row,indices_col)   =   get_rows_for_pe( world.my_pe() );

    let time_matrix_unpermuted_raw_entries   
                                    =   Instant::now().duration_since(start_time_matrix_unpermuted_raw_entries);    

    
    let num_entries                 =   indices_row.len();
    let matrix                      =   TriMat::from_triplets(
                                            (num_rows_global,num_rows_global),
                                            indices_row,
                                            indices_col,
                                            vec![1u8; num_entries], // fill with meaningless coefficients
                                        );
    let matrix                      =   matrix.to_csc();

    // the number and sum-of-column-indices of the nonzero entries in each row
    let mut row_sums:   Vec<_>  =   (0..num_rows_global).map(|_| AtomicUsize::new(0) ).collect();
    let mut row_counts: Vec<_>  =   (0..num_rows_global).map(|_| AtomicUsize::new(0) ).collect();
    for column in 0 .. num_rows_global {
        let nz_entries          =   matrix.outer_view( column ).unwrap();
        let nz_indices          =   nz_entries.indices();   
        for row in nz_indices {
            // add 1 to row_counts
            row_counts[    *row ]
                .fetch_add(
                    1,
                    Ordering::SeqCst
                );
            // add column to row_sums
            row_sums[      *row ]
                .fetch_add(
                    column,
                    Ordering::SeqCst
                );                    
        }        
    }

    // bucket for diagonal elements
    let mut height_bins         =   vec![0]; // histogram of elements according to height in the poset
    let mut diagonal_elements: Vec< Vec< (usize,usize) > >
                                =   vec![ vec![]; 64 ];
    let diagonal_elements_union: Vec< Vec< (usize,usize) > >
                                =   Vec::new();

    // wrap in LocalRwDarc's
    let matrix                  =   Darc::new( world.team(), matrix                  ).unwrap();
    let row_sums                =   Darc::new( world.team(), row_sums                ).unwrap();
    let row_counts              =   Darc::new( world.team(), row_counts              ).unwrap();
    // let diagonal_elements       =   Darc::new( world.team(), diagonal_elements       ).unwrap();
    let num_deleted_global      =   Darc::new( world.team(), AtomicUsize::new(0)     ).unwrap();
    let diagonal_elements_union =   LocalRwDarc::new( world.team(), diagonal_elements_union ).unwrap();
    
    time_to_initialize          =   Instant::now().duration_since(start_time_initializing_values);
    world.barrier();

    // enter loop
    // -----------------

    let start_time_main_loop    =   Instant::now();
    let mut poset_height        =   0;                                    

    for epoch in 0..num_rows_global {

        if diagonal_elements.len() < epoch + 1 {
            diagonal_elements.push(vec![]);
        }

        let columns_to_delete = {
            // Step 1: identify all rows with a single nonzero entry, and their corresponding columns
            //         then push the identified elements to 

            
            for row in rows_owned.iter() {
                if row_counts[ *row ].load(Ordering::SeqCst)  == 1 {
                    let diagonal_element    =   (
                                                    row.clone(),
                                                    row_sums[*row].load(Ordering::SeqCst) // there's only one entry in this row, so its sum is the column where the nz entry appears
                                                );                                          
                    diagonal_elements[ epoch ].push( diagonal_element );
                }
            }
        
            world.barrier();
        
            // list the paired columns, and remove the paired rows from `rows_owned`
            let mut columns_to_delete   =   Vec::with_capacity( diagonal_elements[ epoch ].len() );
            for (row, col) in  diagonal_elements[ epoch ].iter() {
                columns_to_delete.push(col.clone());
                rows_owned.remove(row);
            }

            columns_to_delete
        };

        //  Step 2: delete the rows and columns we've just identified
        if ! columns_to_delete.is_empty() {
            let am  =   ToposortAmX{
                            matrix:             matrix.clone(),           
                            row_sums:           row_sums.clone(),         
                            row_counts:         row_counts.clone(),       
                            columns_to_delete:  columns_to_delete,
                            num_deleted_global: num_deleted_global.clone(),
                        };
            let _ = world.exec_am_all( am );
        }

        world.wait_all();          
        world.barrier();             

        // if we have deted every row and column, then we are done with the loop
        // all we have to do now is 
        if num_deleted_global.load(Ordering::SeqCst) == num_rows_global {

            time_to_loop            =   Instant::now().duration_since(start_time_main_loop);    

            let start_time_pooling_permutations  
                                    =   Instant::now();                    

            // make a vector copy of all the diagonal elements found on this PE
            // let diagonal_elements_to_move: Vec<_> = diagonal_elements.iter().cloned().collect();
            poset_height            =   epoch;
            
            diagonal_elements.truncate(epoch+1); // remove trailing empty vectors
            diagonal_elements.shrink_to_fit();

            {
                diagonal_elements_union.write().extend( vec![vec![]; epoch+1 ] ); // ensure diagonal_elements_union is long enough (before now it had length 0)
            }

            // send all elements to PE0 for integration
            let am  =   PoolDiagonalElementsAmX{
                diagonal_elements_to_move:  diagonal_elements,           
                diagonal_elements_to_stay:  diagonal_elements_union.clone(),
            };
            let _ = world.exec_am_pe( 0, am );  

            world.wait_all();          
            world.barrier();               

            time_to_pool            =    Instant::now().duration_since(start_time_pooling_permutations);                

            // number of elements of each height
            height_bins             =   diagonal_elements_union.read().iter().map(|x| x.len()).collect();
            // let u                   =   diagonal_elements_union.read();
            // height_bins             =   (0..epoch)
            //                                 .map(|x| u[x].len())
            //                                 .collect();             

            break
        }
    }


    
    if world.my_pe() == 0 {

        if verify {
            let start_time_verifying_permutation
                                            =   Instant::now();  
            
            // concatenate all elements on PE0
            let zipped_permutation          =   diagonal_elements_union.read().concat();
            // println!("zipped permutation: {:?}", &zipped_permutation);
            // println!("diagonal elements union: {:?}", &diagonal_elements_union);        

            // calculate the new row and column permutations
            let mut new_permutation_row: Vec<usize>     =   vec![0;num_rows_global];
            let mut new_permutation_col: Vec<usize>     =   vec![0;num_rows_global];
            for (ordinal, (row,col)) in zipped_permutation.into_iter().rev().enumerate() {
                new_permutation_row[row]    =   ordinal;
                new_permutation_col[col]    =   ordinal;            
            }

            //  verify the permutations are correct
            //  -----------------------------------

            //  check that the permuted matrix is upper triangular
            //  for this, it suffices to check that for every nonzero entry (row,col,val), we have row ≤ col
            // for pe in 0 .. world.num_pes() {
            //     let (indices_row,indices_col)   =   get_rows_for_pe( pe );
            //     for (index_row_old,index_col_old) in indices_row.iter().zip( indices_col.iter() ) {
            //         if new_permutation_row[ *index_row_old ] > new_permutation_col[ *index_col_old ] {
            //             panic!("Permutation failed to produce an upper triangular matrix");                        
            //         }
            //     }
            // }

            // for index_row_old in 0 .. num_rows_global {

            //     let indices_column_old  =   bernoulli_upper_unit_triangular_row(
            //                                     seed_matrix + , 
            //                                     side_length: usize, 
            //                                     epsilon: f64, 
            //                                     row: usize,
            //                                 )

            //     let index_row_new       =   new_permutation_row[ index_row_old ];
            //     for index_col_old in get_row( index_row_old ).1 {
            //         let index_col_new   =   new_permutation_col[ index_col_old ];
            //         if index_row_new > index_col_new {
            //             panic!("Permutation failed to produce an upper triangular matrix");
            //         }
            //     }
            // }

            //  check that the permutations are indeed permutations
            let unique_elements: HashSet<&usize>     =   new_permutation_row.iter().collect();
            assert_eq!( unique_elements.len(), num_rows_global );
            let unique_elements: HashSet<&usize>     =   new_permutation_col.iter().collect();
            assert_eq!( unique_elements.len(), num_rows_global ); 
            
            time_to_verify          =    Instant::now().duration_since(start_time_verifying_permutation); 
        }

        
        println!("");
        println!("Finished successfully");
        println!("");
        println!("Number of rows per thead per PE:    {:?}", cli.rows_per_thread_per_pe );        
        println!("Average number of nonzeros per row: {:?}", cli.avg_nnz_per_row );        
        println!("Random seed:                        {:?}", cli.random_seed );
        println!("Number of PE's:                     {:?}", world.num_pes() );  
        println!("Cores per PE:                       {:?}", world.num_threads());
        println!("Number of nonzeros on PE 0:         {:?}", matrix.nnz() );                   
        println!("");       
        println!("Time to generate rand perm's        {:?}", time_to_permute); 
        println!("Time to generate raw matrix entr    {:?}", time_matrix_unpermuted_raw_entries);
        println!("Time to initialize matrix:          {:?}", time_to_initialize );
        println!("Time to identify diagonal elements: {:?}", time_to_loop );
        println!("Time to pool diagonal elements:     {:?}", time_to_pool );
        if verify{
            println!("Time to verify permutations:        {:?}", time_to_verify );        
        } else {
            println!("Time to verify permutations:        Not applicable, did not verify" );                    
        }
        println!("");
        println!("Time to make world:                 {:?}", make_world );
        println!("Time to hash owned rows:            {:?}", time_to_hash );

        println!("");
        println!("Poset height:                       {:?}", poset_height );
        println!("Height bins:                        {:?}", height_bins );
    }
}





//  ===========================================================================
//  COMMAND LINE INTERFACE
//  ===========================================================================



#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Number of rows stored on each PE
    #[arg(short, long, )]
    rows_per_thread_per_pe: usize,

    /// Average number of nonzeros per row
    #[arg(short, long, )]
    avg_nnz_per_row: usize,

    /// Seed for the random generator that determines the matrix.
    #[arg(short, long, )]
    random_seed: usize,

    /// Flag to determine whether or not to verify the permutation
    #[arg(short, long, )]
    verify: Option<bool>,    
}



//  ===========================================================================
//  ACTIVE MESSAGES
//  ===========================================================================


/// Allows each node to tell all other nodes (including itself) which matrix columns
/// to delete.
#[lamellar::AmData(Debug, Clone)]
pub struct ToposortAmX {
    pub matrix:             Darc<CsMat<u8>>, 
    pub row_sums:           Darc<Vec<AtomicUsize>>,       // sum of nonzero column indices for each row
    pub row_counts:         Darc<Vec<AtomicUsize>>,       // number of nonzero column indices for each row
    pub num_deleted_global: Darc<AtomicUsize>,             // number of rows/columns we have deleted in total
    pub columns_to_delete:  Vec<usize>,             // columns to be removed when the am executes
}

#[lamellar::am]
impl LamellarAM for ToposortAmX {
    async fn exec(self) {

        // there is nothing to do if no columns are deleted
        if ! self.columns_to_delete.is_empty() {

            // update the global count of number of deleted rows/columns
            self.num_deleted_global
                .fetch_add(
                    self.columns_to_delete.len(), 
                    Ordering::SeqCst
                );

            // delete the appropriate columns
            for column in & self.columns_to_delete {
                let nz_entries          =   self.matrix.outer_view( *column ).unwrap();
                let nz_indices          =   nz_entries.indices();
                for row in nz_indices {
                    // subtract 1 from row_counts
                    self.row_counts[    *row ]
                        .fetch_sub(
                            1,
                            Ordering::SeqCst
                        );
                    // subtract column from row_sums
                    self.row_sums[      *row ]
                        .fetch_sub(
                            *column,
                            Ordering::SeqCst
                        );                    
                }
            }
        }
    }
}

/// Allows each node to send the diagonal elements it has found to PE 0.
#[lamellar::AmData(Debug, Clone)]
pub struct PoolDiagonalElementsAmX {
    pub diagonal_elements_to_move:     Vec<Vec<(usize,usize)>>,                // diagonal_elements[p] is the set of (row,col) pairs added in epoch p    
    pub diagonal_elements_to_stay:     LocalRwDarc<Vec<Vec<(usize,usize)>>>,             // number of rows/columns we have deleted in total
}

#[lamellar::am]
impl LamellarAM for PoolDiagonalElementsAmX {
    async fn exec(self) {
        let mut diagonal_elements_to_stay   =   self.diagonal_elements_to_stay.write();
        for (epoch,vec) in self.diagonal_elements_to_move.iter().enumerate() {
            diagonal_elements_to_stay[epoch].extend_from_slice(vec);
        }
    }
}


// Number of rows per PE:              100000
// Average number of nonzeros per row: 10
// Random seed:                        0
// Number of PE's:                     32
// Cores per PE:                       2
// Number of nonzeros on PE 0:         1000000

// Time to generate rand perm's        162.513064ms
// Time to generate raw matrix entr    122.961301ms
// Time to initialize matrix:          299.117747ms
// Time to identify diagonal elements: 584.558788ms
// Time to pool diagonal elements:     65.256003ms
// Time to verify permutations:        Not applicable, did not verify