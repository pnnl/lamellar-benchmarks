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


//  ---------------------------------------------------------------------------

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::matrix_constructors::bernoulli_upper_unit_triangular_row;
use sparse_matrix_am::toposort_am::{ToposortAm,PoolDiagonalElementsAm};
use sparse_matrix_am::permutation::Permutation;

use clap::{Parser, Subcommand};

use sprs::{CsMat,TriMat};

use rand::prelude::*;
use rand::seq::SliceRandom;

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
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
fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();    

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let num_rows_global         =   cli.matrix_size;
    let edge_probability        =   cli.edge_probability;
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

    // let num_rows_global     =   10;
    let num_rows_per_pe     =   1 + (num_rows_global / world.num_pes());    
    let row_owned_first_in  =   num_rows_per_pe * world.my_pe();
    let row_owned_first_out =   ( row_owned_first_in + num_rows_per_pe ).min( num_rows_global );
    let num_rows_owned      =   row_owned_first_out - row_owned_first_out;
    // let edge_probability         =   0.5;

    // let seed_permute        =   0;
    let seed_matrix         =   seed_permute+2;

    // we will permute an Erdos Renyi random matrix by replacing each nonzero entry (row,col,val)
    // with (permutation_row.forward(row), permutation_col.forward(col), val)
    let permutation_row     =   Permutation::random(num_rows_global, seed_permute   );
    let permutation_col     =   Permutation::random(num_rows_global, seed_permute+1 );

    let mut rows_owned: HashSet<usize>     // the indices of the rows owned by this PE     
                            =   (row_owned_first_in .. row_owned_first_out).collect();    

    // initialize values
    // -----------------

    let start_time_initializing_values  
                            =   Instant::now();

    // this function returns the `index_row`th row of the permuted matrix, 
    // repersented by a pair of vectors (indices_row,indices_col)
    let get_row             =   | index_row: usize | -> (Vec<usize>, Vec<usize>) {
        let row_of_er       =   permutation_row.get_backward( index_row ); // NB: we have to use the backward permutation here
        let mut indices_col =   bernoulli_upper_unit_triangular_row(
                                    seed_matrix + index_row,
                                    num_rows_global,
                                    edge_probability,
                                    row_of_er,     
                                );
        for p in 0 .. indices_col.len() {
            indices_col[p]  =   permutation_col.get_forward( indices_col[p] ); 
        }        
        let indices_row     =   vec![ index_row; indices_col.len() ]; 
        (indices_row, indices_col)       
    };

    // generate the portion of the matrix owned by this PE
    let mut indices_row         =   Vec::new();
    let mut indices_col         =   Vec::new();
    for index_row in row_owned_first_in .. row_owned_first_out {
        let (indices_row_new, indices_col_new)  =   get_row( index_row );
        indices_row.extend_from_slice( & indices_row_new );
        indices_col.extend_from_slice( & indices_col_new );                                                                            
    }
    let num_entries         =   indices_row.len();
    let matrix              =   TriMat::from_triplets(
                                    (num_rows_global,num_rows_global),
                                    indices_row,
                                    indices_col,
                                    vec![1u8; num_entries], // fill with meaningless coefficients
                                );
    let matrix              =   matrix.to_csc();

    // the number and sum-of-column-indices of the nonzero entries in each row
    let mut row_sums            =   vec![ 0; num_rows_global];
    let mut row_counts          =   vec![ 0; num_rows_global];
    for column in 0 .. num_rows_global {
        let nz_entries          =   matrix.outer_view( column ).unwrap();
        let nz_indices          =   nz_entries.indices();
        for row in nz_indices {
            row_counts[    *row ]  +=  1;
            row_sums[      *row ]  +=  column;
        }        
    }

    // bucket for diagonal elements
    let mut diagonal_elements   =   vec![ vec![]; num_rows_global ];
    let diagonal_elements_union: Vec< Vec< (usize,usize) > >
                                =   vec![ vec![]; num_rows_global ];                               

    // wrap in LocalRwDarc's
    let matrix                  =   LocalRwDarc::new( world.team(), matrix                  ).unwrap();
    let row_sums                =   LocalRwDarc::new( world.team(), row_sums                ).unwrap();
    let row_counts              =   LocalRwDarc::new( world.team(), row_counts              ).unwrap();
    // let diagonal_elements       =   LocalRwDarc::new( world.team(), diagonal_elements       ).unwrap();
    let num_deleted_global      =   LocalRwDarc::new( world.team(), 0usize                  ).unwrap();
    let diagonal_elements_union =   LocalRwDarc::new( world.team(), diagonal_elements_union ).unwrap();
    
    time_to_initialize          =   Instant::now().duration_since(start_time_initializing_values);
    world.barrier();

    // enter loop
    // -----------------

    let start_time_main_loop    =   Instant::now();                                      

    for epoch in 0..num_rows_global {

        let columns_to_delete = {
            // Step 1: identify all rows with a single nonzero entry, and their corresponding columns
            //         then push the identified elements to 

            // let mut diagonal_elements_temp  
            //                             =   diagonal_elements.write();
            let row_counts_temp         =   row_counts.read();
            let row_sums_temp           =   row_sums.read();  
            
            for row in rows_owned.iter() {
                if row_counts_temp[ *row ]  == 1 {
                    let diagonal_element    =   (
                                                    row.clone(),
                                                    row_sums_temp[*row].clone() // there's only one entry in this row, so its sum is the column where the nz entry appears
                                                );                                          
                    diagonal_elements[ epoch ].push( diagonal_element );
                }
            }
        
            world.barrier();
        
            // list the paired columns, and remove the paired rows from `rows_owned`
            let mut columns_to_delete   =   Vec::new();
            for (row, col) in   & diagonal_elements[ epoch ] {
                columns_to_delete.push(col.clone());
                rows_owned.remove(row);
            }

            columns_to_delete
        };

        //  Step 2: delete the rows and columns we've just identified
        if ! columns_to_delete.is_empty() {
            let am  =   ToposortAm{
                            matrix:             matrix.clone(),           
                            row_sums:           row_sums.clone(),         
                            row_counts:         row_counts.clone(),       
                            // diagonal_elements:  diagonal_elements.clone(),
                            columns_to_delete:  columns_to_delete,
                            num_deleted_global: num_deleted_global.clone(),
                        };
            let _ = world.exec_am_all( am );
        }

        world.wait_all();          
        world.barrier();             

        if **num_deleted_global.read() == num_rows_global {

            time_to_loop            =   Instant::now().duration_since(start_time_main_loop);    

            let start_time_pooling_permutations  
                                    =   Instant::now();                    

            // make a vector copy of all the diagonal elements found on this PE
            let diagonal_elements_to_move: Vec<_> = diagonal_elements.iter().cloned().collect();



            // send all elements to PE0 for integration
            let am  =   PoolDiagonalElementsAm{
                diagonal_elements_to_move,           
                diagonal_elements_to_stay:  diagonal_elements_union.clone(),
            };
            let _ = world.exec_am_pe( 0, am );  

            world.wait_all();          
            world.barrier();               

            time_to_pool            =    Instant::now().duration_since(start_time_pooling_permutations);                

            break
        }
    }


    
    if world.my_pe() == 0 {

        if verify {
            let start_time_verifying_permutation
                                            =   Instant::now();  

            // concatenate all elements on PE0
            let zipped_permutation          =   diagonal_elements_union.read().concat();

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
            for index_row_old in 0 .. num_rows_global {
                let index_row_new       =   new_permutation_row[ index_row_old ];
                for index_col_old in get_row( index_row_old ).1 {
                    let index_col_new   =   new_permutation_col[ index_col_old ];
                    if index_row_new > index_col_new {
                        panic!("Permutation failed to produce an upper triangular matrix");
                    }
                }
            }

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
        println!("Matrix size:                        {:?}", cli.matrix_size );        
        println!("Edge probability:                   {:?}", cli.edge_probability );
        println!("Average number of nonzeros per row: {:?}", 1.0 + cli.edge_probability * ((cli.matrix_size -1) as f64) / 2.0 );        
        println!("Random seed:                        {:?}", cli.random_seed );
        println!("Number of PE's:                     {:?}", world.num_pes() );        
        println!("");        
        println!("Time to initialize matrix:          {:?}", time_to_initialize );
        println!("Time to identify diagonal elements: {:?}", time_to_loop );
        println!("Time to pool diagonal elements:     {:?}", time_to_pool );
        if verify{
            println!("Time to verify permutations:        {:?}", time_to_verify );        
        } else {
            println!("Time to verify permutations:        Not applicable, did not verify" );                    
        }
        println!("");
        println!("{:?}", time_to_loop.as_secs() as f64 + time_to_loop.subsec_nanos() as f64 * 1e-9); // we add this extra line at the end so we can feed the run time into a bash script, if desired  
    }
}





//  ===========================================================================
//  COMMAND LINE INTERFACE
//  ===========================================================================



#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The number of rows and columns of the matrix
    #[arg(short, long, )]
    matrix_size: usize,

    /// Probability that each entry will be nonzero (between 0 and 1)
    #[arg(short, long, )]
    edge_probability: f64,

    /// Seed for the random generator that determines the matrix.
    #[arg(short, long, )]
    random_seed: usize,

    /// Flag to determine whether or not to verify the permutation
    #[arg(short, long, )]
    verify: Option<bool>,    
}
