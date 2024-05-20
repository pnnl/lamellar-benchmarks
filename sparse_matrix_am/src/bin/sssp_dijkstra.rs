
//! Dijkstra algorithm for single-source-shortest path
//!
//! Uses an open source Rust crate for computation.


//  ---------------------------------------------------------------------------



use sparse_matrix_am::matrix_constructors::dart_uniform_rows;
use sparse_matrix_am::sssp_dijkstra::{dijkstra_from_row_col_weight};

use clap::{Parser, Subcommand};
use ordered_float::OrderedFloat;
use rand::prelude::*;
use rand::seq::SliceRandom;
use sprs::{CsMat,TriMat};

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::mem;
use std::thread;
use std::time::{Instant, Duration};

use pathfinding::prelude::dijkstra_all;

//  ---------------------------------------------------------------------------
//  NOTES

//  OPPORTUNITIES FOR IMPROVEMENT
//  - place vertices from the bottom bucket into a "holding tank" when we remove them; this way
//    we don't have to do a linear search over every vertex owned by a PE every time we clear
//    a bucket
//  - should we handle special case of empty graph? 

// EXAMPLE RUN COMMAND
// ```
// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=2 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=2 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --delta 0.3 --write-to-json
// ```

// BENCHMARKING
// TRY: GRAPH 500 DATABASE: https://graphchallenge.mit.edu/data-sets
//      SPARSE SUITE MATRIX COLLECTION


// --------------------------------------------------------------------        


type OFloat = OrderedFloat< f64 >;





fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();    

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   rows_per_thread_per_pe * world.num_threads_per_pe();
    let num_rows_global         =   rows_per_pe * world.num_pes();    
    let avg_nnz_per_row         =   cli.avg_nnz_per_row;
    let seed_matrix             =   cli.random_seed;  
    let debug                   =   cli.debug;

    // initialize timer variables
    // --------------------------
    let mut time_to_initialize  =   Instant::now().duration_since(Instant::now());
    let mut time_to_loop        =   Instant::now().duration_since(Instant::now());
    let start_time_initializing_values  
                                =   Instant::now();

    // define parameters
    // -----------------

    let row_owned_first_in      =   rows_per_pe * world.my_pe();
    let row_owned_first_out     =   ( row_owned_first_in + rows_per_pe ).min( num_rows_global );
    let num_rows_owned          =   row_owned_first_out - row_owned_first_in;
    let owned_row_indices       =   (row_owned_first_in..row_owned_first_out).collect::<Vec<usize>>();     
    

    // ----------------------------
    // define the adjacency matrix
    // ----------------------------    



    // a function to generate the slice of the weighted adjacency matrix owned by any pe
    // ---------------------------------------------------------------------------------
    let mut matrix_slice_for_pe =   | pe: usize | -> (Vec<usize>,Vec<usize>,Vec<OrderedFloat<f64>>) {
        
        // ---------------------------------------------
        // generate a cycle graph, if the cycle flag is active
        // ---------------------------------------------  
        
        let (cycle, bicycle, random) = (String::from("cycle"), String::from("bicycle"), String::from("random"));

        println!("graph type === {:?}", cli.graph_type.clone() );

        if cli.graph_type.as_str() == String::from("cycle") {
            println!("-- cycle graph ");
            let indices_row             =   owned_row_indices.clone();
            let indices_col: Vec<_>     =   indices_row // each edge has form N --> (N+1) mod (# vertices)
                                                .iter()
                                                .cloned()
                                                .map(|x| ( x + 1) % num_rows_global )
                                                .collect();
            let weights                 =   vec![ OrderedFloat(1f64); indices_row.len() ]; // all edges get weight 1
            return (indices_row, indices_col, weights)   
        }
        
        if cli.graph_type.as_str() == String::from("bicycle") {
            println!("-- bicycle graph ");                
            let x                       =   owned_row_indices.clone();
            let y: Vec<_>               =   owned_row_indices // each edge has form N --> (N+1) mod (# vertices)
                                                .iter()
                                                .cloned()
                                                .map(|x| ( x + 1) % num_rows_global )
                                                .collect();
            // connect every node to the node that precedes and follows it
            let mut indices_row         =   x.clone();
            let mut indices_col         =   y.clone();
            indices_row.extend_from_slice( & y.clone() );
            indices_col.extend_from_slice( & x.clone() );                

            let weights                 =   vec![ OrderedFloat(1f64); indices_row.len() ]; // all edges get weight 1
            return (indices_row, indices_col, weights)  
        }

        if cli.graph_type.as_str() == String::from("random") {
            println!("-- randome graph ");                
            let (indices_row, indices_col)  
                                        =   dart_uniform_rows(
                                                seed_matrix + pe, // random seed
                                                num_rows_global, // number of matrix columns
                                                avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
                                                & owned_row_indices, // list of row indices; a row will be generated for each index                
                                            );

            let (indices_row_0, indices_col_0)  
                                        =   dart_uniform_rows(
                                                seed_matrix + pe, // random seed
                                                num_rows_global, // number of matrix columns
                                                avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
                                                & owned_row_indices, // list of row indices; a row will be generated for each index                
                                            );
            println!("indices_col {:?}", & indices_col );
            if indices_row != indices_row_0 {
                println!("");
                println!("!!!!!!!!! SAME INPUT DIFFERENT OUTPUT ");
                println!("len(output_0) = {:?}, len(utput_1) = {:?}", indices_row_0.len(), indices_row.len() );
                println!("{:?}", &indices_row_0);
                println!("{:?}", &indices_row);                    
                println!("");                   
            }                                            
            // define a random number generator
            let mut rng                 =   rand::rngs::StdRng::seed_from_u64( (seed_matrix +1) as u64 );
            // define a vector of randomly generated weights
            let weights: Vec<_>         =   (0..indices_col.len())
                                                .map(|x| OrderedFloat(rng.gen::<f64>()) )
                                                .collect();
            return (indices_row, indices_col, weights) 
        }     
        
        println!("-- edgeless graph ");                                
        return (Vec::new(),Vec::new(),Vec::new())        
               
    };                                   


    // let matrix_slice_for_pe     =   match cli.cycle_graph {
    //     false => {
    //         // generate a random graph
    //         | pe: usize | -> (Vec<usize>,Vec<usize>,Vec<OrderedFloat<f64>>) {
    //             let (indices_row, indices_col)  
    //                                         =   dart_uniform_rows(
    //                                                 seed_matrix + pe, // random seed
    //                                                 num_rows_global, // number of matrix columns
    //                                                 avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
    //                                                 & owned_row_indices, // list of row indices; a row will be generated for each index                
    //                                             );
        
    //             let (indices_row_0, indices_col_0)  
    //                                         =   dart_uniform_rows(
    //                                                 seed_matrix + pe, // random seed
    //                                                 num_rows_global, // number of matrix columns
    //                                                 avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
    //                                                 & owned_row_indices, // list of row indices; a row will be generated for each index                
    //                                             );
    //             println!("indices_col {:?}", & indices_col );
    //             if indices_row != indices_row_0 {
    //                 println!("");
    //                 println!("!!!!!!!!! SAME INPUT DIFFERENT OUTPUT ");
    //                 println!("len(output_0) = {:?}, len(utput_1) = {:?}", indices_row_0.len(), indices_row.len() );
    //                 println!("{:?}", &indices_row_0);
    //                 println!("{:?}", &indices_row);                    
    //                 println!("");                   
    //             }                                            
    //             // define a random number generator
    //             let mut rng                 =   rand::rngs::StdRng::seed_from_u64( (seed_matrix +1) as u64 );
    //             // define a vector of randomly generated weights
    //             let weights: Vec<_>         =   (0..indices_col.len())
    //                                                 .map(|x| rng.gen::<f64>() )
    //                                                 .collect();
    //             return (indices_row, indices_col, weights)              
    //         }            
    //     } 
    //     true => {
    //         // generate a cycle graph            
    //         | pe: usize | -> (Vec<usize>,Vec<usize>,Vec<OrderedFloat<f64>>) {
    //                 let indices_row             =   owned_row_indices.clone();
    //                 let indices_col: Vec<_>     =   indices_row // each edge has form N --> (N+1) mod (# vertices)
    //                                                     .iter()
    //                                                     .cloned()
    //                                                     .map(|x| ( x + 1) % num_rows_global )
    //                                                     .collect();
    //                 let weights                 =   vec![ OrderedFloat(1f64); indices_row.len() ]; // all edges get weight 1
    //                 return (indices_row, indices_col, weights)              
    //             }           
    //     }              
        
    // };    

    // use the function to generate the matrix
    // ---------------------------------------
    let owned_row_indices       =   (row_owned_first_in..row_owned_first_out).collect::<Vec<usize>>();
    let (indices_row,indices_col)         
                                =   dart_uniform_rows(
                                        seed_matrix + world.my_pe(), // random seed
                                        num_rows_global, // number of matrix columns
                                        avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
                                        & owned_row_indices, // list of row indices; a row will be generated for each index
                                    );


    // define a random number generator
    let mut rng             =   rand::rngs::StdRng::seed_from_u64( (seed_matrix +1) as u64 );
    // define a vector of randomly generated weights
    let weights: Vec<_>     =   (0..indices_col.len())
                                    .map(|x| OrderedFloat(rng.gen::<f64>()) )
                                    .collect();
    let num_entries             =   indices_row.len();
    let matrix                  =   TriMat::from_triplets(
                                        (num_rows_global,num_rows_global),
                                        indices_row.clone(),
                                        indices_col.clone(),
                                        weights.clone(), 
                                    );
    let matrix                  =   matrix.to_csr::<usize>();
    println!("!!!!!!!!! DELETE THE CSR MATRIX AFTER DEBUGGIN");





    // write the full adjacency matrix to json
    // ---------------------------------------            
    let mut indices_row         =   Vec::new();
    let mut indices_col         =   Vec::new();  
    let mut weights             =   Vec::new();           
    for pe in 0 .. world.num_pes() {
        let (mut ir, mut ic, mut w)           =   matrix_slice_for_pe( pe );
        println!("slice of ir: {:?}", &ir );
        println!("slice of ic: {:?}", &ic );        
        indices_row.append( &mut ir );
        indices_col.append( &mut ic );
        weights.append( &mut w );
    }

    let mut indices_both = indices_row.clone();
    indices_both.append( &mut indices_col.clone() );
    write_to_json_file("sssp_unit_test_matrix_dijkstra.json", &indices_both );
    write_to_json_file("sssp_unit_test_weight_dijkstra.json", &weights );   


    // compute shortest paths
    // --------------------------------    
    let weights: Vec<_>             =   weights.into_iter().map(|x| x.into_inner() ).collect();
    let tentative_distances_pe_0    =   dijkstra_from_row_col_weight( & indices_row, & indices_col, & weights, num_rows_global );


    // // compute shortest paths (deprecated; uses a dependency i haven't gotten to work)
    // // ---------------------------------------    

    // // dump contents into new hash-of-vec matrix format with separate vecs for short and long edges
    // let mut owned_row_vectors: HashMap< usize, Vec<(u32,OFloat)> > = HashMap::new();
    // for ((row, col),weight) in indices_row.into_iter().zip(indices_col).zip(weights) {
    //     owned_row_vectors.entry(row).or_default().push( ( col as u32 , OrderedFloat(weight) ) )
    // }


    // let successors = | row: &u32 | -> Vec<( u32, OFloat )>  {
    //     owned_row_vectors.entry( row.clone() as usize ).or_default().clone()
    // };

    // let reachables = dijkstra_all(&0, successors); // hashmap of form N |--> (predecesor, length_of_minimal_path)

    // println!("!!!!!!!!!!REACHABLES = {:?}", &reachables );
    // let mut tentative_distances_pe_0
    //                             =   vec![ 0.0; num_rows_global ];
    // for ( key, val ) in reachables.iter() {
    //     tentative_distances_pe_0[ key.clone() as usize ] = val.1.clone().into_inner();
    // }



    // finished -- report results
    // --------------------------------    


    
    if world.my_pe() == 0 {


        if cli.write_to_json {

            println!("");
            println!("WRITING TO JSON!!!!!!!!!!!!!!!!!!!!!!!");
            println!("");

            // write the calculated path lengths to json
            // -----------------------------------------
            write_to_json_file("sssp_unit_test_data_dijkstra.json", &tentative_distances_pe_0 );             

        }   

        println!("");                                                                                                        
        println!("Finished successfully");                                                                                                       
        println!("");                                                                                                        
        println!("Number of PE's:                     {:?}", world.num_pes() );                                                                                                          
        println!("Cores per PE:                       {:?}", world.num_threads_per_pe());                                                                                                               
        println!("Matrix size:                        {:?}", num_rows_global );                                                                                                      
        println!("Rows per thread per PE:             {:?}", rows_per_thread_per_pe );                                                                                                               
        println!("Avg nnz per row:                    {:?}", matrix.nnz() as f64 / rows_per_pe as f64 );                                                                                                     
        println!("Random seed:                        {:?}", cli.random_seed );                                                                                                      
        println!("");                                                                                                                
        println!("Time to initialize matrix:          {:?}", time_to_initialize );                                                                                                       
        println!("Time to get shortest paths:         {:?}", time_to_loop );                                                                                                     
        println!("");                                                                                                        
        // println!("Tenative distances on PE 0:         {:?}", tentative_distances_pe_0);                                                                                                      

    }
}



//  ===========================================================================
//  WRITE OUTPUT TO JSON (OPTIONALLY)
//  ===========================================================================



use serde_json::to_writer;
use serde::ser::Serialize;
use std::env;
use std::fs::File;

fn write_to_json_file< T >(filename: &str, data: &[T]) 
    where 
        T:  Sized + Serialize
{
    // Get the current directory
    let current_dir = env::current_dir().unwrap();

    // Construct the path to the JSON file relative to the current directory
    let file_path = current_dir.join(filename);

    // Create a new file at the specified path
    let file = File::create(file_path).unwrap();

    // Serialize the data to JSON and write it to the file
    to_writer(file, data).unwrap();
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

    /// Random seed to initialize matrix
    #[arg(short, long, )]
    random_seed: usize, 

    /// Turn debugging on
    #[arg(short, long, )]
    debug: bool,     
    
    /// If true, then write the first 1000 weights to a .json file
    #[arg(short, long, )]
    write_to_json: bool,       
    
    /// If true, then generate a cycle graph instead of a random one
    #[arg(short, long, )]
    graph_type: String,           
}

