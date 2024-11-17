
//! We use this to get a ballpark estimate of the maximum out-degree of a node in one of our random graphs


//  ---------------------------------------------------------------------------
//  FINDINGS
//  ---------------------------------------------------------------------------

// WE FIND THAT THE MAXIMUM OUT DEGREE IS CONSISTENTLY AROUND 30.
// FOR EXAMPLE, THE TWO COMMANDS BELOW YIELD 28 AND 29, respectively, EVEN
// THOUGH ONE GRAPH HAS 100K NODES AND THE OTHER HAS 1M

//  ---------------------------------------------------------------------------
// EXAMPLE RUN COMMAND
//  ---------------------------------------------------------------------------
// ```
// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/count_max_out_degree --rows-per-thread-per-pe 1000000 --avg-nnz-per-row 10 --graph-type random --random-seed 0
// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/count_max_out_degree --rows-per-thread-per-pe 100000  --avg-nnz-per-row 10 --graph-type random --random-seed 0
// ```





// --------------------------------------------------------------------     


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

use sparse_matrix_am::matrix_constructors::max_repeats;


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
            if cli.debug {
                println!("indices_col {:?}", & indices_col );
            }
            
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


    println!("Maximum out degree: {:?}", max_repeats(indices_col) )

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
    
    /// If true, then generate a cycle graph instead of a random one
    #[arg(short, long, )]
    graph_type: String,        
    
    /// If true, then generate a cycle graph instead of a random one
    #[arg(short, long, )]
    debug: bool,            
}

