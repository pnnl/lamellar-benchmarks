//! Bellman-Ford algorithm; see the Bale documentation for background.


//  ---------------------------------------------------------------------------

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::matrix_constructors::dart_uniform_rows;

use clap::{Parser, Subcommand};
use ordered_float::OrderedFloat;
use rand::prelude::*;
use rand::seq::SliceRandom;
use sprs::{CsMat,TriMat};

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::time::{Instant, Duration};

//  ---------------------------------------------------------------------------






fn main() {

    println!("NOTE: when we perform a weight update with this function, we store a transposed copy of the adjacency matrix. This due to the data layout used by the algorithm to avoid certain types of communication.");

    let world                   =   lamellar::LamellarWorldBuilder::new().build();    

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   rows_per_thread_per_pe * world.num_threads_per_pe();
    let num_rows_global         =   rows_per_pe * world.num_pes();    
    let avg_nnz_per_row         =   cli.avg_nnz_per_row;
    let seed_matrix             =   cli.random_seed;  

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
    let num_rows_owned          =   row_owned_first_out - row_owned_first_out;
    let owned_row_indices       =   (row_owned_first_in..row_owned_first_out).collect::<Vec<usize>>();    
    

    // define the adjacency matrix
    // ----------------------------    
    // let (indices_row,indices_col)         
    //                             =   dart_uniform_rows(
    //                                     seed_matrix + world.my_pe(), // random seed
    //                                     num_rows_global, // number of matrix columns
    //                                     avg_nnz_per_row * rows_per_pe, // desired number of nonzeros
    //                                     & owned_row_indices, // list of row indices; a row will be generated for each index
    //                                 );



    // // define a random number generator
    // let mut rng             =   rand::rngs::StdRng::seed_from_u64( (seed_matrix +1) as u64 );
    // // define a vector of randomly generated weights
    // let weights: Vec<_>     =   (0..indices_col.len())
    //                                 .map(|x| OrderedFloat(rng.gen::<f64>()) )
    //                                 .collect();



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
   

    // use the function to generate a matrix
    // ---------------------------------------------------------------------------------
    let (indices_row, indices_col, weights) 
                                =   matrix_slice_for_pe( world.my_pe() );    
    // let weights: Vec<_>         =   weights.into_iter().map(|x| OrderedFloat(x)).collect();

    let num_entries             =   indices_row.len();
    let matrix                  =   TriMat::from_triplets(
                                        (num_rows_global,num_rows_global),
                                        indices_col,  // <------------------------------------- !!!!!!!!!!!!!!!!!!!!!!!!!!! WHEN WE CALL TriMat::from_triplets WE REVERSE ROWS AND COLUMNS, ESSENTIALLY TRANSPOSING THE MATIRX
                                        indices_row,
                                        weights, 
                                    );
    let matrix                  =   matrix.to_csr::<usize>();

    // set tentative scores
    // --------------------------------
    let mut tentative_scores    =   vec![ OrderedFloat(f64::INFINITY); num_rows_global ];
    tentative_scores[0]         =   OrderedFloat(0.0); // the base node has weight 0

    // wrap in LocalRwDarc's
    let tentative_scores        =   LocalRwDarc::new( world.team(), tentative_scores        ).unwrap();
    let scores_have_changed     =   LocalRwDarc::new( world.team(), false                   ).unwrap();
    
    time_to_initialize          =   Instant::now().duration_since(start_time_initializing_values); 


    // enter loop
    // -----------------

    let start_time_main_loop    =   Instant::now();    

    for epoch in 0.. (10 * num_rows_global) {

        **world.block_on(scores_have_changed.write())               =   false; // update our flag

        // check to see if any scores can be reduced by relaxing edges
        let new_scores = {

            let mut new_scores                      =   Vec::new();
            let mut tentative_scores_temp           =   world.block_on(tentative_scores.write());
            
            for row in row_owned_first_in .. row_owned_first_out {

                let mut tentative_score             =   tentative_scores_temp[ row ].clone();

                // relax each edge in this row
                for ( col, weight ) in matrix.outer_view( row ).unwrap().iter() {
                    let candidate_value             =   tentative_scores_temp[ col ] + weight;
                    if candidate_value < tentative_score {
                        tentative_score             =   candidate_value;
                    }
                }
                if tentative_score < tentative_scores_temp[ row ] {
                    new_scores.push( (row, tentative_score.into_inner()) ); // push the (vertex,value) pair to a list that will broadcast to all the nodes
                    tentative_scores_temp[ row ]    =   tentative_score; // update the local score
                }
            }

            world.barrier();
        
            new_scores
        };

        //  Step 2: if necessary, broadcast the updated vertex scores
        if ! new_scores.is_empty() {
            let am  =   UpdateScoresAm{
                            new_scores:             new_scores,
                            receives_new_scores:    tentative_scores.clone(),
                            scores_have_changed:    scores_have_changed.clone()
                        };
            let _   =   world.exec_am_all( am );
        }

        world.wait_all();          
        world.barrier();             

        println!("NOTE: WE HAVE TO REINSTATE THE STOPPING CONDITION; LOOK FOR THIS MESSAGE IN THE CODE");
        // if ! **world.block_on(scores_have_changed.read()) {
        //     break
        // }
    }
    

    // finished -- report results
    // --------------------------------    



    let tentative_distances_pe_0: Vec< f64 >
                                =   (*world.block_on(tentative_scores.read()))
                                        .clone()
                                        .into_iter()
                                        .take( rows_per_pe )
                                        .map(|of| of.into_inner())
                                        .collect();
    
    if world.my_pe() == 0 {

        time_to_loop            =   Instant::now().duration_since(start_time_main_loop);            

        if cli.write_to_json {

            println!("");
            println!("WRITING TO JSON!!!!!!!!!!!!!!!!!!!!!!!");
            println!("");

            // write the calculated path lengths to json
            // -----------------------------------------
            write_to_json_file("sssp_unit_test_data_bellman_ford.json", &tentative_distances_pe_0 );


            // write the full adjacency matrix to json
            // ---------------------------------------            
            let mut indices_row         =   Vec::new();
            let mut indices_col         =   Vec::new();  
            let mut weights             =   Vec::new();           
            for pe in 0 .. world.num_pes() {
                let (mut ir, mut ic, mut w)           =   matrix_slice_for_pe( pe );
                indices_row.append( &mut ir );
                indices_col.append( &mut ic );
                weights.append( &mut w );
            }
            indices_row.append( &mut indices_col );
            write_to_json_file("sssp_unit_test_matrix_bellman_ford.json", &indices_row );
            write_to_json_file("sssp_unit_test_weight_bellman_ford.json", &weights );   

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

    }
}



//  ===========================================================================
//  ACTIVE MESSAGE
//  ===========================================================================



/// Allows each node to send updated scores to the other PE's
#[lamellar::AmData(Debug, Clone)]
pub struct UpdateScoresAm {
    pub new_scores:             Vec< (usize, f64) >,            // a collection of new vertex scores
    pub receives_new_scores:    LocalRwDarc< Vec< OrderedFloat<f64> > >,   // the score ledger we want to update with the new scores
    pub scores_have_changed:    LocalRwDarc< bool >,                  // flag to track whether any scores have changed
}

#[lamellar::am]
impl LamellarAM for UpdateScoresAm {
    async fn exec(self) {        
        let mut receives_new_scores         =   self.receives_new_scores.write().await; // get a writable handle on the local collection of diagonal elements
        let mut scores_have_changed         =   self.scores_have_changed.write().await;
        **scores_have_changed               =   true; // mark that at least one score has changed
        for ( vertex, score ) in self.new_scores.iter() {
            receives_new_scores[ *vertex ]  =   OrderedFloat( * score );
        }
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

    /// Turn debugging information on
    #[arg(short, long, )]
    random_seed: usize,

    /// If true, then write the first 1000 weights to a .json file
    #[arg(short, long, )]
    write_to_json: bool,  
    
    /// If true, then generate a cycle graph instead of a random one
    #[arg(short, long, )]
    graph_type: String,      
}





//  ===========================================================================
//  PERFORMANCE
//  ===========================================================================


// Number of PE's:                     4
// Cores per PE:                       2
// Matrix size:                        400000
// Rows per PE:                        100000
// Avg nnz per row:                    10.0
// Random seed:                        0

// Time to initialize matrix:          164.042262ms
// Time to get shortest paths:         118.044479ms