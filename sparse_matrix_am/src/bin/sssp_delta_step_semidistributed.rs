//! Delta-stepping algorithm for single-source shortest path; see the Bale documentation for background.
//!
//! This implementation keeps tentative weights for every vertex in the graph on PE 0, unlike `sssp_delta_step` which keeps tenative weights distributed over PE's.


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
use std::mem;
use std::thread;
use std::time::{Instant, Duration};

//  ---------------------------------------------------------------------------
//  NOTES

//  OPPORTUNITIES FOR IMPROVEMENT
//  - place vertices from the bottom bucket into a "holding tank" when we remove them; this way
//    we don't have to do a linear search over every vertex owned by a PE every time we clear
//    a bucket
//  - should we handle special case of empty graph? 

// EXAMPLE RUN COMMAND
// ```
// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=2 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 10 --random-seed 0 --delta 0.3 --write-to-json
// LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=4 srun --cpus-per-task=4 --cpu-bind=ldoms,v  -N 16 --ntasks-per-node=16 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 10 --random-seed 0 --delta 0.3
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
    let delta                   =   cli.delta;
    let delta_ordered           =   OrderedFloat::<f64>(delta);
    let number_of_buckets       =   1 + ( (1.0 / delta) as usize); 
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
    

    // define the adjacency matrix
    // ----------------------------    
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

    if cli.debug {
        println!("!!!!!!!!! DELETE THE CSR MATRIX AFTER DEBUGGIN");
    }


    // dump contents into new hash-of-vec matrix format with separate vecs for short and long edges
    let mut owned_row_vectors: HashMap< usize, (Vec<(usize,OFloat)>, Vec<(usize,OFloat)>) > = HashMap::new();
    for ((row, col),weight) in indices_row.into_iter().zip(indices_col).zip(weights) {
        if weight < delta_ordered {
            owned_row_vectors.entry(row).or_default().0.push( (col, weight) ) // push to the short edges
        } else {
            owned_row_vectors.entry(row).or_default().1.push( (col, weight) ) // push to the long edges
        }
            
    }

    if cli.debug{
        println!("FINISHED BUILDING HASH MATRIX");
    }

    // initialize distributed varialbles
    // ---------------------------------
    // NB: we only allocate capacity for these scores on PE 0; we allocate trivial capacity on the other PE's
    let mut tentative_weights        =   match world.my_pe() == 0 {
                                            True    =>  { vec![ OrderedFloat(f64::INFINITY); num_rows_global ] },
                                            False   =>  { vec![ OrderedFloat(f64::INFINITY); 1 ] },
                                        };
    tentative_weights[0]             =   OrderedFloat(0.0); // the base node has weight 0
    let tentative_weights            =   LocalRwDarc::new( world.team(), tentative_weights        ).unwrap();

    
    // initialize a hashmap of form   pe_number |-> Vec< some_vertices_owned_by_pe >
    // PE0 will use this object to store all its lookup requests before sending them out
    let mut hash_of_requests        =   HashMap::< usize, Vec<usize> >::new();    

    // initialize a vector to hold the lookup requests received from PE0
    let mut lookups_requested_of_self  =   Vec::new();
    let lookups_requested_of_self      =   LocalRwDarc::new( world.team(), lookups_requested_of_self      ).unwrap();
    
    // initilize hashset for the vertices in the bottom bucket
    let mut bottom_bucket_contents  =   HashSet::new();
    let bottom_bucket_contents      =   LocalRwDarc::new( world.team(), bottom_bucket_contents      ).unwrap();
    
    // initialize hashmap of form
    // key |->  (ShortEdges, LongEdges)
    // where ShortEdges and ShortEdges have type Vec< (neighbor, edge_weight) >
    let mut bottom_vertex_rows      =   HashMap::< 
                                            usize, 
                                            (
                                                Vec<(usize, OFloat)>,
                                                Vec<(usize, OFloat)>,
                                            )
                                        >::new();
    let bottom_vertex_rows          =   LocalRwDarc::new( world.team(), bottom_vertex_rows      ).unwrap();    


    // initialize nondistributed parameters
    // ------------------------------------
    let mut bottom_bucket_index         =   0;
    let mut lower_tent_limit            =   OrderedFloat(0.0);
    let mut upper_tent_limit            =   OrderedFloat(0.0);
    let mut vertices_to_relax           =   Vec::new();


    // enter loop
    // -----------------


    time_to_initialize              =   Instant::now().duration_since(start_time_initializing_values);     
    let start_time_main_loop        =   Instant::now();    


    if cli.debug{
        println!("ENTERING LOOP");
    }
    loop {

        if cli.debug{
            println!("pe = {:?}", world.my_pe());
        }

        if world.my_pe() == 0 {
            if world.block_on(bottom_bucket_contents.read()).is_empty() {

                if cli.debug{
                    println!("starting PE 0 top matter");
                }

                // relax the long edges for the current bucket
                // -------------------------------------------
                
                // the edges to relax are contained in the locally held part of the adjacency matrix dedicated to the bottom bucket
                {
                    let mut tentative_weights_temp              =   world.block_on(tentative_weights.write());
                    let mut bottom_vertex_rows_temp             =   world.block_on(bottom_vertex_rows.write());
                    let mut tent_base;                       
                    let mut tent_maybe;
                    let mut tent_neigh;                
                    for (relaxee, edges) in bottom_vertex_rows_temp.drain() { // this will DRAIN the local matrix
                        tent_base                               =   tentative_weights_temp[ relaxee ];
                        for ( neighbor, edge_weight ) in edges.1.iter() {  // the .1 indexes into the long edges
                            tent_maybe                          =   tent_base + edge_weight;
                            tent_neigh                          =   tentative_weights_temp[ *neighbor ];
                            if tent_neigh > tent_maybe {
                                // update tentative weight if necessary
                                tentative_weights_temp[ *neighbor ]   
                                                                =   tent_maybe;
                            }
                        }                    
                    }
                }

                if cli.debug{
                    println!("FINISHED A WRITE LOOP FOR TENT WEIGHTS");                
                }
                         

                // calculate the new bottom bucket
                // -------------------------------

                // find the min tentative weight of the unsettled vertices
                let tentative_weights_temp                  
                                        =   world.block_on(tentative_weights.read());                
                let min_tent_weight     =   tentative_weights_temp
                                                .iter()
                                                .filter(|x| **x >=  upper_tent_limit )
                                                .min();

                if cli.debug{
                    println!("FINISHED A READ LOOP FOR TENT WEIGHTS");                                                                
                }
                
                // if min_tent_weight is None the break; every vertex is settled
                if min_tent_weight.is_none() { break }

                // otherwise unwrap min_tent_weight
                let min_tent_weight     =   min_tent_weight.unwrap();

                // calculate the bottom bucket index and upper/lower limits
                bottom_bucket_index     =   ( min_tent_weight / delta ).floor() as usize;
                lower_tent_limit        =   OrderedFloat( delta * ( bottom_bucket_index as f64 ) );
                upper_tent_limit        =                 delta_ordered + lower_tent_limit;
                
                // find the vertices in the new bottom bucket
                let mut bottom_bucket_temp  
                                        =   world.block_on(bottom_bucket_contents.write());
                for (vertex, tent) in tentative_weights_temp.iter().enumerate() {
                    if ( lower_tent_limit <= *tent ) && ( *tent < upper_tent_limit ) {
                        bottom_bucket_temp.insert( vertex );
                    }
                }  
                
                if cli.debug{
                    println!("FINISHED LAST WRITE LOOP FOR TENT WEIGHTS");                                                                                
                }
            }


            // collect find any new vertices whose rows we have to lookup
            // --------------------------------------------------

            for vertex in world.block_on(bottom_bucket_contents.read()).iter() {
                // get the owning pe
                let owning_pe = vertex / rows_per_pe;
                // add this vertex to the request for that pe
                hash_of_requests.entry(owning_pe).or_default().push(*vertex);
            }


        }

        if cli.debug{
            println!("INSIDE LOOP: FINISHED PE 0 TOP MATTER");
        }

        // send look-up requests (for rows of the adjacency matrix)
        // ---------------------

        // this will fill the vector `lookups_requested_of_self` on each PE with the vertices that that PE needs to send to PE0

        for (pe, vertices) in hash_of_requests.drain() { // this emptys the request hash
            let am  =   SendLookupRequestsAm{
                            vertices,
                            lookups_requested_of_self:    lookups_requested_of_self.clone(),
                        };
            let _   = world.exec_am_pe( pe, am );
        }

        world.wait_all();          
        world.barrier();    

        if cli.debug{
            println!("INSIDE LOOP: FINISHED SENDNG LOOKUP REQUESTS");              
        }


        // fulfill look-up requests (for rows of the adjacency matrix)
        // ------------------------

        let mut rows_to_send    =   Vec::with_capacity( world.block_on(lookups_requested_of_self.read()).len() );
        for vertex in world.block_on(lookups_requested_of_self.write()).drain(..) {
            // push the informatino for vertex to `rows_to_send`
            rows_to_send.push( 
                                (
                                    vertex.clone(),
                                    owned_row_vectors.remove( & vertex ).unwrap(),
                                )
            );          
        }
        let am                  =   SendRowsAm{
                                        rows_to_send,
                                        bottom_vertex_rows:    bottom_vertex_rows.clone(),
                                    };
        let _                   =   world.exec_am_pe( 0, am );          

        world.wait_all();          
        world.barrier();  

        if cli.debug{
            println!("INSIDE LOOP: FINISHED FULFILLING LOOKUP REQUESTS");        
        }
          


        // relax edges
        // -----------

        let mut tentative_weights_temp              =   world.block_on(tentative_weights.write());        
        let mut bottom_bucket_contents_temp         =   world.block_on(bottom_bucket_contents.write());
        vertices_to_relax.extend( bottom_bucket_contents_temp.drain() ); // move all vertices from the bottom bucket to a temporary holding bin

        for relaxee in vertices_to_relax.drain(..) {
            let tent_base                           =   tentative_weights_temp[ relaxee ].clone();
            let bottom_vertex_rows_temp             =   world.block_on(bottom_vertex_rows.read());
            for ( neighbor, edge_weight ) in bottom_vertex_rows_temp.get( & relaxee ).unwrap().1.iter() {  // the .0 indexes into the short edges
                let tent_maybe                      =   tent_base + edge_weight;
                let tent_neigh                      =   tentative_weights_temp[ *neighbor ];
                if tent_neigh > tent_maybe {
                    // update tentative weight if necessary
                    tentative_weights_temp[ *neighbor ]
                                                    =   tent_maybe;
                    // push the neighbor into the bottom bucket, if its score is low enough
                    // the `insert` method will make this insertion and assign a value of `true` 
                    // to `is_new_addition` iff neighbor was already an element of bottom_bucket_contents
                    if tent_maybe < upper_tent_limit {
                        let is_new_addition         =   bottom_bucket_contents_temp.insert( * neighbor );
                        // if the vertex is a new addition to the bottom bucket then create a request to get the correspondign row of the adjacency matrix
                        if is_new_addition {
                            // get the owning pe
                            let owning_pe           =   neighbor / rows_per_pe;
                            // add this vertex to the request for that pe
                            hash_of_requests.entry(owning_pe).or_default().push( neighbor.clone() );                            
                        }
                    }
                }
            }
        }

        if cli.debug{
            println!("INSIDE LOOP: FINISHED RELAXING SHORTED EDGES");                
        }
    }


    // finished -- report results
    // --------------------------------    


    let tentative_distances_pe_0
                                =   world.block_on(tentative_weights.read()).clone();

    
    if world.my_pe() == 0 {

        time_to_loop            =   Instant::now().duration_since(start_time_main_loop);   
        // let tentative_distances_pe_0
        //                         =   *world.block_on(ladle.read()).clone();
        // let tentative_distances_pe_0
        //                         =   ladle_refmut.tentative_distances.clone();
        
        //                         =   Vec::with_capacity( num_rows_owned );
        // for t in *world.block_on(ladle.read()).tentative_distances.iter().cloned() {
        //     tentative_distances_pe_0.push(t);
        // }


        if cli.write_to_json {

            println!("");
            println!("WRITING TO JSON!!!!!!!!!!!!!!!!!!!!!!!");
            println!("");

            // write the calculated path lengths to json
            // -----------------------------------------
            write_to_json_file("sssp_unit_test_data_delta_step_semidistributed.json", &tentative_distances_pe_0 );


            // a function to generate the slice of the weighted adjacency matrix owned by any pe
            // ---------------------------------------------------------------------------------
            let matrix_slice_for_pe     =   | pe: usize | -> (Vec<usize>,Vec<usize>,Vec<f64>) {
                // calculate which rows are owned by this pe
                let row_owned_first_in      =   rows_per_pe * pe;
                let row_owned_first_out     =   ( row_owned_first_in + rows_per_pe ).min( num_rows_global );
                let num_rows_owned          =   row_owned_first_out - row_owned_first_out;                
                let owned_row_indices       =   (row_owned_first_in..row_owned_first_out).collect::<Vec<usize>>();
                // 
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
                                                    .map(|x| rng.gen::<f64>() )
                                                    .collect();
                return (indices_row, indices_col, weights)    
                
            };

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
            write_to_json_file("sssp_unit_test_matrix_delta_step_semidistributed.json", &indices_row );
            write_to_json_file("sssp_unit_test_weight_delta_step_semidistributed.json", &weights );   

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
        
        println!("");
        println!("{:?}", time_to_loop.as_secs() as f64 + time_to_loop.subsec_nanos() as f64 * 1e-9); // we add this extra line at the end so we can feed the run time into a bash script, if desired                                                   

    }
}




//  ===========================================================================
//  ACTIVE MESSAGE
//  ===========================================================================



/// Allows PE0 to tell the other PE's what rows of the adjacency matrix it needs
#[lamellar::AmData(Debug, Clone)]
pub struct SendLookupRequestsAm {
    // the rows to send
    vertices:               Vec< usize >,
    // where the rows are destined
    lookups_requested_of_self: LocalRwDarc< Vec< usize > >,
}

#[lamellar::am]
impl LamellarAM for SendLookupRequestsAm {
    async fn exec(self) {        
        let mut lookups_requested_of_self      =   self.lookups_requested_of_self.write().await; // get a writable handle on the local ladle
        lookups_requested_of_self.extend( self.vertices.iter().cloned() );
    }
}    




/// Allows each PE to send some rows of the adjacency matrix to PE0
#[lamellar::AmData(Debug, Clone)]
pub struct SendRowsAm {
    // the rows to send
    rows_to_send:           Vec< 
                                (
                                    usize, 
                                    ( Vec<(usize, OFloat)>, Vec<(usize, OFloat)> ),
                                ) 
                            >,
    // where the rows are destined
    bottom_vertex_rows:     LocalRwDarc<
                                HashMap<
                                    usize,
                                    ( Vec<(usize, OFloat)>, Vec<(usize, OFloat)> )
                                >
                            >,
}

#[lamellar::am]
impl LamellarAM for SendRowsAm {
    async fn exec(self) {        
        let mut bottom_vertex_rows      =   self.bottom_vertex_rows.write().await; // get a writable handle on the local ladle
        bottom_vertex_rows.extend( self.rows_to_send.iter().cloned() );
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

    /// Delta parameter for the algorithm
    #[arg(short, long, )]
    delta: f64,    

    /// Turn debugging on
    #[arg(short, long, )]
    debug: bool,     
    
    /// If true, then write the first 1000 weights to a .json file
    #[arg(short, long, )]
    write_to_json: bool,         
}

