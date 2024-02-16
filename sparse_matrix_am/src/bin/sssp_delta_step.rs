//! Delta-stepping algorithm for single-source shortest path; see the Bale documentation for background.


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
// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=2 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=2 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --delta 0.3
// ```

// BENCHMARKING
// TRY: GRAPH 500 DATABASE: https://graphchallenge.mit.edu/data-sets
//      SPARSE SUITE MATRIX COLLECTION


// --------------------------------------------------------------------        


type OFloat = OrderedFloat< f64 >;


/// Returns the index of the bucket that contains the float
///
/// That is, it returns the index i such that float lies in the half open interval [ i*delta, (i+1)*delta )
pub fn bucket_index_of_float( float: OFloat, delta: f64 ) -> usize {
    ( f64::from(float) / delta ).floor() as usize  // the unwrap command converts OrderedFloat<f64> to f64
}

/// Contains tentative weights and the bottom bucket
///
/// The tentative weights are stored as a column vector 
///   - we think of this column as the handle of the ladel
///   - the entries of this column are sorted in vertex order, not according to tentative weight
///   - this vector holds weights for vertices (self.first_vertex_in) .. (self.first_vertex_out)
/// The bottom bucket (of the unsettled vertices held on THIS PE) is the cup of the ladel
/// 
/// NB: the cup of the ladle holds the bottom bucket for THIS PE.  THIS MAY NOT BE A SUBSET OF THE GLOBALLY BOTTOM BUCKET.
#[derive(Clone,Debug)]
pub struct Ladle{
    pub tentative_distances: Vec< f64 >,
    pub first_vertex_in: usize,
    pub first_vertex_out: usize,
    pub cup_contents: Vec<usize>,              // the contents of the bottom nonempty bucket
    pub cup_index: Option<usize>,                      // the index of the bottom nonempty bucket
    pub delta: f64,
}

impl Ladle {

    /// Returns the tentative distance of the vertex
    ///
    /// The ladle only stores tentative distances for vertices in a given window [a,b). If the
    /// vertex provided lies outside this window, then the function returns None.  Otherwise it
    /// returns Some(OFloat).
    fn get_tentative_distance( & self, vertex: usize ) -> Option<OFloat> {        
        if ( vertex < self.first_vertex_in ) || ( vertex >= self.first_vertex_out ) { 
            println!(
                "ERROR: attempted to get the tentative distance of vertex {:?} in a ladle that only contains tentative weights for vertices in the half-open interval [{:?},{:?})",
                vertex,
                self.first_vertex_in,
                self.first_vertex_out,
            );
            return None 
        }
        
        return  Some( 
                    OrderedFloat::<f64>(
                        self.tentative_distances[ vertex - self.first_vertex_in ]
                            .clone() 
                    )
                )
    }


    /// Returns the tentative distance of the vertex
    ///
    /// The ladle only stores tentative distances for vertices in a given window [a,b). If the
    /// vertex provided lies outside this window, then the function returns None.  Otherwise it
    /// returns Some(OFloat).
    fn set_tentative_distance( &mut self, vertex: usize, tentative_distance: OFloat )  {        
        if  ( vertex < self.first_vertex_in ) 
            || 
            ( vertex >= self.first_vertex_out ) { 
            println!(
                "ERROR: attempted to set the tentative distance of vertex {:?} in a ladle that only contains tentative weights for vertices in the half-open interval [{:?},{:?})",
                vertex,
                self.first_vertex_in,
                self.first_vertex_out,
            );
        }
        
        self.tentative_distances[ vertex - self.first_vertex_in ] = f64::from(tentative_distance)
    }    

    /// Fulfill relaxation requests
    ///
    /// This operation updates the tentative distance of each vertex in the request.
    ///
    /// It also updates the "cup at the bottom of the ladel," i.e. the bottom bucket.
    /// Concretely, for each vertex v whose tentative distance **strictly decreases** we either
    /// move v into the cup OR, if v belongs to a bucket strictly below the cup, we empty the
    /// cup and replace it with the set { v }.  **This ensures that the cup contains the bottom bucket owned by this PE.**
    fn relax( &mut self, relaxation_requests: & Vec< (usize, OFloat) > ) {
        for (m,t) in relaxation_requests.iter().cloned() {

            let vertex_to_update       = m;
            let tentative_distance_new = t;
            let tentative_distance_old = self.get_tentative_distance( m ).unwrap();

            // do nothing if we do not strictly decrease tentative distance
            // NB: this explicitly excludes consideration of any relaxation requests on settled nodes
            if tentative_distance_new >= tentative_distance_old { continue } 

            // update the tentative weight
            self.set_tentative_distance(m,t);

            // if necessary, add vertex to bottom bucket (creating a new bucket if needed)
            let bucket_index_new = bucket_index_of_float( tentative_distance_new, self.delta );
            let bucket_index_old = bucket_index_of_float( tentative_distance_old, self.delta );

            if bucket_index_new == bucket_index_old { 
                // the vertex stays in its bucket
                continue 
            } else if Some(bucket_index_new) == self.cup_index {
                // the vertex moves into the bottom bucket
                self.cup_contents.push( vertex_to_update );
            } else {
                // we create a new bottom bucket for the vertex; it contains only the vertex
                self.cup_contents.clear();
                self.cup_contents.push( vertex_to_update );
                // we also update the bottom bucket index
                self.cup_index = Some( bucket_index_new );
            }
        }
    }


    /// Calculates the bottom bucket (its index and contents), excluding all buckets with index strictly-less-than `exclude_buckets_strictly_below`
    fn rebase( &mut self, exclude_buckets_strictly_below: usize ) {

        // calculate the minimum tentative weight that we do not exclude
        let floor                   =   OrderedFloat::<f64>(
                                            self.delta * ( exclude_buckets_strictly_below as f64 )
                                        );
        let tentative_distance_min  =   self.tentative_distances
                                            .iter()
                                            .cloned()
                                            .map( |x| OrderedFloat::<f64>(x) ) // convert to OrderedFloat
                                            .filter(|x| *x >= floor)
                                            .min();

        // if all tenative weights are excluded, then we don't have to do any more work                                            
        if tentative_distance_min.is_none() {
            // this happens only if every vertex lies in a bucket of index < `exclude_buckets_strictly_below`
            self.cup_index          =   None;
            self.cup_contents.clear();
            return
        }

        // calculate the index of the bottom bucket
        let tentative_distance_min  =   tentative_distance_min.unwrap().clone();
        let cup_index               =   bucket_index_of_float( tentative_distance_min, self.delta );
        
        // calculate the contents fo the bottom bucket
        let tentative_distance_max  =   OrderedFloat::<f64>(
                                            ( cup_index + 1 ) as f64 * self.delta
                                        );
        let cup_contents: Vec<usize> 
                                    =   self.tentative_distances
                                            .iter()
                                            .cloned()
                                            .map(|x| OrderedFloat::<f64>(x) ) // convert to ordered float
                                            .enumerate()
                                            .filter(
                                                |(m,x)| 
                                                ( tentative_distance_min <= *x )
                                                &&
                                                ( *x < tentative_distance_max )
                                            )
                                            .map(|(m,x)| m + self.first_vertex_in ) // !!! NOTE THAT WE HAVE TO OFFSET OUR INDICES BECAUSE THIS LADLE ONLY CONTAINS VERTICES IN A CERTAIN INTERVAL
                                            .collect();
        
        // update self with info about the bottom bucket
        self.cup_index              =   Some(cup_index);
        self.cup_contents           =   cup_contents;
    }
}

























fn main() {

    let world                   =   lamellar::LamellarWorldBuilder::new().build();    

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let rows_per_thread_per_pe  =   cli.rows_per_thread_per_pe;
    let rows_per_pe             =   rows_per_thread_per_pe * world.num_threads();
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
                                        indices_row,
                                        indices_col,
                                        weights, 
                                    );
    let matrix                  =   matrix.to_csr::<usize>();


    // create a ladle
    let mut ladle               =   Ladle{
                                        delta:                  delta,         
                                        cup_index:              None,                                 
                                        cup_contents:           Vec::new(),     
                                        tentative_distances:    vec![ f64::INFINITY; num_rows_owned ],                                                                                                
                                        first_vertex_in:        row_owned_first_in,
                                        first_vertex_out:       row_owned_first_out,
                                    };

    if world.my_pe() == 0 {         

        // set the tentative distance of vertex 0 to 0
        ladle.tentative_distances[0] 
                                =   0.0;
        // push vertex 0 to the bottom bucket
        ladle.cup_contents.push(0); 
        // record the index of the bottom bucket as 0
        ladle.cup_index         =   Some(0); 
    }



    // wrap in LocalRwDarc's

    let ladle                       =   LocalRwDarc::new( world.team(), ladle        ).unwrap();
    let bottom_bucket_index_darc    =   LocalRwDarc::new( world.team(), Some(0)      ).unwrap();
    let bottom_bucket_is_empty_darc =   LocalRwDarc::new( world.team(), false        ).unwrap();

    // let mut ladle_refmut            =   ladle.write();    

    // initialize a holder for relaxation requests
    let mut relaxation_request_bins 
                                =   vec![ Vec::new(); world.num_pes()  ];
    
    time_to_initialize          =   Instant::now().duration_since(start_time_initializing_values); 


    // enter loop
    // -----------------

    let start_time_main_loop    =   Instant::now(); 

    let mut epoch_outer         =   0;
    
    loop {

        // thread::sleep(Duration::from_secs(1));

        epoch_outer += 1;

        //  find the (global) bottom bucket index
        //  -------------------------------------
        //
        //  NB: this means that every PE will know the index of the globally bottom bucket
        //
        //  We don't "rebase" any of the ladles. Each PE just reports what it currently
        //  records as its bottom bucket index.    

        if debug { println!("PE {:?}: bottom bucket index BEFORE global synchronization: {:?}", world.my_pe(), (*ladle.read()).cup_index.clone() ); }
        
        // We are about to take the minimum of the set { bottom bucket index for each PE }. 
        // We'll do this by taking an initial value X, and running X = min( X, bottom bucket index for PE i) for all i.  
        // We initialize X = +infinity. In this context, +infinity is represented by None 
        // (this is different from standard Rust; in standard Rust, None typically represents the bottom elelement of a poset)
        **bottom_bucket_index_darc.write() = None;

        let am  =   UpdateBottomBucketIndex {
                        bottom_bucket_index_on_sending_pe:    (*ladle.read()).cup_index.clone(),   // the index of the bottom bucket on the current PE
                        bottom_bucket_index_on_receiving_pe:  bottom_bucket_index_darc.clone(),    // the index of the bottom bucket on the remote PE that we want to update
                    };
        let _   =   world.exec_am_all( am );

        world.wait_all();          
        world.barrier();  

        let bottom_bucket_index_global: Option<usize>      =   *bottom_bucket_index_darc.read().clone();
        if debug { println!("PE {:?}: bottom bucket index AFTER global synchronization: {:?}", world.my_pe(), bottom_bucket_index_global.clone() ); }                
        
        //  if the index is None, then break -- all vertices are settled
        //  ------------------------------------------------------------

        if bottom_bucket_index_global.is_none()
            ||
           bottom_bucket_index_global > Some( number_of_buckets ) {
            if debug { println!("Number of outer epochs = {:?}", epoch_outer - 1);                      }
            if debug { println!("Outer view of row 0 = {:?}", matrix.outer_view( 0 ).unwrap() );        }
            break
        }        

        //  otherwise relax light edges emanating from the bottom bucket (in a LOOP)
        //  ------------------------------------------------------------------------
        
        loop {

            // thread::sleep(Duration::from_secs(1));

            // if this PE contains some vertices in the global bottom bucket, then  
            // (1) create relaxation requestions
            // (2) clear all vertices from the local bottom bucket (ie the cup_contents of the ladle), but DO NOT CHANGE the cup_index of the ladle
            // (3) send and execute the relaxation requests

            // create relaxation requests and place them in bins
            if (*ladle.read()).cup_index == bottom_bucket_index_global {

                for vertex_source in (*ladle.read()).cup_contents.iter().cloned() {
                    let tentative_distance_vertex_source 
                                                        =   (*ladle.read()).get_tentative_distance( vertex_source ).unwrap();
                    for ( vertex_target, weight ) in matrix.outer_view( vertex_source ).unwrap().iter() {
                        if debug { println!("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!         relaxing edge {:?}", (vertex_target,weight) );  }
                        if *weight > delta_ordered { continue } // we ignore heavy edges
                        let destination_pe              =   vertex_target / rows_per_pe; // determine which pe will receive the relaxation request
                        let relaxation_request          =   ( vertex_target, weight + tentative_distance_vertex_source  );  // define the request                
                        relaxation_request_bins[ destination_pe ].push( relaxation_request  ); // place the request in a bin containing all the requests for the specified destination pe
                    }
                    if debug { println!("PE {:?}: relaxation_request_bins edge {:?}", world.my_pe(), relaxation_request_bins );      }               
                }

                // clear all vertices from the local bottom bucket
                (*ladle.write()).cup_contents.clear();
            }   

            // mark the bottom bucket empty (we may reverse this in the next step)
            **bottom_bucket_is_empty_darc.write() = true;            
            
            if debug { println!("PE {:?} before BARRIER 1", world.my_pe() );    }

            // wait
            world.wait_all();          
            world.barrier();             

            // send the relaxation requests            
            if (*ladle.read()).cup_index == bottom_bucket_index_global {            
                for destination_pe in 0..world.num_pes() {
                    if ! relaxation_request_bins[ destination_pe ].is_empty() {
                        // pull out the set of relaxation requests and replace it with an empty vector
                        let relaxation_requests         =   mem::replace(
                                                                &mut relaxation_request_bins[ destination_pe ], 
                                                                vec![],
                                                            );
                        if debug { println!("PE {:?}: sending relaxation requests {:?}", world.my_pe(), & relaxation_requests );    }                                                   

                        let am  =   RelaxAM {
                                        relaxation_requests:    relaxation_requests,       // a collection of new vertex scores
                                        receives_new_scores:    ladle.clone(),             // the score ledger we want to update with the new scores
                                    };                                    
                        let _   =   world.exec_am_pe( destination_pe, am );
                                                                            
                    }
                }
            }

            if debug { println!("PE {:?} before BARRIER 2", world.my_pe() );        }   

            // wait
            world.wait_all();          
            world.barrier(); 
            
            // if the local bottom bucket remains NON-empty, and it belongs to the global bottom bucket, then inform all other PE's
            if  (*ladle.read()).cup_index == bottom_bucket_index_global
                &&
                ! (*ladle.read()).cup_contents.is_empty()
            {
                let am  =  MarkBottomBucketNonemptyAM {
                    bottom_is_empty_on_receiving_pe:  bottom_bucket_is_empty_darc.clone(), // the value on the remote PE that we want to update
                };
                let _   =   world.exec_am_all( am );
            }

            if debug { println!("PE {:?} before BARRIER 3", world.my_pe() );       }     

            // wait
            world.wait_all();          
            world.barrier(); 
            
            // if the global bottom bucket is globally empty,then break
            if **bottom_bucket_is_empty_darc.read() {
                break
            }
          
        }

        // relax heavy edges
        // -----------------

        // at this point we have settled all the vertices in the bottom bucket.
        // now, create relaxation requests for the heavy edges, and place the requests in bins
        for vertex_source in row_owned_first_in .. row_owned_first_out {
            let tentative_distance              =   (*ladle.read()).get_tentative_distance( vertex_source ).unwrap();
            let bucket_index                    =   Some( bucket_index_of_float( tentative_distance, delta  ) );
            
            if bucket_index != bottom_bucket_index_global { continue } // ignore vertex_source if it doesn't lie in the (global) bottom bucket

            let tentative_distance_vertex_source 
                                                =   (*ladle.read()).get_tentative_distance( vertex_source ).unwrap();
            for ( vertex_target, weight ) in matrix.outer_view( vertex_source ).unwrap().iter() {
                if *weight <= delta_ordered { continue } // we ignore light edges
                let destination_pe              =   vertex_target / rows_per_pe; // determine which pe will receive the relaxation request
                let relaxation_request          =   ( vertex_target, weight + tentative_distance_vertex_source  );  // define the request                
                relaxation_request_bins[ destination_pe ].push( relaxation_request  ); // place the request in a bin containing all the requests for the specified destination pe
            }
        }

        for destination_pe in 0..world.num_pes() {
            if ! relaxation_request_bins[ destination_pe ].is_empty() {
                // pull out the set of relaxation requests and replace it with an empty vector
                let relaxation_requests         =   mem::replace(
                                                        &mut relaxation_request_bins[ destination_pe ], 
                                                        vec![],
                                                    );

                let am  =   RelaxAM {
                                relaxation_requests:    relaxation_requests,       // a collection of new vertex scores
                                receives_new_scores:    ladle.clone(),             // the score ledger we want to update with the new scores
                            };                                    
                let _   =   world.exec_am_pe( destination_pe, am );
                                                                    
            }
        }     
        
        if debug { println!("PE {:?} before BARRIER 4", world.my_pe() );         }     

        // wait
        world.wait_all();          
        world.barrier();             


        // clear all vertices from the local bottom bucket
        // -----------------------------------------------
        (*ladle.write()).cup_contents.clear();


        // find the new local bottom bucket
        // --------------------------------

        // Update the ladle by excluding every local bucket B_i such that i ≤ (the index of the bucket we just settled).
        // This will update the index of the bottom bucket in the ladle, and update the contents of the cup.

        (*ladle.write()).rebase( 1 + bottom_bucket_index_global.unwrap() );
        
        if debug { println!("PE {:?}: bottom bucket index local after rebase: {:?}", world.my_pe(), (*ladle.read()).cup_index );    }
        if debug { println!("PE {:?}: tent = {:?}", world.my_pe(), & *ladle.read().tentative_distances );                           }

        if debug { println!("PE {:?} before BARRIER 5", world.my_pe() );                                                            }    
        // wait
        world.wait_all();          
        world.barrier();  
    }


    // finished -- report results
    // --------------------------------    


    let tentative_distances_pe_0
                                =   (*ladle.read()).tentative_distances.clone();

    
    if world.my_pe() == 0 {

        time_to_loop            =   Instant::now().duration_since(start_time_main_loop);   
        // let tentative_distances_pe_0
        //                         =   *ladle.read().clone();
        // let tentative_distances_pe_0
        //                         =   ladle_refmut.tentative_distances.clone();
        
        //                         =   Vec::with_capacity( num_rows_owned );
        // for t in *ladle.read().tentative_distances.iter().cloned() {
        //     tentative_distances_pe_0.push(t);
        // }

        println!("");                                                                                                        
        println!("Finished successfully");                                                                                                       
        println!("");                                                                                                        
        println!("Number of PE's:                     {:?}", world.num_pes() );                                                                                                          
        println!("Cores per PE:                       {:?}", world.num_threads());                                                                                                               
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
//  ACTIVE MESSAGE
//  ===========================================================================



/// Allows each node to send relaxation requests to the other PE's
#[lamellar::AmData(Debug, Clone)]
pub struct RelaxAM {
    pub relaxation_requests:    Vec< (usize, OFloat ) >,       // a collection of new vertex scores
    pub receives_new_scores:    LocalRwDarc< Ladle >,          // the score ledger we want to update with the new scores
}

#[lamellar::am]
impl LamellarAM for RelaxAM {
    async fn exec(self) {        
        let mut receives_new_scores         =   self.receives_new_scores.write(); // get a writable handle on the local ladle
        receives_new_scores.relax( & self.relaxation_requests );
    }
}


/// Allows nodes to determine if bottom bucket is nonempty
#[lamellar::AmData(Debug, Clone)]
pub struct MarkBottomBucketNonemptyAM {
    pub bottom_is_empty_on_receiving_pe:  LocalRwDarc< bool >,    // the value on the remote PE that we want to update
}

#[lamellar::am]
impl LamellarAM for MarkBottomBucketNonemptyAM {
    async fn exec(self) {        
        let mut bottom_is_empty_on_receiving_pe         =   **self.bottom_is_empty_on_receiving_pe.write(); // get a writable handle on the local ladle
        bottom_is_empty_on_receiving_pe                 =   false; // mark the bottom bucket as nonempty
    }
}


/// Allows nodes to determine the index of the new bottom bucket
#[lamellar::AmData(Debug, Clone)]
pub struct UpdateBottomBucketIndex {
    pub bottom_bucket_index_on_sending_pe:    Option<usize>,                   // the index of the bottom bucket on the current PE
    pub bottom_bucket_index_on_receiving_pe:  LocalRwDarc< Option<usize> >,    // the index of the bottom bucket on the remote PE that we want to update
}

#[lamellar::am]
impl LamellarAM for UpdateBottomBucketIndex {
    async fn exec(self) {        
        let mut bottom_bucket_index_on_receiving_pe     =   self.bottom_bucket_index_on_receiving_pe.write(); // get a writable handle on the local ladle
        // take the minimum of the two indices, which has form Some(x) if both of the indices is Some(a), and which has form None if one of the indices is None
        let mut merged_bottom                           =   self.bottom_bucket_index_on_sending_pe.min( *bottom_bucket_index_on_receiving_pe.clone() );
        // if at least one of the indices is None, then return the max of the two indices (which equals None if the other index is None, and equals the other index, otherwise)
        if merged_bottom.is_none() {
            merged_bottom                               =   self.bottom_bucket_index_on_sending_pe.max( *bottom_bucket_index_on_receiving_pe.clone() );
        }
        // println!("MEREGED BOTTOM (EXECUTING AM) == {:?}", merged_bottom );
        **bottom_bucket_index_on_receiving_pe           =   merged_bottom;

        // println!("VERIFICATION OF MEREGED BOTTOM (EXECUTING AM) == {:?}", bottom_bucket_index_on_receiving_pe );
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

    /// Random seed to initialize matrix
    #[arg(short, long, )]
    random_seed: usize,

    /// Delta parameter for the algorithm
    #[arg(short, long, )]
    delta: f64,    

    /// Turn debugging on
    #[arg(short, long, )]
    debug: bool,        
}

