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
use std::time::{Instant, Duration};

//  ---------------------------------------------------------------------------
//  BALE NOTES



// // We write an edge with tail v, head w and weight (cost) as {v,w,c}.
// // We will say the weight of the lightest tentative path 
// // to a vertex is the price of the vertex.  The relax routine is 
// // more involved than it is in other sssp algorithms.  In addition 
// // to (possibly) reducing the price of the head of an edge,
// // it can move the head from one bucket to another, possibly to the 
// // bucket of the tail.

// def relax(w, p, B):
//   if tent[w] > p:
//     remove.bucket(w, B(tent[w]/delta))
//     add.bucket(w, B(p/delta))
//     tent[w] = x


// program sssp:

//   set tent[v] = inf for all v

//   relax(s, 0, B)  // sets tent[s] = 0  and puts s in B[0]

//   while "there is a non-empty bucket" :
//     let B[i] "be the first (smallest i) non-empty bucket"
//     set R = NULL   // set of vertices that we will retire

//     while B[i] is not empty:
//       let v = a vertex in B[i]
//       for all light edges {v,w,c}:
//         p = tent[v] + c  // possible new price of w
//         relax (w, p, B)  // possibly adding w to B[i]
//         add v to R
//         remove v from B[i]
    
//     for all v in R:
//       for all heavy edges {v,w,c}
//         p = tent[v] + c  // possible new price of w
//         relax (w, p, B)  // won't add w to B[i]




// --------------------------------------------------------------------        











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


    // // this function returns the `index_row`th row of the permuted matrix, 
    // // repersented by a pair of vectors (indices_row,indices_col)
    // let get_row                 =   | index_row: usize | -> (Vec<usize>, Vec<usize>, Vec<OrderedFloat<f64>>) {
    //     let mut indices_col     =   erdos_renyi_row(
    //                                     seed_matrix + index_row,
    //                                     num_rows_global,
    //                                     edge_probability,
    //                                     index_row,     
    //                                 );    
    //     // define a row vector (filled with value `index_row`)
    //     let indices_row         =   vec![ index_row; indices_col.len() ]; 

    // define a random number generator
    let mut rng             =   rand::rngs::StdRng::seed_from_u64( (seed_matrix +1) as u64 );
    // define a vector of randomly generated weights
    let weights: Vec<_>     =   (0..indices_col.len())
                                    .map(|x| OrderedFloat(rng.gen::<f64>()) )
                                    .collect();

    //     (indices_row, indices_col, weights)       
    // };

    // // generate the portion of the matrix owned by this PE
    // let mut indices_row         =   Vec::new();
    // let mut indices_col         =   Vec::new();
    // let mut weights             =   Vec::new();
    // for index_row in row_owned_first_in .. row_owned_first_out {
    //     let (indices_row_new, indices_col_new, weights_new)  =   get_row( index_row );
    //     indices_row.extend_from_slice( & indices_row_new );
    //     indices_col.extend_from_slice( & indices_col_new ); 
    //     weights.extend_from_slice( & weights_new );                                                                           
    // }
    let num_entries             =   indices_row.len();
    let matrix                  =   TriMat::from_triplets(
                                        (num_rows_global,num_rows_global),
                                        indices_row,
                                        indices_col,
                                        weights, 
                                    );
    let matrix                  =   matrix.to_csr::<usize>();

    // the number and sum-of-column-indices of the nonzero entries in each row
    let mut tentative_scores    =   vec![ OrderedFloat(f64::INFINITY); num_rows_global ];
    tentative_scores[0]         =   OrderedFloat(0.0); // the base node has weight 0


    // wrap in LocalRwDarc's
    let tentative_scores        =   LocalRwDarc::new( world.team(), tentative_scores        ).unwrap();
    let scores_have_changed     =   LocalRwDarc::new( world.team(), false                   ).unwrap();
    
    time_to_initialize          =   Instant::now().duration_since(start_time_initializing_values);


    // enter loop
    // -----------------

    let start_time_main_loop    =   Instant::now();   


    // TO-DO LIST
    //  
    //  - AM FOR LONG VS. SHORT EDGES
    //  - STRUCT FOR BINS
    //  - 
    


    


    // #[lamellar::am]
    // impl LamellarAM for SendLong {
    //     async fn exec(self) {
    //         let mut destination_histo    =   self.destination_histo.write();
    //         for ( local_column_number, nnz ) in self.source_histo.iter().cloned() {
    //             destination_histo[ local_column_number ] +=     nnz;
    //         }
    //     }
    // }



    // /// Allows each node to transmit its row indices to a destination node
    // #[lamellar::AmData(Debug, Clone)]
    // pub struct SendNodes {
    //     pub source_offset:             Vec< usize >,                   
    //     pub source_row_indices:         Vec< usize >,                   // source_row_indices[ source_offset[i] .. source_offset[i+1] ] = the row indices for column i owned by the sending PE
    //     pub destination_offset_walker:  LocalRwDarc< Vec< usize > >,    
    //     pub destination_row_indices:    LocalRwDarc< Vec< usize > >,    
    // }

    // #[lamellar::am]
    // impl LamellarAM for SendNodes {
    //     async fn exec(self) {
    //         let mut destination_offset_walker   =   self.destination_offset_walker.write();
    //         let mut destination_row_indices     =   self.destination_row_indices.write();
    //         let source_offset                   =   & self.source_offset;
    //         let source_row_indices              =   & self.source_row_indices;                
            
    //         // for each column, add row indices from the source PE to the destination PE
    //         for col in 0 .. source_offset.len()-1 {

    //             let source_col_nnz              =   source_offset[ col + 1] - source_offset[ col ];
    //             let linear_index_start_source   =   source_offset[ col ].clone();
    //             let linear_index_start_destination
    //                                             =   destination_offset_walker[ col ].clone();
    //             // add each row index in the current column of the source PE to the destination PE
    //             for i in 0 .. source_col_nnz {
    //                 let read_from               =   linear_index_start_source + i;
    //                 let write_to                =   linear_index_start_destination + i;
    //                 destination_row_indices[ write_to ]
    //                                             =   source_row_indices[ read_from ];
    //             }

    //             // update the column offset vector of the destination PE to reflect the new elements
    //             *(**destination_offset_walker).get_mut( col ).unwrap()
    //                                             +=  source_col_nnz; // update the 
    //         }
    //     }
    // }




    // /// Allows each node to transmit its row indices to a destination node
    // #[lamellar::AmData(Debug, Clone)]
    // pub struct SendShort {
    //     pub source_offset:             Vec< usize >,                   
    //     pub source_row_indices:         Vec< usize >,                   // source_row_indices[ source_offset[i] .. source_offset[i+1] ] = the row indices for column i owned by the sending PE
    //     pub destination_offset_walker:  LocalRwDarc< Vec< usize > >,    
    //     pub destination_row_indices:    LocalRwDarc< Vec< usize > >,    
    // }

    // #[lamellar::am]
    // impl LamellarAM for SendShort {
    //     async fn exec(self) {
    //         let mut destination_offset_walker   =   self.destination_offset_walker.write();
    //         let mut destination_row_indices     =   self.destination_row_indices.write();
    //         let source_offset                   =   & self.source_offset;
    //         let source_row_indices              =   & self.source_row_indices;                
            
    //         // for each column, add row indices from the source PE to the destination PE
    //         for col in 0 .. source_offset.len()-1 {

    //             let source_col_nnz              =   source_offset[ col + 1] - source_offset[ col ];
    //             let linear_index_start_source   =   source_offset[ col ].clone();
    //             let linear_index_start_destination
    //                                             =   destination_offset_walker[ col ].clone();
    //             // add each row index in the current column of the source PE to the destination PE
    //             for i in 0 .. source_col_nnz {
    //                 let read_from               =   linear_index_start_source + i;
    //                 let write_to                =   linear_index_start_destination + i;
    //                 destination_row_indices[ write_to ]
    //                                             =   source_row_indices[ read_from ];
    //             }

    //             // update the column offset vector of the destination PE to reflect the new elements
    //             *(**destination_offset_walker).get_mut( col ).unwrap()
    //                                             +=  source_col_nnz; // update the 
    //         }
    //     }
    // }    



    
    // while let Some(State { node, distance }) = heap.pop() {
    //     if let Some(&current_distance) = distances.get(&node) {
    //         // Skip nodes that have already been processed with a shorter distance
    //         if distance > current_distance {
    //             continue;
    //         }
    //     }

    //     for edge in graph.edges(node) {
    //         let next_node = edge.target();
    //         let weight = *graph.edge_weight(edge.id()).unwrap();
    //         let new_distance = distance + weight;

    //         if let Some(&current_distance) = distances.get(&next_node) {
    //             // Relaxation step: Update the distance if a shorter path is found
    //             if new_distance < current_distance {
    //                 distances.insert(next_node, new_distance);
    //                 heap.push(State {
    //                     node: next_node,
    //                     distance: new_distance,
    //                 });
    //             }
    //         } else {
    //             // First visit to the node
    //             distances.insert(next_node, new_distance);
    //             heap.push(State {
    //                 node: next_node,
    //                 distance: new_distance,
    //             });
    //         }
    //     }
    // }    
    
    
    
    
    
    
    
    // for epoch in 0..num_rows_global {

    //     **scores_have_changed.write()               =   false; // update our flag

    //     // check to see if any scores can be reduced by relaxing edges
    //     let new_scores = {

    //         let mut new_scores                      =   Vec::new();
    //         let mut tentative_scores_temp           =   tentative_scores.write();
            
    //         for row in row_owned_first_in .. row_owned_first_out {

    //             let mut tentative_score             =   tentative_scores_temp[ row ].clone();

    //             // relax each edge in this row
    //             for ( col, weight ) in matrix.outer_view( row ).unwrap().iter() {
    //                 let candidate_value             =   tentative_scores_temp[ col ] + weight;
    //                 if candidate_value < tentative_score {
    //                     tentative_score             =   candidate_value;
    //                 }
    //             }
    //             if tentative_score < tentative_scores_temp[ row ] {
    //                 new_scores.push( (row, tentative_score.into_inner()) ); // push the (vertex,value) pair to a list that will broadcast to all the nodes
    //                 tentative_scores_temp[ row ]    =   tentative_score; // update the local score
    //             }
    //         }

    //         world.barrier();
        
    //         new_scores
    //     };

    //     //  Step 2: if necessary, broadcast the updated vertex scores
    //     if ! new_scores.is_empty() {
    //         let am  =   UpdateScoresAm{
    //                         new_scores:             new_scores,
    //                         receives_new_scores:    tentative_scores.clone(),
    //                         scores_have_changed:    scores_have_changed.clone()
    //                     };
    //         let _   =   world.exec_am_all( am );
    //     }

    //     world.wait_all();          
    //     world.barrier();             

    //     if ! **scores_have_changed.read() {
    //         break
    //     }
    // }


    
    if world.my_pe() == 0 {

        time_to_loop            =   Instant::now().duration_since(start_time_main_loop);            

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
        let mut receives_new_scores         =   self.receives_new_scores.write(); // get a writable handle on the local collection of diagonal elements
        let mut scores_have_changed         =   self.scores_have_changed.write();
        **scores_have_changed               =   true; // mark that at least one score has changed
        for ( vertex, score ) in self.new_scores.iter() {
            receives_new_scores[ *vertex ]  =   OrderedFloat( * score );
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

