//! Bellman-Ford algorithm; see the Bale documentation for background.


//  ---------------------------------------------------------------------------

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sparse_matrix_am::matrix_constructors::erdos_renyi_row;

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

    let world                   =   lamellar::LamellarWorldBuilder::new().build();    

    // user inputs
    // -----------------    

    // command line arguments
    // -----------------    

    let cli = Cli::parse();

    let num_rows_global         =   cli.matrix_size;
    let edge_probability        =   cli.edge_probability;
    let seed_permute            =   cli.random_seed;  

    // initialize timer variables
    // --------------------------
    let mut time_to_initialize  =   Instant::now().duration_since(Instant::now());
    let mut time_to_loop        =   Instant::now().duration_since(Instant::now());


    // define parameters
    // -----------------

    let num_rows_per_pe         =   1 + (num_rows_global / world.num_pes());    
    let row_owned_first_in      =   num_rows_per_pe * world.my_pe();
    let row_owned_first_out     =   ( row_owned_first_in + num_rows_per_pe ).min( num_rows_global );
    let num_rows_owned          =   row_owned_first_out - row_owned_first_out;

    let seed_matrix             =   seed_permute+2;   

    // initialize values
    // -----------------

    let start_time_initializing_values  
                                =   Instant::now();

    // this function returns the `index_row`th row of the permuted matrix, 
    // repersented by a pair of vectors (indices_row,indices_col)
    let get_row                 =   | index_row: usize | -> (Vec<usize>, Vec<usize>, Vec<OrderedFloat<f64>>) {
        let mut indices_col     =   erdos_renyi_row(
                                        seed_matrix + index_row,
                                        num_rows_global,
                                        edge_probability,
                                        index_row,     
                                    );    
        // define a row vector (filled with value `index_row`)
        let indices_row         =   vec![ index_row; indices_col.len() ]; 
        // define a random number generator
        let mut rng             =   rand::rngs::StdRng::seed_from_u64( (seed_matrix + num_rows_global + index_row) as u64 );
        // define a vector of randomly generated weights
        let weights: Vec<_>     =   (0..indices_col.len())
                                        .map(|x| OrderedFloat(rng.gen::<f64>()) )
                                        .collect();

        (indices_row, indices_col, weights)       
    };

    // generate the portion of the matrix owned by this PE
    let mut indices_row         =   Vec::new();
    let mut indices_col         =   Vec::new();
    let mut weights             =   Vec::new();
    for index_row in row_owned_first_in .. row_owned_first_out {
        let (indices_row_new, indices_col_new, weights_new)  =   get_row( index_row );
        indices_row.extend_from_slice( & indices_row_new );
        indices_col.extend_from_slice( & indices_col_new ); 
        weights.extend_from_slice( & weights_new );                                                                           
    }
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

    for epoch in 0..num_rows_global {

        **scores_have_changed.write()               =   false; // update our flag

        // check to see if any scores can be reduced by relaxing edges
        let new_scores = {

            let mut new_scores                      =   Vec::new();
            let mut tentative_scores_temp           =   tentative_scores.write();
            
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

        if ! **scores_have_changed.read() {
            break
        }
    }


    
    if world.my_pe() == 0 {

        time_to_loop            =   Instant::now().duration_since(start_time_main_loop);            

        println!("");
        println!("Finished successfully");
        println!("");
        println!("Matrix size:                        {:?}", cli.matrix_size );        
        println!("Edge probability:                   {:?}", cli.edge_probability );
        println!("Random seed:                        {:?}", cli.random_seed );
        println!("");          
        println!("Time to initialize matrix:          {:?}", time_to_initialize );
        println!("Time to get shortest paths:         {:?}", time_to_loop );
        println!("");

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

    /// Turn debugging information on
    #[arg(short, long, )]
    random_seed: usize,
}






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
