//! Active messages for the Toposort problem
//!
//! The AM's defined in this file are used in several versions of the Toposort
//! algorithm implemented in bin/


//  ---------------------------------------------------------------------------

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use sprs::CsMat;

use std::collections::HashMap;
use std::collections::HashSet;

//  ---------------------------------------------------------------------------


/// Allows each node to tell all other nodes (including itself) which matrix columns
/// to delete.
#[lamellar::AmData(Debug, Clone)]
pub struct ToposortAm {
    pub matrix:             LocalRwDarc<CsMat<u8>>, 
    pub row_sums:           LocalRwDarc<Vec<usize>>,       // sum of nonzero column indices for each row
    pub row_counts:         LocalRwDarc<Vec<usize>>,       // number of nonzero column indices for each row
    // pub diagonal_elements:  LocalRwDarc<Vec<Vec<(usize,usize)>>>, // diagonal_elements[p] is the set of (row,col) pairs added in epoch p    
    pub num_deleted_global: LocalRwDarc<usize>,             // number of rows/columns we have deleted in total
    pub columns_to_delete:  Vec<usize>,             // columns to be removed when the am executes
}

#[lamellar::am]
impl LamellarAM for ToposortAm {
    async fn exec(self) {

        // there is nothing to do if no columns are deleted
        if ! self.columns_to_delete.is_empty() {

            // let     diagonal_elements   =   self.diagonal_elements.write();
            let     matrix              =   self.matrix.read().await;
            let mut row_counts          =   self.row_counts.write().await;
            let mut row_sums            =   self.row_sums.write().await;
            let mut num_deleted_global  =   self.num_deleted_global.write().await;

            // update the global count of number of deleted rows/columns
            **num_deleted_global        +=  self.columns_to_delete.len();

            // delete the appropriate columns
            for column in & self.columns_to_delete {
                let nz_entries          =   matrix.outer_view( *column ).unwrap();
                let nz_indices          =   nz_entries.indices();
                for row in nz_indices {
                    row_counts[    *row ]  -=  1;
                    row_sums[      *row ]  -=  column;
                }
            }
        }
    }
}

/// Allows each node to send the diagonal elements it has found to PE 0.
#[lamellar::AmData(Debug, Clone)]
pub struct PoolDiagonalElementsAm {
    pub diagonal_elements_to_move:     Vec<Vec<(usize,usize)>>,                // diagonal_elements[p] is the set of (row,col) pairs added in epoch p    
    pub diagonal_elements_to_stay:     LocalRwDarc<Vec<Vec<(usize,usize)>>>,             // number of rows/columns we have deleted in total
}

#[lamellar::am]
impl LamellarAM for PoolDiagonalElementsAm {
    async fn exec(self) {
        let mut diagonal_elements_to_stay  =   self.diagonal_elements_to_stay.write().await; // get a writable handle on the local collection of diagonal elements
        for (epoch,vec) in self.diagonal_elements_to_move.iter().enumerate() {
            diagonal_elements_to_stay[epoch].extend_from_slice(vec);
        }
    }
}


