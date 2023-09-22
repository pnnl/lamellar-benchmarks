// //! Matrix permutation
// //!


// //  ---------------------------------------------------------------------------

// // use lamellar::array::{ReadOnlyArray, ReadOnlyOps };
// // use lamellar::LamellarArray;
// use lamellar::array::prelude::*;
// use lamellar::active_messaging::prelude::*;
// use lamellar::darc::prelude::*;

// use sparse_matrix_am::permutation::randperm_am_darc_darts::random_permutation;
// use sparse_matrix_am::matrix_constructors::dart_uniform_rows;

// use std::mem::replace;

// //  ---------------------------------------------------------------------------


fn main() {}

//     let world                   =   lamellar::LamellarWorldBuilder::new().build();
//     let num_pes                 =   world.num_pes();
    
//     let seed                    =   0;
//     let rows_per_pe             =   1000; // number of permuted integers stored on each pe
//     let nnz_per_row             =   10;
//     let num_rows_global         =   nnz_per_row * world.num_pes();
//     let row_indices             =   ( world.my_pe() * rows_per_pe .. (world.my_pe()+1) * rows_per_pe ).collect::<Vec<usize>>();
                                  

//     // define the owned portion of the matrix
//     // ----------------------------    
//     let dummy_indices           =   (0..rows_per_pe).collect::<Vec<usize>>();
//     // returns a the locations of nonzero indices in the form of (list_of_row_indices, list_of_column_indices)
//     let mut matrix_data         =   dart_uniform_rows(
//                                         seed + world.my_pe() + 2,
//                                         num_rows_global,
//                                         nnz_per_row * rows_per_pe,
//                                         &dummy_indices,
//                                     );
//     let nnz_in_this_pe          =   matrix_data.0.len();


//     // define a vector V such that V[i] = number of nonzero entries in the first i columns
//     // -----------------------------------------------------------------------------------
    
//     // NB: this means that V[i] does *not* count the entries in column i

//     let mut column_offsets      =   vec![
//                                         vec![ 0; rows_per_pe + 1 ]; // this is the vector V
//                                         num_pes
//                                     ];

//     // calculate nnz per column
//     for j in matrix_data.1.iter().cloned() {
//         let pe                  =   j / rows_per_pe;
//         let local_column_num    =   j % rows_per_pe;
//         column_offsets[ pe ][ local_column_num + 1 ] += 1; // NB: we offset the histogram by shifting all entries to the right one place
//     }

//     // calculate a running sum across columns
//     for pe in 0 .. num_pes {
//         let mut column_offsets_local    
//                                 =   column_offsets.get_mut( pe );
//         for j in 1 .. rows_per_pe {
//             column_offsets_local[ pe ][ j ] += column_offsets_local[ pe ][ j-1 ]
//         }
//     }

//     // pre-allocate space for row indices
//     // ----------------------------------

//     let mut row_indices         =    Vec::with_capacity( num_pes );
//     for pe in 0 .. num_pes {
//         let nnz                 =   column_offsets_local[ pe ][ rows_per_pe ].clone();
//         let row_indices_local   =   vec![ 0; nnz ];
//         row_indices.push( row_indices_local );
//     }    

//     // 1) update `column_offsets` so that it becomes the column offset vector of the transpose
//     //    of the *owned* portion of the matrix --WITH THE LEADING ZERO ENTRY DELETED--, and
//     // 2) generate the (unsorted) row index vector of the transpose of the *owned* portion of
//     //    the matrix
//     // ---------------------------------------------------------------------------------------

//     let mut pe;
//     let mut col_local;
//     let mut row_local;
//     let mut row_indices_local;
//     let mut col_offset_local;
//     let mut linear_index_loca;
//     for (row,col) in matrix.0.iter().cloned().zip( matrix.1.iter().cloned() ) {
//         pe                      =   col / rows_per_pe;
//         col_local               =   col % rows_per_pe;
//         row_local               =   row + world.my_pe() * rows_per_pe;

//         row_indices_local       =   row_indices[ pe ];
//         col_offset_local        =   col_offset[ pe ];

//         linear_index_local      =   col_offset_local[ col_local ];
        
//         row_indices_local[ linear_index_local ]     
//                                 =   row_local;
        
//         col_offset_local[ col_local ] 
//                                 +=  1;
//     }

//     // send an active message containing just the column offsets
//     // wait
//     // send an active message with both column offsets and row entries

    






            
//     // initialize the permuted matrix
//     // ------------------------------    
//     let matrix_permuted         =   vec![ vec![]; rows_per_pe ];
//     let matrix_permuted         =   LocalRwDarc::new( world.team(), matrix_permuted ).unwrap();

//     // place data in bins for distribution
//     // -----------------------------------
//     let mut bins_to_send        =   vec![ vec![] ; world.num_pes() ];
    
//     // remove each row from the matrix, and place it into a bin based on its destination
//     for (row_future, row_now) in permutation_row.local_data().iter().enumerate() {
//         let pe_destination      =   row_future / rows_per_pe;
//         let receiving_row_index =   row_future % rows_per_pe; // we reduct things MODULO the number of PE's, so that `receiving_row_index` is the exact index to which the row will be sent, on the remote PE
//         let row_to_send         =   replace( &mut matrix[ *row_now ], vec![] ); // pull this row out of the matrix

//         bins_to_send[ pe_destination ].push( (receiving_row_index, row_to_send) );
//     }

//     // send the bins
//     // -------------
//     for ( pe, bin_to_send ) in bins_to_send.drain(..).enumerate() {
//         if ! bin_to_send.is_empty() {
//             let am              =   SendRows{
//                                         rows:               bin_to_send,
//                                         receiver_of_rows:   matrix_permuted.clone(),
//                                     };
//             let _ = world.exec_am_pe( pe, am );            
//         }
//     }

//     world.wait_all();
//     world.barrier();

//     // relabel the columns
//     // -------------------

//     for row in matrix_permuted.write().iter_mut() {
//         * row     =   permutation_col.block_on( permutation_col.batch_load( row.clone() ) );
//     }
    
// }


// //  ===========================================================================
// //  ACTIVE MESSAGE
// //  ===========================================================================


// /// Allows each node to send a batch of rows to other nodes
// #[lamellar::AmData(Debug, Clone)]
// pub struct SendRows {
//     pub rows:               Vec< (usize, Vec<usize>) >,               // diagonal_elements[p] is the set of (row,col) pairs added in epoch p    
//     pub receiver_of_rows:   LocalRwDarc< Vec< Vec<usize > > >,             // number of rows/columns we have deleted in total
// }

// impl SendRows {
//     // removes and returns all the rows contained in the am
//     pub fn pop_rows( &mut self ) -> Vec< (usize, Vec<usize>) > {
//         replace( &mut self.rows, vec![] )
//     }
// }

// #[lamellar::am]
// impl LamellarAM for SendRows {
//     async fn exec(self) {
//         let mut receiver_of_rows    =   self.receiver_of_rows.write();
//         for (row_index, row_data) in self.rows.iter().cloned() {
//             receiver_of_rows[ row_index ] = row_data;
//         }
//         // let rows                    =   (*self).pop_rows();
//         // for (receiving_row,row_data) in rows.drain(..) {
//         //     let _ = replace( &mut receiver_of_rows[receiving_row], row_data );
//         // }
//     }
// }
