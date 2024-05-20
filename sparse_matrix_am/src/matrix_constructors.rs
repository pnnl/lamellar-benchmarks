//! Sparse matrix constructors
//!
//! Provides efficient constructors for random matrices used in Bale benchmarks.
//!
//! NB: not all matrices we generate are Erdos-Renyi.  For very large arrays the
//! ER model turns out to be very expensive (so much so that it dwarfs the execution
//  time of the algorithms we would like to benchmark).

use sprs;
use rand;

use sprs::{CsMat,TriMat};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

use std::collections::HashSet;
use std::sync::{Arc,Mutex};
use std::thread;


//  ===========================================================================
//  RANDOM UPPER UNIT TRIANGULAR (ERDOS-RENYI)
//  ===========================================================================



/// Works by randomly generating (row,col) pairs, keeping the first `nnz` unique pairs that have been generated
///
/// Returns two vectors of row and column indices, each of length `nnz`.  Duplicate row-column pairs are discarded,
/// so all row-column pairs returned are unique.
pub fn dart_uniform_rows(
            seed: usize,
            side_length: usize,
            nnz: usize,
            row_indices: &[usize],
        )
        -> (Vec<usize>,Vec<usize>) {

    println!("!!!!!!!!!! SEED: {:?}", seed );
    println!("!!!!!!!!!! row_indices: {:?}", row_indices );

    let mut rng             =   rand::rngs::StdRng::seed_from_u64( seed as u64 ); // a different / offset random seed for each row         

    let mut generated_indices
                            =   Vec::with_capacity(nnz); 

    let mut row;
    let mut col;
    let m                   =   row_indices.len() as f64;
    let n                   =   side_length as f64;
    while generated_indices.len() < nnz {
        row                 =   row_indices[ ( rng.gen::<f64>() * m ) as usize ];
        col                 =   ( rng.gen::<f64>() * n ) as usize;
        generated_indices.push((row,col));
    }

    generated_indices.into_iter().unzip()
    
    // return (generated_indices_row, generated_indices_col)
}



/// Works by randomly generating (row,col) pairs, and keeping a pair when it falls above the diagonal
///
/// Returns two vectors of row and column indices, each of length `nnz`.  Duplicate row-column pairs are discarded,
/// so all row-column pairs returned are unique.
pub fn dart_unit_triangular_rows(
            seed: usize,
            side_length: usize,
            nnz: usize,
            row_indices: &[usize],
        )
        -> (Vec<usize>,Vec<usize>) {

    let mut rng             =   rand::rngs::StdRng::seed_from_u64( seed as u64 ); // a different / offset random seed for each row         

    let mut generated_indices       =   HashSet::with_capacity(nnz);
    for p in row_indices.iter().cloned() {
        generated_indices.insert((p,p));
    }    

    let mut row;
    let mut col;
    let m                   =   row_indices.len() as f64;
    let n                   =   side_length as f64;
    while generated_indices.len() < nnz {
        row                 =   row_indices[ ( rng.gen::<f64>() * m ) as usize ];
        col                 =   ( rng.gen::<f64>() * n ) as usize;
        if row < col {
            generated_indices.insert((row,col));
        }
    }

    generated_indices.drain().unzip()
    

    // return (generated_indices_row, generated_indices_col)
}




pub fn bernoulli_upper_unit_triangular_rows< RowIndexIter: Iterator<Item=usize> >(
            seed_matrix: usize,
            side_length: usize,
            bernoulli: f64,
            row_indices: RowIndexIter,
        )
        -> (Vec<usize>,Vec<usize>)
{
    let mut indices_matrix     =   Arc::new(Mutex::new((Vec::new(),Vec::new())));
    let mut handles = vec![];

    for index_row in row_indices {

        let matrix_indices_clone        =   Arc::clone(&indices_matrix);

        // Spawn a new thread for each row processing
        let handle = thread::spawn(move || {

            let indices_col             =   bernoulli_upper_unit_triangular_row(
                                                seed_matrix + index_row,
                                                side_length,
                                                bernoulli, 
                                                index_row,
                                            );
            let indices_row             =   vec![ index_row; indices_col.len()];

            let mut indices_matrix_lock =   matrix_indices_clone.lock().unwrap();
            indices_matrix_lock.0.extend_from_slice(&indices_row);
            indices_matrix_lock.1.extend_from_slice(&indices_col);            
        });

        handles.push(handle);
    }

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }  


    let unwrapped_mutex = Arc::try_unwrap(indices_matrix).expect("Failed to unwrap Arc");

    // Unwrap the Mutex and extract the tuple
    let (indices_row, indices_col) = unwrapped_mutex.into_inner().expect("Failed to unwrap Mutex");

    // let mut indices_matrix_guard = indices_matrix.lock().unwrap(); // Lock the Mutex and get a guard
    // let indices: (Vec<usize>,Vec<usize>)     
    //                                     =   indices_matrix_guard.0.drain(..)
    //                                             .zip( indices_matrix_guard.1.drain(..) )
    //                                             .unzip();
    //                                             // .collect::<(Vec<usize>,Vec<usize>)>(); // Drain the guard to extract the vector

    (indices_row, indices_col)
}



/// Generates a row of a random `side_length x side_length`
/// upper triangular matrix with 1's on the diagonal, where each entry above the 
/// diagonal drawn is from an indepedent Bernoulli distribution equal to 1 with probability
/// epsilon and 0 otherwise.
///
/// Output is returned as a vector of column indices
pub fn bernoulli_upper_unit_triangular_row(
        seed: usize, 
        side_length: usize, 
        epsilon: f64, 
        row: usize,
    ) -> Vec<usize> {

    let mut rng         =   rand::rngs::StdRng::seed_from_u64( seed as u64 ); // a different / offset random seed for each row
    let mut ind_col     =   Vec::new();
    
    for j in row..side_length {
        if row == j || rng.gen::<f64>() <= epsilon {
            ind_col.push(j);
        }
    }

    ind_col
}



/// Generates a row of an Erdos-Renyi random `side_length x side_length`
/// where each entry is from an indepedent Bernoulli distribution equal to 1 with probability
/// epsilon and 0 otherwise.
///
/// Output is returned as a vector of column indices
pub fn erdos_renyi_row(
        seed: usize, 
        side_length: usize, 
        epsilon: f64, 
        row: usize,
    ) -> Vec<usize> {

    let mut rng             =   rand::rngs::StdRng::seed_from_u64( seed as u64 ); // a different / offset random seed for each row

    (0..side_length)
        .filter(|_| rng.gen::<f64>() <= epsilon )
        .collect()
}


//  ===========================================================================
//  RANDOM UPPER UNIT TRIANGULAR (ERDOS-RENYI)
//  ===========================================================================

//  !!!     NOTA BENE: 
//  !!!     CODE BELOW THIS POINT IS NOT CURRENTLY USED



/// Generates a subset of rows of a random `side_length x side_length`
/// upper triangular matrix with 1's on the diagonal, where each entry above the 
/// diagonal drawn is from an indepedent Bernoulli distribution equal to 1 with probability
/// epsilon and 0 otherwise.
///
/// Output is returned as a pair of vectors (row_indices, col_indices)
pub fn bernoulli_upper_unit_triangular_row_slice_indices<I: Iterator<Item=usize> >(
        seed: usize, 
        side_length: usize, 
        epsilon: f64, 
        rows: I,
    ) -> (Vec<usize>, Vec<usize>) {

    let mut ind_row    =   Vec::new();
    let mut ind_col    =   Vec::new();

    for i in rows {

        let mut rng = rand::rngs::StdRng::seed_from_u64( (seed + i) as u64 ); // a different / offset random seed for each row
        for j in i..side_length {
            if i == j || rng.gen::<f64>() <= epsilon {
                ind_row.push(i);
                ind_col.push(j);
            }
        }
    }

    (ind_row, ind_col)
}


/// Generates rows `row_first_in .. row_first_out` of a random `side_length x side_length`
/// upper triangular matrix with 1's on the diagonal, where each entry above the 
/// diagonal drawn is from an indepedent Bernoulli distribution equal to 1 with probability
/// epsilon and 0 otherwise.
pub fn bernoulli_upper_unit_triangular_row_slice_csr(
        seed: usize, 
        side_length: usize, 
        epsilon: f64, 
        row_first_in: usize, 
        row_first_out: usize
    ) -> CsMat<usize> {

    let mut data = vec![];
    let mut indices = vec![];
    let mut indptr = vec![0];

    for i in row_first_in .. row_first_out {

        let mut rng = rand::rngs::StdRng::seed_from_u64( (seed + i) as u64 ); // a different / offset random seed for each row
        for j in i..side_length {
            if i == j || rng.gen::<f64>() <= epsilon {
                data.push(1);
                indices.push(j);
            }
        }
        indptr.push(indices.len());
    }

    CsMat::new((side_length, side_length), indptr, indices, data)
}

/// Generates rows `row_first_in .. row_first_out` of a random `side_length x side_length`
/// upper triangular matrix with 1's on the diagonal, where each entry above the 
/// diagonal drawn is from an indepedent Bernoulli distribution equal to 1 with probability
/// epsilon and 0 otherwise.
pub fn bernoulli_upper_unit_triangular_row_slice_csc(
    seed: usize, 
    side_length: usize, 
    epsilon: f64, 
    row_first_in: usize, 
    row_first_out: usize
) -> CsMat<usize> {

    let mut data = vec![];
    let mut indices = vec![];
    let mut indptr = vec![0];

    for j in 0..side_length {

        let mut rng = rand::rngs::StdRng::seed_from_u64( (seed + j) as u64 ); // a different / offset random seed for each column        
        let row_first_out_this_column     =   row_first_out.min(j+1); // exclude entries below j
        for i in row_first_in .. row_first_out_this_column {
            if i == j || rng.gen::<f64>() <= epsilon {
                data.push(1);
                indices.push(i);
            }
        }
        indptr.push(indices.len());
    }

    CsMat::new((side_length, side_length), indptr, indices, data)
}

fn main() {
    let seed = 1234;
    let side_length = 5;
    let epsilon = 0.2;
    let row_first_in = 0;
    let row_first_out = side_length - 1;

    let matrix = bernoulli_upper_unit_triangular_row_slice_csc(seed, side_length, epsilon, row_first_in, row_first_out);
    println!("{:?}", matrix);
}


/// Structural nonzero coefficients are not stored
#[derive(Debug,Clone)]
pub struct Csc{
    pub rows:       Vec<usize>,
    pub offsets:    Vec<usize>,
}

impl Csc{
    /// Returns the row indices of the nonzero entries in this column
    pub fn column( &self, column_number: usize ) -> &[usize] { 
        & self.rows[ 
            self.offsets[column_number]
            .. 
            self.offsets[column_number+1] 
        ] 
    }
}

