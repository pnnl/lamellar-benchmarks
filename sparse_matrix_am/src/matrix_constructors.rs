//! Sparse matrix constructors

use sprs;
use rand;

use sprs::{CsMat,TriMat};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;


//  ===========================================================================
//  RANDOM UPPER UNIT TRIANGULAR (ERDOS-RENYI)
//  ===========================================================================



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

