//! Permutation of vec-of-vec sparse matrices, via serial methods in the standard Rust library (no Lamellar dependencies).








/// Apply row and column permutations to a sparse matrix (without explicit structural nonzero values)
/// 
/// The entries in each row of the permuted matrix are **not** sorted.
/// 
/// # Arguments
/// 
/// - `vecvec` - sparse matrix represented as a vector of row vectors
/// - `rperminv[i] = j` means that row i of the original matrix becomes row j of the permuted matrix
/// - `cperminv[i] = j` means that col i of the original matrix becomes col j of the permuted matrix
pub fn permute_vec_of_vec( 
            vecvec:   & Vec<Vec<usize>>, 
            rperminv: & Vec<usize>, 
            cperminv: & Vec<usize> 
        ) -> Vec<Vec<usize>>
    {

    let numrows = vecvec.len();

    // get the invere of rperminv
    let mut rperm: Vec<usize> = (0..numrows).collect();
    for (indexold, indexnew) in rperminv.iter().cloned().enumerate() {
        rperm[ indexnew ] = indexold;
    }

    // initialized the permuted matrix
    let mut permuted = Vec::with_capacity(numrows);

    // for each entry of the new (outer) vec
    for indexnew in 0 .. numrows {
        let new_row: Vec<_> = 
            vecvec[ rperm[indexnew] ] // read the proper row of the original matrix
                .iter()
                .map(|x| cperminv[*x] ) // update the column indices
                .collect(); // collect into a new vector
        permuted.push( new_row ); // push the new row to the permuted matrix
    }

    // we don't sort each row (bale doesn't mention it)
    // for indexnew in 0 .. numrows {
    //     permuted[indexnew].sort();
    // }    

    return permuted
}