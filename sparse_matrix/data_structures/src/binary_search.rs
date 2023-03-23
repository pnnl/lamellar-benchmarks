use std::cmp::Ordering;

/// Find a value `p` in `{ min, .., max }` that meets a given criterion, or return `None`.
/// 
/// The returned value is **not** gauranteed to be the first one that satisfies the criterion.
/// 
/// We implicitly assume that there exists an ambient sequence of form
///  
/// `v = [ false, false, .., true, true, ..., false, false, ..]`
/// 
/// where at least one  `true` value exists, and all `true` values are contiguous.
/// We assumte that `search_direction(p)` is equal to `Ordering::Equal` if `v[p]==true`,
/// is equal to `Ordering::Less` if the first true value occurs below `p`, and 
/// is equal to `Ordering::Greater` if the first true value occurs above `p`
/// 
/// 
/// # Examples
/// ```
/// use solar::utilities::indexing_and_bijection::find_sorted_binary_oracle;
/// 
/// let v = vec![ (3,1.0), (5,1.0) ];
/// 
/// assert_eq!( find_sorted_binary_oracle(0,1, |p| 1.cmp( & v[p as usize].0 ) ), None    ); // search for an entry with index 1
/// assert_eq!( find_sorted_binary_oracle(0,1, |p| 3.cmp( & v[p as usize].0 ) ), Some(0) ); // search for an entry with index 3
/// assert_eq!( find_sorted_binary_oracle(0,1, |p| 5.cmp( & v[p as usize].0 ) ), Some(1) ); // search for an entry with index 5
/// assert_eq!( find_sorted_binary_oracle(0,1, |p| 7.cmp( & v[p as usize].0 ) ), None    ); // search for an entry with index 7
/// ```
/// 
/// # Notes
/// This code is unit-tested on all 0-1 sparse vectors of length < 8; see source code for details.
pub fn find_sorted_binary_oracle<F: Fn(isize) -> Ordering >( mut min: isize, mut max:isize, search_direction: F ) -> Option< isize > {  
    let mut mid;
    if max < min { return None }    
    while min <= max {
        mid = (min + max)/2;
        match search_direction(mid) {
            Ordering::Equal     =>  { return Some(mid) },
            Ordering::Less      =>  { max = mid - 1; },
            Ordering::Greater   =>  { min = mid + 1; },
        }
    }
    return None
}


/// Find the storage location of an entry with index `n`in a *strictly sorted* sparse vector.
/// 
/// Input `sparsevec` is a Rust vector of form `[ (i0,v0), (i1,v1), .. ]` where 
/// `i0 < i1 < ..` are the indices of the structural nonzero coefficients `v0, v1, ..`.
/// 
/// The function returns `Some( &(in, vn) )` if `sparsevec` contains an entry with index `in`.
/// Otherwise it returns `None`.
/// 
/// # Examples
/// ```
/// use solar::utilities::indexing_and_bijection::find_sorted_binary;
/// 
/// let sparsevec = vec![ (3,1.0), (5,1.0) ];
/// 
/// assert_eq!( find_sorted_binary(&sparsevec, 1), None      );
/// assert_eq!( find_sorted_binary(&sparsevec, 3), Some( 0 ) );
/// assert_eq!( find_sorted_binary(&sparsevec, 5), Some( 1 ) );
/// assert_eq!( find_sorted_binary(&sparsevec, 7), None      );
/// ```
/// 
/// # Notes
/// This code is unit-tested on all 0-1 sparse vectors of length < 8; see source code for details.
pub fn find_sorted_binary<T>( sparsevec: &Vec<(usize,T)>, n: usize ) -> Option< usize > {  
    let search_direction = |p: isize| n.cmp( & sparsevec[p as usize].0 );
    let a = 0;
    let b = (sparsevec.len() as isize) -1 ;
    return  find_sorted_binary_oracle(a, b, search_direction).map(|x| x as usize)
}

/// Given an increasing sequence `left_limits` and an integer `pigeon`, find
/// an index `p` such that `left_limites[p] <= pigoen < left_limits[p+1]`.
/// 
/// # Examples
/// ```
/// use solar::utilities::indexing_and_bijection::find_window;
/// 
/// let left_limits = vec![0,2,2,3];
/// 
/// let u: Vec< Option<usize> > = (0..5).map( |x| find_window( &left_limits, x ) ).collect();
/// let v = vec![Some(0),Some(0),Some(2),None,None];
/// 
/// assert_eq!( u, v );
/// ```
pub fn find_window( left_limits: &Vec<usize>, pigeon: usize ) -> Option< usize > {
    let search_direction = |p: isize| -> Ordering {
            let min = left_limits[p as usize];
            let max = left_limits[p as usize +1 ];
            if max <= pigeon { return Ordering::Greater }
            if min >  pigeon { return Ordering::Less }
            Ordering::Equal
        };
    let a = 0;
    let b = ( left_limits.len() as isize ) - 2; // the value couldn't be greater than 2, as we need to index "up" by one to get the upper limit
    return  find_sorted_binary_oracle(a, b, search_direction).map(|x| x as usize)
}