//! Distributed permutation of sparse matrices, with Lamellar.
//! 
//! See the unit tests at the bottom of `lamellar_spmat_distributed.rs` for some examples.  These can be used as a starting point for benchmarks.

use lamellar::{LamellarWorld, IndexedDistributedIterator, LamellarArray, LamellarArrayIterators, SubArray, Dist, IndexedLocalIterator, LocalIterator, AccessOps, ActiveMessaging};
use lamellar::array::{UnsafeArray, Distribution, DistributedIterator, ReadOnlyArray, ReadOnlyOps };

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{Ordering, AtomicBool};


//  ----------------------------------------------------------------
//  SPARSE MATRIX STRUCT      (INCLUDES METHOD FOR PERMUTATION)
//  ----------------------------------------------------------------


/// CSR data structure for a sparse matrix
/// 
/// The matrix need not contain explicit values for the structural nonzero coefficients.
#[derive(Debug, Clone)]
pub struct SparseMat {
    /// The total number of rows in the matrix
    pub numrows: usize, 
    /// The column indices of the structural nonzeros
    pub numcols: usize, 
    /// The number of structural nonzeros
    pub nnz: usize,     
    /// The row offsets into the arrays nonzeros and values, size is nrows+1,
    /// rowptr[nrows+1] is nnz
    pub rowptr: ReadOnlyArray<usize>,
    /// The nonzero column indicies
    pub nzind: ReadOnlyArray<usize>,
    /// The values, if present
    pub nzval: Option<ReadOnlyArray<f64>>,
}

impl SparseMat {
    /// a new sparse matrix without values
    pub fn new(numrows: usize, numcols: usize, nnz: usize, world: LamellarWorld) -> SparseMat {
        let rowptr  = UnsafeArray::<usize>::new(world.team(), numcols+1 , Distribution::Cyclic).into_read_only();
        let nzind = UnsafeArray::<usize>::new(world.team(), nnz , Distribution::Cyclic).into_read_only();
        SparseMat {
            numrows,
            numcols,
            nnz,
            rowptr,
            nzind,
            nzval: None,
        }
    }

    /// a new sparse matrix with values
    pub fn new_with_values(numrows: usize, numcols: usize, nnz: usize, world: LamellarWorld) -> SparseMat {
        let rowptr  = UnsafeArray::<usize>::new(world.team(), numcols+1 , Distribution::Cyclic).into_read_only();
        let nzind = UnsafeArray::<usize>::new(world.team(), nnz , Distribution::Cyclic).into_read_only();

        let value_inner             =   UnsafeArray::<f64>::new(world.team(), nnz , Distribution::Cyclic).into_read_only();
        let nzval: Option<ReadOnlyArray<f64>> =   Some(value_inner);
        SparseMat {
            numrows,
            numcols,
            nnz,
            rowptr,
            nzind,
            nzval,
        }
    }

    /// inspector fn for numrows
    pub fn numrows(&self) -> usize {
        self.numrows
    }

    /// inspector fn for numcols
    pub fn numcols(&self) -> usize {
        self.numcols
    }

    /// inspector fn for nnz
    pub fn nnz(&self) -> usize {
        self.nnz
    }


    /// Apply row and column permutations to a sparse matrix
    /// 
    /// The entries in each row of the permuted matrix are **not** sorted.
    /// (It seems that the original Bale serial implementation of matrix permutation
    /// does not always sort, either -- this is supported by some of the unit tests
    /// in the `lamellar_spmat_distributed.rs` file).
    /// 
    /// **NB** The suffix "_fax" indicates that this is as close as we can come to a
    /// facsimile of the serial Rust code for matrix permutatino provided in Bale.
    /// Essentially, we run a serial for-loop over the rows of the permuted matrix,
    /// in order, using distributed arrays only to accelerate the transfer of each
    /// row.
    /// 
    /// # Arguments
    /// 
    /// - `rperminv[i] = j` means that row i of the original matrix becomes row j of the permuted matrix
    /// - `cperminv[i] = j` means that col i of the original matrix becomes col j of the permuted matrix
    /// 
    /// # Outputs (in order)
    /// 
    /// - the permuted matrix
    /// - a Vec<usize> representing the row permutation (inverse to `rperminv`)
    /// - a Vec<(f64, usize, usize)> called "decorated ranges," used for debugging
    pub fn permute_fax(&self, rperminv: &ReadOnlyArray<usize>, cperminv: &ReadOnlyArray<usize>, world: LamellarWorld ) 
            -> (SparseMat, Vec<usize>, Vec<(f64, usize,usize)> ) {

        
        // we'll handle values in future versions of this code
        // -----------------------------------------------------------------------------------------------------------------
        if let Some(_) = &self.nzval {
            todo!()
        }


        // Preallocate the new sparse matrix + a distributed array representing the inverse of the row permutation
        // -----------------------------------------------------------------------------------------------------------------        
        let numrows                         =   self.numrows();
        let numcols                         =   self.numcols();
        let nnz                                 =   self.nnz();
        let nzval                               =   None;
        let othr_rowptr  = UnsafeArray::<usize>::new(world.team(), numcols+1 , Distribution::Cyclic).into_atomic();        
        let othr_nzind = UnsafeArray::<usize>::new(world.team(), nnz , Distribution::Cyclic).into_atomic();
        let rperm   =   UnsafeArray::<usize>::new(world.team(), numrows , Distribution::Cyclic).into_atomic();        

        // Invert the row permutation
        // -----------------------------------------------------------------------------------------------------------------        
        // This helps by allowing us to run a (serial) for-loop in ascending order of the new rows    
        
        // The following loop needs to modify the entries in rperm, but the array we modify
        // will be comsumed by the closure.  So we clone rperm, and use the clone in the loop.
        // The clone will be consumed but the entries of rperm will be updated.
        let rperm_clone = rperm.clone(); 
        rperminv.dist_iter()
            .enumerate()
            .for_each( 
                        move |(indold, indnew)|
                        { rperm_clone.store(*indnew, indold ); }
            );
        // wait for all updates to finish            
        world.wait_all(); 
        // all updates to rperm have finished, so we can make it read-only        
        let rperm = rperm.into_read_only(); 



        // Populate othr_rowptr and othr_nzind
        // -----------------------------------------------------------------------------------------------------------------            
        //  NB  WE DO NOT UPDATE COLUMN INDICES AT THIS POINT; WILL DO THAT IN A SINGLE BATCH, LATER

        // Define some dummy variables
        let mut nnzwritten = 0usize; // we update this number once for each iteration of the for-loop; 
                                            // it equals the number of entries we have written to the permuted matrix after
                                            // the kth row is written
        let mut decorated_ranges = Vec::new();    // only used for debugging
                                                                            // tracks where we read entries in the old
                                                                            // matrix's nzind
        let self_rowptr_clone = self.rowptr.clone();  // a clone that can be consumed
        let self_nzind_clone = self.nzind.clone();    // a clone that can be consumed

        // Write each row of the permuted matrix in a for-loop
        for rindexnew in 0 .. numrows
        {
            let rindexold = rperm.block_on(rperm.load( rindexnew ));
            {
                // define the section of self.nzind that we will copy into the permuted matrix
                let read_range_start = self_rowptr_clone.block_on(self_rowptr_clone.load(rindexold )); //.await;
                let read_range_end = self_rowptr_clone.block_on(self_rowptr_clone.load(rindexold +1 )); //.await;
                let read_range  =   read_range_start .. read_range_end;
                let subarrayold     =   self_nzind_clone.sub_array(read_range);

                // define a clone that can be consumed
                let othr_nzind_clone = othr_nzind.clone();  // NB: we get an error if we move this                                                                                 
                                                                                //     outside the for-loop                

                // copy the subarray into the permuted matrix
                subarrayold
                    .dist_iter()
                    .enumerate()
                    .for_each(
                        move
                        | ( destination_pointer, cindex ) |
                        {
                            othr_nzind_clone.store( 
                                destination_pointer + nnzwritten, 
                                *cindex );
                        }
                    );
                world.wait_all();
                
                // update the variable that tracks the number of structural nonzeros we've
                // written to the last k rows
                nnzwritten += read_range_end - read_range_start;    

                // update a variable used for debugging
                decorated_ranges.push( (rindexold as f64, read_range_start, read_range_end) );                
            }

            // update the row offset pattern of the new array
            othr_rowptr.block_on( othr_rowptr.store( rindexnew+1, nnzwritten ) );            
        }

        // we're done updating rowptr, so conver to read-only
        let rowptr = othr_rowptr.into_read_only();

        // Update the nonzero indices
        // -----------------------------------------------------------------------------------------------------------------            

        let nzind = 
                    vec_to_unsafe_array( 
                            & world.block_on( cperminv.batch_load( othr_nzind.local_data() ) ), 
                            world 
                        )
                        .into_read_only();
    
        // Return 
        // -----------------------------------------------------------------------------------------------------------------            
        
        println!("{:?}", "FINISHED PERMUTE FAX");
        return  (   
                    SparseMat { numrows, numcols, nnz, rowptr, nzind, nzval }, 
                    read_only_array_to_vec(&rperm ),
                    decorated_ranges
                )
                
    }    

    /// Construct a SparseMat from a vector of vectors.
    pub fn from_vec_of_rowvec( vecvec: Vec< Vec< usize > >, numrows: usize, numcols: usize, world: LamellarWorld ) -> Self {
        if numrows > vecvec.len() { panic!( "numrows must be >= vecvec.len()" ) }

        // construct nzind (vec style) AND nnz
        let nzind_vec: Vec<usize> = vecvec.iter().flatten().cloned().collect();
        let nnz = nzind_vec.len();
        
        // construct rowptr (vec style)
        let mut rowptr_vec = Vec::with_capacity( numrows+1 );
        let mut running_sum = 0;
        rowptr_vec.push( running_sum );
        for p in 0 .. numrows {
            if p < vecvec.len() { running_sum += vecvec[p].len() }
            rowptr_vec.push(running_sum);
        }

        // format as a sparse matrix
        let rowptr = vec_to_unsafe_array( & rowptr_vec, world.clone() ).into_read_only();
        let nzind  = vec_to_unsafe_array( & nzind_vec,  world ).into_read_only();
        let nzval = None;

        return SparseMat{ numrows, numcols, nnz, nzind, nzval, rowptr }

    }
        
    /// Generate a Vector-of-Vectors representation of SparseMat.
    pub fn to_vec_of_rowvec( &self ) -> Vec< Vec< usize > > {
        
        // println!("nzind: {:?}", self.nzind.local_data().to_vec() );    
        // println!("rowptr: {:?}", self.rowptr.local_data().to_vec() );            

        // initialize
        let mut vecvec = Vec::with_capacity( self.numrows() );
        let mut newrow; 
        let mut ind_alpha; 
        let mut ind_omega;

        // populate vecvec
        for p in 0 .. self.numrows {
            ind_alpha = self.rowptr.block_on( self.rowptr.load(p) );
            ind_omega = self.rowptr.block_on( self.rowptr.load(p+1) );

            // println!("{:?}", "add new vec: start");
            // println!("alpha, omega, nnz = {:?}, {:?}, {:?}", ind_alpha, ind_omega, self.nzind.len() );        
            let a = self.nzind.sub_array(ind_alpha .. ind_omega);
            // let x = a.local_data(); raises an error when a is emtpy
            newrow = read_only_array_to_vec( &a );
            vecvec.push( newrow );
            // println!("{:?}", "add new vec: end");            
        }
        
        return vecvec

    }


    // PRIOR DRAFT OF PERUMTATION CODE (PARTIALLY COPLETED; WILL RETURN AND REVISE WHEN TIME IS AVAILABLE)
    // -----------------------------------------------------------------------------------------------------------------            
        
    // /// apply row and column permutations to a sparse matrix
    // /// # Arguments
    // /// * rperminv pointer to the global array holding the inverse of the row permutation
    // /// *cperminv pointer to the global array holding the inverse of the column permutation
    // ///     rperminv[i] = j means that row i of A goes to row j in matrix Ap
    // ///     cperminv[i] = j means that col i of A goes to col j in matrix Ap
    // pub fn permute(&self, rperminv: &Perm, cperminv: &Perm) -> SparseMat {
        
    //     let world           =       lamellar::LamellarWorldBuilder::new().build();
    //     // let mut ap = SparseMat::new(self.numrows, self.numcols, 0, world);
    //     let rperm = rperminv.inverse();

    //     if let Some(_) = &self.nzval {
    //         todo!()
    //     }

    //     let numrows     =   self.numrows;

    //     let rowptr  = UnsafeArray::<usize>::new(world.team(), self.numcols+1 , Distribution::Cyclic).into_atomic();        

    //     // update the new rowptr array
    //     // for each i in  0..numrows
    //     //   add nnz(row i) to the rowptr for rows perm[i]+2 .. end
    //     // note the +2 -- this is an old trick; the rowptr array will be incorrect,
    //     // but when we add the nz values we'll adjust it to where it needs to be
    //     self.rowptr
    //         .dist_iter()
    //         .enumerate()
    //         .for_each(
    //             |( rindex_old, rowptr_old )|
    //                 {
    //                     let rindex_new          =   rperminv.entry( rindex_old );
    //                     let rowptr_old_incr     =   self.rowptr.block_on(self.rowptr.load(rindex_old+1 ))
    //                                                         - rowptr_old;
    //                     for rindex_dummy in rindex_new+2 .. numrows+1 { 
    //                         rowptr.add(rindex_dummy, rowptr_old_incr);
    //                     }
    //                 }
    //         );

    //     for (rowindex_old, rowptr_old) in self.rowptr.dist_iter().enumerate() {

    //     }

    //     // fill in permuted rows with permuted columns
    //     for i in 0..ap.numrows {
    //         let row = rperm.perm[i];
    //         for nz in &self.nzind[self.rowptr[row]..self.rowptr[row + 1]] {
    //             ap.nzind.push(cperminv.perm[*nz]);
    //         }
    //         ap.rowptr[i + 1] = ap.nzind.len();
    //     }
    //     ap.nnz = ap.nzind.len();
    //     ap
    // }

}


//  ----------------------------------------------------------------
//  CONVERSION OF DATA STRUCTURES
//  ----------------------------------------------------------------


/// Convert Vec to UnsafeArray.
pub fn vec_to_unsafe_array<T:Dist + Clone>( vec: &Vec<T>, world: LamellarWorld ) -> UnsafeArray<T> {
    let array = UnsafeArray::<T>::new(world.team(), vec.len() , Distribution::Cyclic); 
    let vec_clone = vec.clone(); // not ideal to copy the data, i just haven't had time to figure out a fix
    unsafe {
        array
            .dist_iter_mut()
            .enumerate()
            .for_each(
                    move |x|
                    { *x.1 = vec_clone[ x.0 ].clone(); }
                );        
    }        
    world.wait_all();
    return array
}

/// Convert ReadOnlyArray to Vec.
pub fn read_only_array_to_vec<T:Dist+Clone+std::fmt::Debug>( array: &ReadOnlyArray<T> ) -> Vec<T> {
    
    if array.len() == 0 { return Vec::with_capacity(0) }
    else {
        // println!("num_entries = {:?}", array.len() );
        return array.local_data().to_vec(); 
    }
}


/// Returns `true` iff the arrays have equal length and are element-wise equal.
pub fn equal_read_only_arrays<T : PartialEq + Dist >( arr0: &ReadOnlyArray<T>, arr1: &ReadOnlyArray<T>) -> bool {
    
    // return false if the arrays have unequal length
    if arr0.len() != arr1.len() { return false }

    // return false if any entry doesn't match
    let is_equal = Arc::new(Mutex::new(true));
    let is_equal_clone = is_equal.clone();
    arr0
        .local_iter()
        .zip( arr1.local_iter() )
        .for_each( 
            move |x| if  x.0 != x.1  { 
                let mut data = is_equal_clone.lock().unwrap();
                * data = false;
            }
        );      
    arr0.wait_all();
    arr1.wait_all();

    // otherwise return true
    return *is_equal.lock().unwrap();
}


//  ----------------------------------------------------------------
//  FUNCTIONS TO EVALUATE MATRIX EQUALITY
//  ----------------------------------------------------------------


/// Returns `true` iff the arrays have equal length and are element-wise equal.
pub fn equal_read_only_arrays_atomic<T : PartialEq + Dist >( arr0: &ReadOnlyArray<T>, arr1: &ReadOnlyArray<T>) -> bool {
    
    // return false if the arrays have unequal length
    if arr0.len() != arr1.len() { return false }

    // return false if any entry doesn't match
    let is_equal = Arc::new(AtomicBool::new(true));
    let is_equal_clone = is_equal.clone();
    arr0
        .local_iter()
        .zip( arr1.local_iter() )
        .for_each( 
            move |x| if  x.0 != x.1  { 
                is_equal_clone.swap(false, Ordering::Relaxed);
            }
        );     
    arr0.wait_all(); 
    arr1.wait_all();

    // otherwise return true
    return is_equal.load(Ordering::SeqCst);
}


/// Returns true iff each attribute of the two matrices 
/// (`numcols`, `numrows`, `rowptr`, `nzind`, `nzval`) is the same.
pub fn equal_sparse_matrices( m0: &SparseMat, m1: &SparseMat) -> bool {

    let mut tests: HashMap< &str, bool > = HashMap::new();
    tests.insert( "numrows", m0.numrows == m1.numrows );    
    tests.insert( "numcols", m0.numcols == m1.numcols );
    tests.insert( "rowptr ", equal_read_only_arrays_atomic( & m0.rowptr, & m1.rowptr ) );
    tests.insert( "nzind  ", equal_read_only_arrays_atomic( & m0.nzind,  & m1.nzind  ) );

    let eq_nzval = 
        match ( & m0.nzval, & m1.nzval) {
            (None, None) => { true },
            (None, Some(_)) => { false },
            (Some(_), None) => { false },
            (Some(x), Some(y)) => { equal_read_only_arrays_atomic( &x, &y) }
        };
    tests.insert("nzval  ", eq_nzval );
    

    println!("{:?}", &tests );
    let eq_all = tests.values().all(|x| *x);

    return eq_all
}


//  ----------------------------------------------------------------
//  PRINT ENTRIES OF A READ-ONLY-ARRAY
//  ----------------------------------------------------------------


/// Print the entries in a ReadOnlyArray.
/// 
/// Each entry appears in a new line; `header` appears in the first line.
/// Print statements are executed in a `dist_iter` loop [ie an async
/// environment] so entries may appear out of order.
pub fn print_read_only_array<T: Dist + std::fmt::Debug>(
        array:  & ReadOnlyArray<T>,
        header: & str,
    ) {
    println!("{:?}", header );
    array.dist_iter()
        .enumerate()
        .for_each(|(i,entry)| println!("entry {:?}: {:?}", i, entry ) );
    array.wait_all();
}



















//  ======================================================================================================
//  UNIT TESTS
//  ======================================================================================================



#[cfg(test)]
mod tests {

    use crate::serial;
    use sparsemat as bale;
    use lamellar::LamellarWorldBuilder;    

    use super::*;


    //  ----------------------------------------------------------------
    //  3 X 3 IDENTITY MATRIX
    //  ---------------------------------------------------------------- 


    /// Test Lamellar permutation of the 3x3 identity matrix, where both permutations are identity
    /// 
    /// The test consists of applying the function `test_permutation` to the matrix.
    /// 
    /// This test is primarily intended as a sanity check.
    #[test]
    fn permute_small() {

        // let numrows = 3; let numcols = 3; let nnz = 3; 
        let offset = vec![0,1,2,3]; let nonzero = vec![0,1,2];

        let mut matrix_bale = bale::SparseMat::new(3,3,3);
        matrix_bale.offset = offset;
        matrix_bale.nonzero = nonzero;
        // let matrix_bale = bale::SparseMat{ numrows, numcols, nnz, offset, nonzero, value };
        let mut rperminv_bale = bale::Perm::new(3); // this generates the length-3 identity permutation
        let mut cperminv_bale = bale::Perm::new(3); // this generates the length-3 identity permutation
    
        let verbose = false;
        test_permutation(&matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );        
       
    }    

    //  ----------------------------------------------------------------
    //  RANDOM SQUARE MATRICES (ERDOS-RENYI)
    //  ----------------------------------------------------------------


    /// Test matrix permutation on Erdos-Renyi (ER) random matrices
    /// 
    /// - The ER matrices are generated via `bale::SparseMat::erdos_renyi_graph`.
    /// - The test consists of applying the function `test_permutation` to the matrix.
    /// - The test randomly generates a matrix of size n x n for each even n in 5 .. 100
    ///   - NB: Matrices must have at least one nonzero coefficient, else Lamellar throws an error (we could correct for this with case handling; shoudl we?)
    #[test]
    fn permute_erdos_renyi() {

        use sparsemat as bale;
        use rand::Rng;

        // parameters to generate the matrix
        let edge_probability        =   0.05;
        let simple                  =   false;
        let seed: i64                  =   rand::thread_rng().gen();

        for numrows in (5 .. 50).step_by(10) {

            // randomly generate a sparse matrix and permutation
            let mut rperminv_bale                =   bale::Perm::random( numrows, seed );
            let mut cperminv_bale                =   bale::Perm::random( numrows, seed );             
            let mut matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
            while matrix_bale.nonzero.len() == 0 { // re-generate the matrix until it has at least one structural nonzero
                matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
            }       

            // test the lamellar implementation matrix permutation
            let verbose = false;
            test_permutation(& matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );
       
        }    
    }


    //  ----------------------------------------------------------------
    //  HELPER FUNCTIONS
    //  ---------------------------------------------------------------- 


    /// Compare the results of matrix permutation via three methods (Bale serial, vec-of-vec serial, and Lamellar distributed)
    /// 
    /// - We ignore structural nonzero coefficients (only focus on sparcity pattern)
    /// - For each matrix, we compute its permutation 3 different ways
    ///   - The original Bale implementation
    ///   - A simple vec-of-vec implementation
    ///   - The distributed Lamellar implementation
    /// - Matrices must have at least one nonzero coefficient, else Lamellar throws an error (we could correct for this with case handling; shoudl we?)
    fn test_permutation(
        // matrix_vecvec:      & Vec< Vec< usize > >,
        // rperminv:           & Vec< usize >,
        // cperminv:           & Vec< usize >,
        matrix_bale:    & bale::SparseMat,
        rperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
        cperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
        verbose:        bool,
        ) {
    
           

        // ------------------------
        // SERIAL COMPUTATIONS 
        // ------------------------      

        // permute with Bale
        let permuted_bale           =   matrix_bale.permute(&rperminv_bale, &cperminv_bale);

        // permute the vecofvec
        let rperminv            =   perm_bale_to_perm_vec( rperminv_bale );
        let cperminv            =   perm_bale_to_perm_vec( cperminv_bale ); 



        let matrix_vecvec = matrix_bale_to_matrix_vecvec( matrix_bale );

        let permuted_vecvec = serial::permute_vec_of_vec(
                                                    & matrix_vecvec, 
                                                    & rperminv, 
                                                    & cperminv,
                                                );     
                                                
        let numrows = rperminv.len();     
        let numcols = cperminv.len();
        let nnz = matrix_vecvec.iter().map(|x| x.len()).sum();                                                    


        // ------------------------
        // DISTRIBUTED COMPUTATIONS
        // ------------------------


        // initialize
        // ----------
        
        let world = LamellarWorldBuilder::new().build(); // the world
        let nzind                   =   UnsafeArray::<usize>::new(&world,nnz,Distribution::Block); 
        let rowptr =   UnsafeArray::<usize>::new(&world,numrows + 1,Distribution::Block).into_atomic(); 
        let rperminv    =   UnsafeArray::<usize>::new(&world,numrows,Distribution::Block).into_atomic(); 
        let cperminv    =   UnsafeArray::<usize>::new(&world,numrows,Distribution::Block).into_atomic();         


        // copy data from the bale matrix to the lamellar arrays
        // ----------------------------------------------------------

        rowptr.store(0, 0);
        for p in 0..numrows {
            rowptr.store(p+1,matrix_bale.offset[p+1]);
            rperminv.store(p, rperminv_bale.entry(p) );            
            cperminv.store(p, cperminv_bale.entry(p) ); 
        }
        for p in 0..nnz {
            nzind.store(p, matrix_bale.nonzero[p] );                    
        }
        rowptr.wait_all(); rperminv.wait_all(); cperminv.wait_all(); nzind.wait_all();


        // make the lamellar arrays read-only
        // ----------------------------------

        let rowptr = rowptr.into_read_only();
        let nzind = nzind.into_read_only();
        let rperminv = rperminv.into_read_only();
        let cperminv = cperminv.into_read_only();


        // permute
        // -------

        let nzval = None;
        let matrix = SparseMat{ numrows, numcols, nnz, rowptr, nzind, nzval, };
        let (permuted, rperm_lamellar, decorated_ranges) 
                = matrix.permute_fax( &rperminv, &cperminv, world);


        // ------------------------
        // PRINT DIAGONSTICS (IF DESIRED)
        // ------------------------


        if verbose {
            println!("");
            println!("PERMUTATIONS");        
            println!("rperm lamellar {:?}", & rperm_lamellar );        
            println!("rperm vector   {:?}", & rperminv_bale.inverse().perm() );  

            println!("");
            println!("INVERSE PERMUTATIONS");
            println!("row bale:  {:?}", perm_bale_to_perm_vec( rperminv_bale ) );
            println!("row array: {:?}", read_only_array_to_vec(&rperminv) );        
            println!("col bale:  {:?}", perm_bale_to_perm_vec( cperminv_bale ) );        
            println!("col array: {:?}", read_only_array_to_vec(&cperminv) );  

            println!("");
            println!("ORIGINAL MATRICES");
            println!("vecvec from matrix          {:?}", matrix.to_vec_of_rowvec() );
            println!("vecvec from matrix_vecvec:  {:?}", &matrix_vecvec);
            println!("vecvec from matrix_bale:    {:?}", matrix_bale_to_matrix_vecvec(matrix_bale));

            println!("");
            println!("PERMUTED MATRICES");
            println!("vecvec from permuted         {:?}", permuted.to_vec_of_rowvec() );
            println!("vecvec from permuted_vecvec: {:?}", &permuted_vecvec);
            println!("vecvec from permuted_bale:   {:?}", matrix_bale_to_matrix_vecvec( &permuted_bale));

            println!("");
            println!("COMPONENTS - PERMUTED");
            println!("rowptr {:?}", read_only_array_to_vec( &permuted.rowptr) );
            println!("nzind {:?}", read_only_array_to_vec( &permuted.nzind) );        

            println!("");
            println!("COMPONENTS - ORIGINAL");
            println!("rowptr {:?}", read_only_array_to_vec( &matrix.rowptr) );
            println!("decorated_ranges {:?}", decorated_ranges );       
            println!("rperm lamellar {:?}", &rperm_lamellar ); 
        }
        

        // -------------------------------------------------------------
        // CHECK THAT ALL THREE PERMUTATION METHODS GIVE THE SAME ANSWER
        // -------------------------------------------------------------


        assert_eq!( &matrix_vecvec,   &matrix.to_vec_of_rowvec() );
        assert_eq!( &matrix_vecvec,   &matrix_bale_to_matrix_vecvec(matrix_bale) );   
        assert_eq!( &permuted_vecvec, &permuted.to_vec_of_rowvec() );
        assert_eq!( &permuted_vecvec, &matrix_bale_to_matrix_vecvec(&permuted_bale) );                           
    }    


    /// Convert a vector-of-row-vectors sparse matrix to a bale sparse matrix.
    fn vecvec_matrix_to_bale_matrix( vecvec: Vec<Vec<usize>>, numcols: usize) -> bale::SparseMat {

        let nnz = vecvec.iter().map(|x| x.len()).sum();
        let numrows = vecvec.len();

        let mut matrix_bale = bale::SparseMat::new( numrows, numcols, nnz, );

        for p in 0 .. numrows {
            matrix_bale.offset[p+1] = matrix_bale.offset[p] + vecvec[p].len();
            matrix_bale
                .nonzero[ matrix_bale.offset[p] .. matrix_bale.offset[p+1] ]
                .clone_from_slice( & vecvec[p][..] );
        }
        return matrix_bale
    }

    /// Export a copy of the matrix in vec-of-vec format.
    pub fn matrix_bale_to_matrix_vecvec( matrix_bale: & bale::SparseMat ) -> Vec<Vec<usize>> {
        let mut vecvec = Vec::with_capacity( matrix_bale.numrows() );
        for rindex in 0 .. matrix_bale.numrows() {
            let new_vec = matrix_bale.nonzero[ matrix_bale.offset[rindex] .. matrix_bale.offset[rindex+1] ].iter().cloned().collect();
            vecvec.push( new_vec );
        }
        return vecvec
    }    

    /// Convert a Bale Perm to a Vec< usize >
    pub fn perm_bale_to_perm_vec( perm_bale: & bale::Perm ) -> Vec<usize> {
        let mut perm = Vec::with_capacity( perm_bale.len() );
        for p in 0 .. perm_bale.len() {
            perm.push( perm_bale.entry(p) )
        }
        return perm
        // let perm_vec = Vec::with_capacity( perm_bale.len() );
        // let inner_data = perm_bale.perm();
        // for p in 0 .. inner_data.len() {
        //     perm_vec[p] = inner_data[p].clone();
        // }
        // return perm_vec;
    }

    // /// Convert a permutation represented as a Vec<usize> to a bale Perm
    // pub fn perm_vec_to_perm_bale(perm_vec: Vec<usize>){
    //     let mut perm_bale = bale::Perm::new( perm_vec.len() );
    //     let mut inner_data = perm_bale.perm();
    //     for 
    // } 

    //  ----------------------------------------------------------------
    //  MISCELLANEOUS
    //  ----------------------------------------------------------------     


    // Initialization of new sparse matrix
    #[test]
    fn new1() {
        let numrows         =       10;
        let numcols         =       10;
        let nnz             =       10;

        let world           =       lamellar::LamellarWorldBuilder::new().build();
        let _mat = SparseMat::new(numrows, numcols, nnz, world);

        let world           =       lamellar::LamellarWorldBuilder::new().build();
        let _mat = SparseMat::new_with_values(numrows, numcols, nnz, world);        
    }    

}

