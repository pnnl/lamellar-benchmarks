//! Distributed permutation of sparse matrices, with Lamellar.
//! 
//! See the unit tests at the bottom of `lamellar_spmat_distributed.rs` for some examples.  These can be used as a starting point for benchmarks.

use lamellar::{LamellarWorld, IndexedDistributedIterator, LamellarArray, LamellarArrayIterators, SubArray, Dist, IndexedLocalIterator, LocalIterator, AccessOps, ActiveMessaging};
use lamellar::array::{AtomicArray, UnsafeArray, Distribution, DistributedIterator, ReadOnlyArray, ReadOnlyOps };
use lamellar::LamellarArrayMutIterators;
use lamellar::LamellarArrayCompareReduce;
use lamellar::LamellarWorldBuilder;  
use lamellar::StridedArch; 
use lamellar::OneSidedIterator;
use lamellar::ArithmeticOps;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{Ordering, AtomicBool};

use crate::serial;
use crate::bale::sparsemat as bale;
use crate::binary_search::find_window;

use tabled::{Table, Tabled};




//  ----------------------------------------------------------------
//  SPARSE MATRIX STRUCT      (INCLUDES METHOD FOR PERMUTATION)
//  ----------------------------------------------------------------


/// CSR data structure for a sparse matrix
/// 
/// The matrix need not contain explicit values for the structural nonzero coefficients.
#[derive(Debug, Clone)]
pub struct SparseMat {
    /// number of rows
    pub numrows: usize, 
    /// number of columns
    pub numcols: usize, 
    /// number of structural nonzeros
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
        let rowptr  = ReadOnlyArray::<usize>::new(world.team(), numcols+1 , Distribution::Block);
        let nzind = ReadOnlyArray::<usize>::new(world.team(), nnz , Distribution::Block);
        SparseMat {
            numrows,
            numcols,
            nnz,
            rowptr,
            nzind,
            nzval: None,
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

    /// Export self as a dense matrix
    pub fn dense(&self, world: &LamellarWorld) -> Vec<Vec<usize>> {        
        let vecvec  =   self.to_vec_of_rowvec( world );
        let mut dense   =   vec![ vec![0; self.numcols() ]; self.numrows()  ];
        for (rownum, rowvec) in vecvec.iter().enumerate() { 
            for colnum in rowvec.iter() { dense[rownum][*colnum] = 1 } 
        }
        return dense
    }

    /// Print self as a dense matrix.
    pub fn print(&self, world: &LamellarWorld ) {
        let vecvec  =   self.to_vec_of_rowvec( world );
        let mut row =   vec![0; self.numcols() ];
        for (rownum, rowvec) in vecvec.iter().enumerate() { 
            for k in 0..self.numcols() { row[k] = 0 };
            for colnum in rowvec.iter() { row[*colnum] = 1 } 
            println!("row {:03}: {:?}", rownum, & row);
        }        
    }


    /// Apply row and column permutations to a sparse matrix
    /// 
    /// The entries in each row of the permuted matrix are **not** sorted.
    /// (It seems that the original Bale serial implementation of matrix permutation
    /// does not always sort, either -- this is supported by some of the unit tests
    /// in the `data_structures/src/distributed.rs` file).
    /// 
    /// **NB** The suffix "_like_serial_bale" indicates that this is as close as we can come to a
    /// facsimile of the serial Rust code for matrix permutation provided in Bale.
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
    pub fn permute_like_serial_bale(
                &self, 
                cperminv:       &   ReadOnlyArray<usize>, 
                rperminv:       &   ReadOnlyArray<usize>, 
                world:          &   LamellarWorld,
                verbose_debug:      bool,
            ) 
            ->  (
                    SparseMat, 
                    Vec<usize>, 
                    Vec<(f64, usize,usize)>,
                    HashMap< &str, f64 >,
                ) 
        {                  
            

        // wait for all other processes to finish so we can get a clean read on time
        // -----------------------------------------------------------------------------------------------------------------        
        let my_pe = world.my_pe();
        let timer_total = std::time::Instant::now();            

        
        // we'll handle matrix coefficients in future versions of this code
        // -----------------------------------------------------------------------------------------------------------------
        if let Some(_) = &self.nzval {
            todo!()
        }

        // a hashmap to track a few run times
        // -----------------------------------------------------------------------------------------------------------------        
        let mut times                   =   HashMap::new(); 


        // Preallocate a new sparse matrix + a distributed array representing the inverse of the row permutation
        // -----------------------------------------------------------------------------------------------------------------        
        let numrows         =   self.numrows();
        let numcols         =   self.numcols();
        let nnz             =   self.nnz();
        let nzval           =   None;
        let othr_rowptr     =   AtomicArray::<usize>::new( world.team(), numcols+1 , Distribution::Block);        
        let othr_nzind      =   AtomicArray::<usize>::new( world.team(), nnz ,       Distribution::Block);
        let rperm           =   AtomicArray::<usize>::new( world.team(), numrows ,   Distribution::Block);           
        if verbose_debug {
            println!("check matrices have equal size --- numrow {numrows} numcols {numcols} nnz {nnz}");                
        }

        // Invert the row permutation
        // -----------------------------------------------------------------------------------------------------------------        
        // This helps by allowing us to run a (serial) for-loop in ascending order of the rows in the *new* matrix
        // (that is, for i = 0, 1, .., we update the ith row of the new matrix by pulling data from the rperm[i] row
        // of the old matrix)
        
        // The following loop needs to modify the entries in rperm, but the array we modify
        // will be comsumed by the closure.  So use a clone of rperm in the loop.
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
        world.barrier();
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
        let timer = std::time::Instant::now();
        for rindexnew in 0 .. numrows
        {
            let rindexold = rperm.block_on(rperm.load( rindexnew ));
            {
                // define the section of self.nzind that we will copy into the permuted matrix
                // !!! this is probably unnecessary
                let read_range_start    =   self_rowptr_clone.block_on(self_rowptr_clone.load(rindexold )); //.await;
                let read_range_end      =   self_rowptr_clone.block_on(self_rowptr_clone.load(rindexold +1 )); //.await;
                let read_range          =   read_range_start .. read_range_end;
                let subarrayold         =   self_nzind_clone.sub_array(read_range);

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
                world.barrier();
                
                // update the variable that tracks the number of structural nonzeros we've
                // written to the last k rows
                nnzwritten += read_range_end - read_range_start;    

                // update a variable used for debugging
                decorated_ranges.push( (rindexold as f64, read_range_start, read_range_end) );                             
            }
            world.wait_all();
            world.barrier(); 
            // update the row offset pattern of the new array  
            if my_pe == 0 {
                othr_rowptr.block_on( othr_rowptr.store( rindexnew+1, nnzwritten ) );  
                if verbose_debug { println!( "nnzwritten = {:?}", nnzwritten ); }                
            }
            world.wait_all();
            world.barrier();              
        }
        times.insert( "rows",  timer.elapsed().as_secs_f64() );        


        // we're done updating rowptr, so conver to read-only
        let rowptr = othr_rowptr.into_read_only();

        // sanity check
        let maxind = othr_nzind.block_on(othr_nzind.max());
        if maxind >= cperminv.len() {
            println!("!!! error: maxind >= cperminv.len()");
        }        

        // Update the nonzero indices
        // -----------------------------------------------------------------------------------------------------------------            

        let timer = std::time::Instant::now();        

        // relabel the contiguous batch of indices stored locally on this pe
        let remapped_indices = world.block_on( cperminv.batch_load( othr_nzind.local_data() ) );
        world.barrier();

        // encode the destinations where we will write the remapped indices as a vector
        let first_global_index = othr_nzind.first_global_index_for_pe( my_pe ).unwrap() ;
        let last_global_index  = first_global_index + remapped_indices.len();           
        let insert_range: Vec<usize> = ( first_global_index .. last_global_index ).collect();

        // write the remapped indices to their destinations
        othr_nzind.batch_store( insert_range, remapped_indices );
        world.barrier();
        
        let nzind = othr_nzind.into_read_only();

        times.insert( "colind", timer.elapsed().as_secs_f64() );                          
    
        // Return 
        // -----------------------------------------------------------------------------------------------------------------            
        
        times.insert("total",  timer_total.elapsed().as_secs_f64() );                          
        return  (   
                    SparseMat { numrows, numcols, nnz, rowptr, nzind, nzval }, 
                    read_only_array_to_vec(&rperm, &world ),
                    decorated_ranges,
                    times,
                )
                
    }    
















    pub fn transpose( &self, world: &LamellarWorld, verbose: bool ) -> SparseMat {

        // wait for all other processes to finish so we can get a clean read on time
        // -----------------------------------------------------------------------------------------------------------------        
        let my_pe = world.my_pe();
        let timer_total = std::time::Instant::now();            

        
        // we'll handle matrix coefficients in future versions of this code
        // -----------------------------------------------------------------------------------------------------------------
        if let Some(_) = &self.nzval {
            todo!()
        }

        // a hashmap to track a few run times
        // -----------------------------------------------------------------------------------------------------------------        
        // let mut times                   =   HashMap::new(); 


        // Preallocate a new sparse matrix 
        // -----------------------------------------------------------------------------------------------------------------        
        let numrows         =   self.numcols();
        let numcols         =   self.numrows();
        let nnz             =   self.nnz();
        let nzval           =   None;
        let rowptr          =   AtomicArray::<usize>::new( world.team(), numcols+1 , Distribution::Block);        
        let nzind           =   AtomicArray::<usize>::new( world.team(), nnz ,       Distribution::Block);

        // Create a local copy of the row pointers
        // ------------------------------------------------  
        let self_rowptr_local   =   read_only_array_to_vec( & self.rowptr, world );
        
        // Precompute the sparsity pattern of the transpose
        // ------------------------------------------------
        let rowptr_clone    =   rowptr.clone(); // create a clone that can be consumed, but will allow us to modify entries in the (uncomsumed) array

        // let mut rowptr_vec  =   vec![0; numcols+1 ];
        // self.nzind.dist_iter()
        //     .for_each( 
        //                 |colind|
        //                 { rowptr_vec.add( colind+1, 1 ); }
        //     );      
        // we can almost do this with an index gather, but not quite, because we need to offset things by 1  
        self.nzind.dist_iter()
            .for_each( 
                        move |col|
                        if *col < numcols-1 { rowptr_clone.add( col+2, 1 ); }
            );
        //wait for all updates to finish            
        world.wait_all(); 
        world.barrier();

        if verbose {
            println!("column counts (each entry shifted 2 steps to the right):");
            rowptr.print();        
        }

        

        if my_pe == 0 {
            for (index,entry) in rowptr.onesided_iter().into_iter().enumerate() {
                if index < self.numrows {
                    rowptr.block_on( rowptr.add( index+1, *entry ) );
                }                
            }
        }
        // rowptr.onesided_iter() //iterate over entire array from a single PE
        //     .enumerate()
        //     .for_each(
        //         |(index,entry)|
        //         if index < self.numrows - 1 {
        //             rowptr.add( index+1, entry );
        //         }
        //     );
        world.wait_all(); 
        world.barrier();  
        
        if verbose{
            println!("preformatted rowptr:");
            rowptr.print();
                   
            println!("add note to header of atomic array file");
        }

        // Write to the matrix
        let rowptr_clone = rowptr.clone();
        let nzind_clone = nzind.clone();        
        self.nzind
            .dist_iter()
            .enumerate()
            .for_each(
                move |(linindex_old, col)|  // the linear index of the entry, and the col to which it belongs
                {
                    let row             =   find_window( & self_rowptr_local, linindex_old ).unwrap();       // the row to which the entry belongs
                    let linindex_new    =   rowptr_clone.block_on( rowptr_clone.fetch_add( *col+1, 1) );     // the linear index in the target array
                    nzind_clone.block_on( nzind_clone.store( linindex_new, row ) );
                }
            );
        world.wait_all(); 
        world.barrier();                
            

        // Wrap results into a sparse matrix, and return
        // ---------------------------------------------
        let nzind           =   nzind.into_read_only();
        let rowptr          =   rowptr.into_read_only();        
        return SparseMat { numrows, numcols, nnz, rowptr, nzind, nzval }
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
        let rowptr = vec_to_atomic_array( & rowptr_vec, & world ).into_read_only();
        let nzind  = vec_to_atomic_array( & nzind_vec,  & world ).into_read_only();
        let nzval = None;

        return SparseMat{ numrows, numcols, nnz, nzind, nzval, rowptr }

    }
        
    /// Generate a Vector-of-Vectors representation of SparseMat.
    pub fn to_vec_of_rowvec( &self, world: &LamellarWorld ) -> Vec< Vec< usize > > {
        
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

            // println!("{p} (in `to_vec_of_rowvec`) self.nzind = ");
            // self.nzind.print();
            // println!("{:?}", "add new vec: start");
            // println!("alpha, omega, nnz = {:?}, {:?}, {:?}", ind_alpha, ind_omega, self.nzind.len() );        
            let a = self.nzind.sub_array(ind_alpha .. ind_omega);
            // let x = a.local_data(); raises an error when a is emtpy
            newrow = read_only_array_to_vec( &a, world );
            vecvec.push( newrow );
            // println!("{p} -- {:?}", "add new vec: end");            
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

    //     let rowptr  = UnsafeArray::<usize>::new(world.team(), self.numcols+1 , Distribution::Block).into_atomic();        

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
pub fn vec_to_atomic_array<T:Dist + Clone + Default>( vec: &Vec<T>, world: & LamellarWorld ) -> AtomicArray<T> {
    let array = AtomicArray::<T>::new( world.team(), vec.len() , Distribution::Block); 
    if world.my_pe() == 0 { //we only need to do the init from pe 0 not all the pes...        
        let indices = (0..vec.len()).collect::<Vec<_>>();
        array.batch_store(indices,vec.clone());   //I think...             
        world.wait_all();
    }
    world.barrier();  
    return array   
    // DEPRECATED ALTERNATIVE (UNTESTED): 
    // let array = AtomicArray::<T>::new( world.team(), vec.len() , Distribution::Block); 
    // let vec_clone = vec.clone(); // not ideal to copy the data, i just haven't had time to figure out a fix
    // unsafe {
    //     array
    //         .dist_iter_mut()
    //         .enumerate()
    //         .for_each(
    //                 move |x|
    //                 { *x.1 = vec_clone[ x.0 ].clone(); }
    //             );        
    // }        
    // world.wait_all();
    // return array    
}

/// Convert ReadOnlyArray to Vec.
pub fn read_only_array_to_vec<T:Dist+Clone+std::fmt::Debug>( array: &ReadOnlyArray<T>, world: & LamellarWorld ) -> Vec<T> {
    array.onesided_iter() //iterate over entire array from a single PE
         .into_iter() // convert into rust iterator
         .cloned()
         .collect::<Vec<_>>()  
    // DEPRECATED ALTERNATIVE (UNTESTED)  
    // if array.len() == 0 { return Vec::with_capacity(0) }
    // else {
    //     // println!("num_entries = {:?}", array.len() );
    //     if world.my_pe() == 0 {
    //         let vec = array.local_data().to_vec(); 
    //         world.wait_all();
    //     }
    //     world.barrier();
    //     return array.local_data().to_vec(); 
    // }         
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




//  ----------------------------------------------------------------
//  HELPER FUNCTIONS FOR UNIT TESTING
//  ---------------------------------------------------------------- 

/// Returns a string of form "EK X.YZ"
///
/// NB: Tried providing an argument for number of significant digits,
///     but there are a few hurdles:
///     - Rust's `.round()` only rounds to integer values
///     - Tried the trick of multiplying by a power of 10, rounding to 
///       the nearest integer, then scaling back, but sometimes this
///       creates trailing entries far beyond the intended decimal point
pub fn sci_notation(x: f64 ) -> String {
    if x == 0.0 { return String::from("0.00") }
    let log = x.log10().floor() as i32;
    let mut val = x * 10.0_f64.powi(-log);    
    // val = val * 10.0_f64.powi(n_decimals);
    // val = val.round();
    // val = val * 10.0_f64.powi(-n_decimals);
    format!("E{} {:.2}", log, val)
}

/// A struct used as a plug-in for the `tabled` crate, which prints tables;
/// Specifically, each instance of this struct will become a row in a table.
#[derive(Tabled,Clone)]
pub struct RunTimeMatrixPerm {
    pub numpes:                 usize,
    pub numrows:                usize,
    pub numcols:                usize,
    pub nnz:                    usize,
    pub bale_serial:            String,
    pub lamellar_serial:        String,
    pub lamellar_dist_total:    String,
    pub lamellar_dist_rows:     String,
    pub lamellar_dist_colind:   String,
}   

/// A struct used as a plug-in for the `tabled` crate, which prints tables;
/// Specifically, each instance of this struct will become a row in a table.
#[derive(Tabled,Clone)]
pub struct RunTimeMatrixTranspose {
    pub numpes:                 usize,
    pub numrows:                usize,
    pub numcols:                usize,
    pub nnz:                    usize,
    pub lamellar_serial:        String,
    pub lamellar_distributed:   String,
}   

/// Compare the results of matrix permutation via three methods (Bale serial, vec-of-vec serial, and Lamellar distributed)
/// 
/// - We ignore structural nonzero coefficients (only focus on sparcity pattern)
/// - For each matrix, we compute its permutation 3 different ways
///   - The original Bale implementation
///   - A simple vec-of-vec implementation
///   - The distributed Lamellar implementation
/// - Matrices must have at least one nonzero coefficient, else Lamellar throws an error (we could correct for this with case handling; shoudl we?)
pub fn test_permutation(
    // matrix_vecvec:      & Vec< Vec< usize > >,
    // rperminv:           & Vec< usize >,
    // cperminv:           & Vec< usize >,
    world:          & LamellarWorld,
    matrix_bale:    & bale::SparseMat,
    rperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
    cperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
    verbose:        bool,
    verbose_debug:  bool,
    ) 
    -> RunTimeMatrixPerm
    {

        
    // ------------------------
    // PARSE INPUT
    // ------------------------ 

    let rperminv            =   perm_bale_to_perm_vec( rperminv_bale );
    let cperminv            =   perm_bale_to_perm_vec( cperminv_bale ); 
    let numrows             =   rperminv.len();     
    let numcols             =   cperminv.len();
    let matrix_vecvec       =   matrix_bale_to_matrix_vecvec( matrix_bale );  // format a new matrix   
    let nnz                 =   matrix_vecvec.iter().map(|x| x.len()).sum();     

    let my_pe       =   world.my_pe();    
    if my_pe == 0 {
        println!("number of rows: {:?}",numrows);
        println!("number of colinds: {:?}",nnz);  
    } 
    

    // // ------------------------
    // // SERIAL COMPUTATIONS 
    // // ------------------------      

    // // permute with Bale
    // // -----------------

    world.wait_all();
    world.barrier();
    let timer = std::time::Instant::now();  // start timer
    let permuted_serial_bale           =   matrix_bale.permute(&rperminv_bale, &cperminv_bale);
    let time_bale_serial = timer.elapsed().as_secs_f64();
    if my_pe == 0 {
        println!("bale serial time: {:?}", time_bale_serial );
    }


    // // permute the vecofvec
    // // --------------------

    // world.barrier();    
    let timer = std::time::Instant::now();  // start timer    
    let permuted_vecvec = serial::permute_vec_of_vec(
                                                & matrix_vecvec, 
                                                & rperminv, 
                                                & cperminv,
                                            );  
    let time_lamellar_serial            =  timer.elapsed().as_secs_f64(); 
    if my_pe == 0 {    
        println!("lamellar serial time: {:?}", time_lamellar_serial );
    }
                                         

    // ------------------------
    // DISTRIBUTED COMPUTATIONS
    // ------------------------

    if verbose { println!("initiating distributed computations"); }
    
    // initialize
    // ----------
    
    let rowptr          =   vec_to_atomic_array( & matrix_bale.offset,      world ).into_read_only();
    let rperminv        =   vec_to_atomic_array( rperminv_bale.perm_ref(),  world ).into_read_only();
    let cperminv        =   vec_to_atomic_array( cperminv_bale.perm_ref(),  world ).into_read_only();
    let nzind           =   vec_to_atomic_array( & matrix_bale.nonzero,     world ).into_read_only();    
    if verbose { 
        println!("lamellar nzind =");
        nzind.print();
    }

    rowptr.wait_all(); rperminv.wait_all(); cperminv.wait_all(); nzind.wait_all();
    world.barrier();        

    // permute
    // -------

    let nzval = None;

    if verbose_debug{ println!("CREATING SPARSE MATRIX"); }   
    let matrix = SparseMat{ numrows, numcols, nnz, rowptr, nzind, nzval, };
    if verbose_debug{ println!("about to distributed_permute"); }      
    let (permuted, rperm_lamellar, decorated_ranges, distributed_run_times ) 
            = matrix.permute_like_serial_bale( &rperminv, &cperminv, &world, verbose_debug );

    world.barrier(); // wait for processes to finish    

    if my_pe == 0 {
        println!("lamellar distributed time: {:?}", distributed_run_times["total"]);
    };

    // ------------------------
    // PRINT DIAGONSTICS (IF DESIRED)
    // ------------------------


    // if verbose && my_pe == 0 {
    //     println!("");
    //     println!("PERMUTATIONS");        
    //     println!("rperm lamellar {:?}", & rperm_lamellar );        
    //     println!("rperm vector   {:?}", & rperminv_bale.inverse().perm() );  

    //     println!("");
    //     println!("INVERSE PERMUTATIONS");
    //     println!("row bale:  {:?}", perm_bale_to_perm_vec( rperminv_bale ) );
    //     println!("row array: {:?}", read_only_array_to_vec(&rperminv, &world) );        
    //     println!("col bale:  {:?}", perm_bale_to_perm_vec( cperminv_bale ) );        
    //     println!("col array: {:?}", read_only_array_to_vec(&cperminv, &world) );  

    //     println!("");
    //     println!("ORIGINAL MATRICES");
    //     println!("vecvec from matrix          {:?}", matrix.to_vec_of_rowvec( &world ) );
    //     println!("vecvec from matrix_vecvec:  {:?}", &matrix_vecvec);
    //     println!("vecvec from matrix_bale:    {:?}", matrix_bale_to_matrix_vecvec(matrix_bale));

    //     println!("");
    //     println!("PERMUTED MATRICES");        
    //     println!("vecvec from permuted         {:?}", permuted.to_vec_of_rowvec( &world ) );
    //     println!("vecvec from permuted_vecvec: {:?}", &permuted_vecvec);
    //     println!("vecvec from permuted_serial_bale:   {:?}", matrix_bale_to_matrix_vecvec( &permuted_serial_bale));

    //     println!("");
    //     println!("COMPONENTS - PERMUTED");
    //     println!("rowptr {:?}", read_only_array_to_vec( &permuted.rowptr, &world) );
    //     println!("nzind {:?}", read_only_array_to_vec( &permuted.nzind, &world ) );        

    //     println!("");
    //     println!("COMPONENTS - ORIGINAL");
    //     println!("rowptr {:?}", read_only_array_to_vec( &matrix.rowptr, &world ) );
    //     println!("decorated_ranges {:?}", decorated_ranges );       
    //     println!("rperm lamellar {:?}", &rperm_lamellar ); 
    // }
    

    // -------------------------------------------------------------
    // CHECK THAT ALL THREE PERMUTATION METHODS GIVE THE SAME ANSWER
    // -------------------------------------------------------------

    if verbose_debug {
        println!("checking whether .to_vec_of_rowvec is the root of our problem");
        let _ = matrix.to_vec_of_rowvec( &world );
        println!("if this prints, then .to_vec_of_rowvec probably isn't the root");    
    }

    assert_eq!( &matrix_vecvec,   &matrix.to_vec_of_rowvec( &world ) );
    assert_eq!( &matrix_vecvec,   &matrix_bale_to_matrix_vecvec(matrix_bale) );   
    assert_eq!( &permuted_vecvec, &permuted.to_vec_of_rowvec( &world ) );
    assert_eq!( &permuted_vecvec, &matrix_bale_to_matrix_vecvec(&permuted_serial_bale) );  
    
    return RunTimeMatrixPerm{
            numrows, 
            numcols,
            nnz, 
            bale_serial:            sci_notation( time_bale_serial                ),// , 3_i32   ), 
            lamellar_serial:        sci_notation( time_lamellar_serial            ),// , 3_i32   ),
            lamellar_dist_total:    sci_notation( distributed_run_times["total"]  ),// , 3_i32   ),
            lamellar_dist_rows:     sci_notation( distributed_run_times["rows"]   ),// , 3_i32   ),
            lamellar_dist_colind:   sci_notation( distributed_run_times["colind"] ),// , 3_i32   ),                        
            numpes:                 world.num_pes(),
        }

}    



/// Compare the results of matrix permutation via three methods (Bale serial, vec-of-vec serial, and Lamellar distributed)
/// 
/// - We ignore structural nonzero coefficients (only focus on sparcity pattern)
/// - For each matrix, we compute its permutation 3 different ways
///   - The original Bale implementation
///   - A simple vec-of-vec implementation
///   - The distributed Lamellar implementation
/// - Matrices must have at least one nonzero coefficient, else Lamellar throws an error (we could correct for this with case handling; shoudl we?)
pub fn test_transpose(
    // matrix_vecvec:      & Vec< Vec< usize > >,
    // rperminv:           & Vec< usize >,
    // cperminv:           & Vec< usize >,
    world:          & LamellarWorld,
    matrix_bale:    & bale::SparseMat,
    verbose:        bool,
    verbose_debug:  bool,
    ) 
    -> RunTimeMatrixTranspose
    {

        
    // ------------------------
    // PARSE INPUT
    // ------------------------ 


    let matrix_vecvec       =   matrix_bale_to_matrix_vecvec( matrix_bale );  // format a new matrix   
    let numrows             =   matrix_bale.numrows();
    let numcols             =   matrix_bale.numcols();   
    let nnz                 =   matrix_bale.nnz();     

    let my_pe       =   world.my_pe();    
    if my_pe == 0 {
        println!("number of rows: {:?}",    numrows);
        println!("number of colinds: {:?}", nnz);  
    } 

    // // ------------------------
    // // SERIAL COMPUTATIONS 
    // // ------------------------      

    // // transpose the vecofvec
    // // --------------------

    // world.barrier();    
    let timer = std::time::Instant::now();  // start timer    
    let transposed_vecvec = serial::transpose_vec_of_vec(
                                                & matrix_vecvec, 
                                                matrix_bale.numcols, 
                                            );  
    let time_lamellar_serial            =  timer.elapsed().as_secs_f64(); 
    if my_pe == 0 {    
        println!("lamellar serial time: {:?}", time_lamellar_serial );
    }
                                         

    // ------------------------
    // DISTRIBUTED COMPUTATIONS
    // ------------------------
    
    // initialize
    // ----------
    
    let rowptr          =   vec_to_atomic_array( & matrix_bale.offset,      world ).into_read_only();
    let nzind           =   vec_to_atomic_array( & matrix_bale.nonzero,     world ).into_read_only();    
    if verbose { println!("initiating distributed computations"); }    
    if verbose { println!("lamellar nzind ="); nzind.print(); }

    rowptr.wait_all(); 
    nzind.wait_all();
    world.barrier();        


    // permute
    // -------

    let nzval = None;

    if verbose_debug{ println!("creating sparse matrix"); }   
    let matrix = SparseMat{ numrows, numcols, nnz, rowptr, nzind, nzval, };
    
    if verbose{ println!("matrix:"); matrix.print(&world); println!("about to distributed_transpose"); }    
    

    let timer = std::time::Instant::now();  // start timer       
    let transposed_lamellar 
            = matrix.transpose( &world, verbose );
    let time_lamellar_parallel            =  timer.elapsed().as_secs_f64();             

    world.barrier(); // wait for processes to finish  
    


    // if my_pe == 0 {
    //     println!("lamellar distributed time: {:?}", distributed_run_times["total"]);
    // };

    // ------------------------
    // PRINT DIAGONSTICS (IF DESIRED)
    // ------------------------


    // if verbose && my_pe == 0 {
    //     println!("");
    //     println!("PERMUTATIONS");        
    //     println!("rperm lamellar {:?}", & rperm_lamellar );        
    //     println!("rperm vector   {:?}", & rperminv_bale.inverse().perm() );  

    //     println!("");
    //     println!("INVERSE PERMUTATIONS");
    //     println!("row bale:  {:?}", perm_bale_to_perm_vec( rperminv_bale ) );
    //     println!("row array: {:?}", read_only_array_to_vec(&rperminv, &world) );        
    //     println!("col bale:  {:?}", perm_bale_to_perm_vec( cperminv_bale ) );        
    //     println!("col array: {:?}", read_only_array_to_vec(&cperminv, &world) );  

    //     println!("");
    //     println!("ORIGINAL MATRICES");
    //     println!("vecvec from matrix          {:?}", matrix.to_vec_of_rowvec( &world ) );
    //     println!("vecvec from matrix_vecvec:  {:?}", &matrix_vecvec);
    //     println!("vecvec from matrix_bale:    {:?}", matrix_bale_to_matrix_vecvec(matrix_bale));

    //     println!("");
    //     println!("PERMUTED MATRICES");        
    //     println!("vecvec from permuted         {:?}", permuted.to_vec_of_rowvec( &world ) );
    //     println!("vecvec from permuted_vecvec: {:?}", &permuted_vecvec);
    //     println!("vecvec from permuted_serial_bale:   {:?}", matrix_bale_to_matrix_vecvec( &permuted_serial_bale));

    //     println!("");
    //     println!("COMPONENTS - PERMUTED");
    //     println!("rowptr {:?}", read_only_array_to_vec( &permuted.rowptr, &world) );
    //     println!("nzind {:?}", read_only_array_to_vec( &permuted.nzind, &world ) );        

    //     println!("");
    //     println!("COMPONENTS - ORIGINAL");
    //     println!("rowptr {:?}", read_only_array_to_vec( &matrix.rowptr, &world ) );
    //     println!("decorated_ranges {:?}", decorated_ranges );       
    //     println!("rperm lamellar {:?}", &rperm_lamellar ); 
    // }
    

    // -------------------------------------------------------------
    // CHECK THAT ALL THREE PERMUTATION METHODS GIVE THE SAME ANSWER
    // -------------------------------------------------------------

    if verbose_debug {
        println!("checking whether .to_vec_of_rowvec is the root of our problem");
        let _ = matrix.to_vec_of_rowvec( &world );
        println!("if this prints, then .to_vec_of_rowvec probably isn't the root");    
        println!("the transposed matrix row pattern:");
        transposed_lamellar.rowptr.print();
        println!("the transposed matrix nzind:");        
        transposed_lamellar.nzind.print();        
        transposed_lamellar.print( &world );
    }



    let mut transposed_lamellar_vecvec  =   transposed_lamellar.to_vec_of_rowvec( &world );
    for vec in transposed_lamellar_vecvec.iter_mut() { vec.sort() }

    if verbose_debug {
        println!("writing the transposed matrix to vec-of-vec format isn't the problem");
    }

    assert_eq!( &transposed_vecvec, & transposed_lamellar_vecvec );

    // assert_eq!( &matrix_vecvec,   &matrix.to_vec_of_rowvec( &world ) );
    // assert_eq!( &matrix_vecvec,   &matrix_bale_to_matrix_vecvec(matrix_bale) );   
    // assert_eq!( &transposed_vecvec, &permuted.to_vec_of_rowvec( &world ) );
    // assert_eq!( &transposed_vecvec, &matrix_bale_to_matrix_vecvec(&permuted_serial_bale) );  
    
    return RunTimeMatrixTranspose{
            numrows, 
            numcols,
            nnz, 
            lamellar_serial:        sci_notation( time_lamellar_serial      ),// , 3_i32   ),
            lamellar_distributed:   sci_notation( time_lamellar_parallel    ),// , 3_i32   ),
            numpes:                 world.num_pes(),
        }
} 




pub fn bale_serial_agrees_with_lamellar_serial(
    world:          & LamellarWorld,
    matrix_bale:    & bale::SparseMat,
    rperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
    cperminv_bale:  & bale::Perm, // we have to borrow as mutable because otherwise there's no way to access the inner data
    verbose:        bool,
    ) 
    {
        
    // ------------------------
    // PARSE INPUT
    // ------------------------ 

    let rperminv            =   perm_bale_to_perm_vec( rperminv_bale );
    let cperminv            =   perm_bale_to_perm_vec( cperminv_bale ); 
    let numrows             =   rperminv.len();     
    let numcols             =   cperminv.len();
    let matrix_vecvec       =   matrix_bale_to_matrix_vecvec( matrix_bale );  // format a new matrix   
    let nnz: usize          =   matrix_vecvec.iter().map(|x| x.len()).sum();     

    let my_pe       =   world.my_pe();    
    if my_pe == 0 && verbose {
        println!("number of rows: {:?}",numrows);
        println!("number of colinds: {:?}",nnz);  
    }
 
    let permuted_serial_bale           =   matrix_bale.permute(&rperminv_bale, &cperminv_bale);
    let permuted_vecvec = serial::permute_vec_of_vec(
                                                & matrix_vecvec, 
                                                & rperminv, 
                                                & cperminv,
                                            );      

    // -------------------------------------------------------------
    // CHECK THAT ALL THREE PERMUTATION METHODS GIVE THE SAME ANSWER
    // -------------------------------------------------------------

    assert_eq!( &matrix_vecvec,   &matrix_bale_to_matrix_vecvec(matrix_bale) );   
    assert_eq!( &permuted_vecvec, &matrix_bale_to_matrix_vecvec(&permuted_serial_bale) );  
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
    // DEPRECATED ALTERNATIVE (UNTESTED)
    // let perm_vec = Vec::with_capacity( perm_bale.len() );
    // let inner_data = perm_bale.perm();
    // for p in 0 .. inner_data.len() {
    //     perm_vec[p] = inner_data[p].clone();
    // }
    // return perm_vec;
}















//  ======================================================================================================
//  UNIT TESTS
//  ======================================================================================================



#[cfg(test)]
pub mod tests {

    use crate::serial;
    use crate::bale::sparsemat as bale;
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
        let verbose_debug = false;
        let world = lamellar::LamellarWorldBuilder::new().build();
        test_permutation(&world, &matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose, verbose_debug );        
       
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

        use crate::bale::sparsemat as bale;
        use rand::Rng;

        // parameters to generate the matrix
        let edge_probability        =   0.05;
        let simple                  =   false;
        let seed: i64                  =   rand::thread_rng().gen();

        // create the world
        let wolrd = LamellarWorldBuilder::new().build();

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
            let verbose_debug = false;
            bale_serial_agrees_with_lamellar_serial( & matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );
            _ = test_permutation( &world, & matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose, verbose_debug );
       
        }    
    }






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

        // let world           =       lamellar::LamellarWorldBuilder::new().build();
        // let _mat = SparseMat::new_with_values(numrows, numcols, nnz, world);        
    }    

}

