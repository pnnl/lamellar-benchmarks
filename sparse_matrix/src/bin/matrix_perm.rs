


use data_structures::*;
use data_structures::distributed::*;
use data_structures::bale::sparsemat as bale;
use tabled::{Table, Tabled};

use lamellar::{LamellarWorld, LamellarWorldBuilder};  
use lamellar::ActiveMessaging;



fn main() {
    

    //  SMALL EXAMPLE
    //  =======================================================
    // println!("stuff stuff stuff");
    // // let numrows = 3; let numcols = 3; let nnz = 3; 
    // let offset = vec![0,1,2,3]; let nonzero = vec![0,1,2];

    // let mut matrix_bale = bale::SparseMat::new(3,3,3);
    // matrix_bale.offset = offset;
    // matrix_bale.nonzero = nonzero;
    // // let matrix_bale = bale::SparseMat{ numrows, numcols, nnz, offset, nonzero, value };
    // let mut rperminv_bale = bale::Perm::new(3); // this generates the length-3 identity permutation
    // let mut cperminv_bale = bale::Perm::new(3); // this generates the length-3 identity permutation

    // let verbose = false;
    // test_permutation(&matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );   
    

    //  ERDOS-RENYI EXAMPLE
    //  =======================================================    
    use rand::Rng;
 
    // parameters to generate the matrix
    let edge_probability        =   0.05;
    let simple                  =   false; // refers to the type of random matrix we generate
    let mut seed: u64;           //=   rand::thread_rng().gen();   <-- could try replacing with this to get randomly generated random seed ... but don't think we want that
    let mut times               =   Vec::new(); // each entry in this vec will store the run times for one execution of matrix perm

    // the world
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();

    // run the tests
    for numrows in (20 .. 201).step_by(20) {

        seed = numrows as u64;
        // randomly generate a sparse matrix and permutation
        let mut rperminv_bale                =   bale::Perm::random( numrows, seed );
        let mut cperminv_bale                =   bale::Perm::random( numrows, seed );             
        let mut matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
        while matrix_bale.nonzero.len() == 0 || matrix_bale.rowcounts().min().unwrap()==0 { // re-generate the matrix until it has at least one structural nonzero
            seed += 1;
            matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
        }       
        // println!("BALE MATRIX = {:?}", matrix_bale );

        // test the lamellar implementation of matrix perm
        let verbose = false;
        let verbose_debug = false;
        let measurements = test_permutation( &world, & matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose, verbose_debug, );
        
        times.push(measurements);
        world.wait_all();
        world.barrier();
    }    

    let table = Table::new(times.clone()).to_string();
    println!("{}",table.to_string());

}     