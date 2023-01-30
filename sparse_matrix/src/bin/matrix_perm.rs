

use data_structures::*;
use data_structures::distributed::*;
use tabled::{Table, Tabled};

use lamellar::{LamellarWorld, LamellarWorldBuilder};  
use lamellar::ActiveMessaging;

use sparsemat as bale;

fn main() {

    //  PRINT TEST
    // let world = LamellarWorldBuilder::new().build();
    // if world.my_pe() == 0 { println!("print test"); }
    

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
    use sparsemat as bale;
    use rand::Rng;
 
    // parameters to generate the matrix
    let edge_probability        =   0.05;
    let simple                  =   false; // refers to the type of random matrix we generate
    let mut seed: i64;           //=   rand::thread_rng().gen();
    let mut times               =   Vec::new(); // each entry in this vec will store the run times for one execution of matrix perm

    // the world
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();

    // if my_pe == 0 {
        // run the tests
        for numrows in (5 .. 10).step_by(5) {

            seed = numrows as i64;
            // randomly generate a sparse matrix and permutation
            let mut rperminv_bale                =   bale::Perm::random( numrows, seed );
            let mut cperminv_bale                =   bale::Perm::random( numrows, seed );             
            let mut matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
            while matrix_bale.nonzero.len() == 0 { // re-generate the matrix until it has at least one structural nonzero
                matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
            }       
            println!("{:?}", matrix_bale );

            // test the lamellar implementation of matrix permu
            let verbose = false;
            println!("about to run test_permutation");
            let measurements = test_permutation( &world, & matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );
            
            times.push(measurements);
            world.wait_all();
            world.barrier();
        }    

        let table = Table::new(times.clone()).to_string();
        println!("{}",table.to_string());
    // }

}

// !!! check that wait_all and block_on happens outside if statement       


// - benchmarks: use master (probably a rofi issue; probably goes away when swith to master)
// - competing versions of randperm
// - debugging stack error
// - write little concise summary; share with ryan; see if anything jumps out at him
// - look for batching as a way to improve performance
// - 