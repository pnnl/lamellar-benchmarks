

use data_structures::*;
use data_structures::distributed::*;
use tabled::{Table, Tabled};

use sparsemat as bale;

fn main() {
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
    
    
    use sparsemat as bale;
    use rand::Rng;
 

    // parameters to generate the matrix
    let edge_probability        =   0.05;
    let simple                  =   false;
    let seed: i64               =   rand::thread_rng().gen();
    let mut times               =   Vec::new();

    for numrows in (500 .. 5000).step_by(500) {

        // randomly generate a sparse matrix and permutation
        let mut rperminv_bale                =   bale::Perm::random( numrows, seed );
        let mut cperminv_bale                =   bale::Perm::random( numrows, seed );             
        let mut matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
        while matrix_bale.nonzero.len() == 0 { // re-generate the matrix until it has at least one structural nonzero
            matrix_bale = bale::SparseMat::erdos_renyi_graph(numrows, edge_probability, simple, seed); 
        }       

        // test the lamellar implementation matrix permutation
        let verbose = false;
        let measurements = test_permutation(& matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose );
        
        times.push(measurements)
    }    
    
    let table = Table::new(times.clone()).to_string();
    println!("{}",table.to_string());
}

       


// - benchmarks: use master (probably a rofi issue; probably goes away when swith to master)
// - competing versions of randperm
// - debugging stack error
// - write little concise summary; share with ryan; see if anything jumps out at him
// - look for batching as a way to improve performance
// - 