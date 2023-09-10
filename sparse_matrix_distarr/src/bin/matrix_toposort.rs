


use data_structures::*;
use data_structures::distributed::*;
use data_structures::bale::sparsemat as bale;
use tabled::{Table, Tabled};

use lamellar::{LamellarWorld, LamellarWorldBuilder};  
use lamellar::ActiveMessaging;



fn main() {

    // println!("Enter matrix size:");
    // let mut line                =   String::new();    
    // std::io::stdin().read_line(&mut line).unwrap();
    // let numrows                 =   line.trim().parse().unwrap();
    // println!("You entered {:?}", numrows );
    // println!("Enter probability of generating a nonzero for each entry above the diagonal:");
    // let mut line                =   String::new();    
    // std::io::stdin().read_line(&mut line).expect("Failed to read input");
    // let edge_probability        =   line.trim().parse().unwrap();
    // println!("You entered {:?}", edge_probability );


    let numrows                     =   50;
    let edge_probability            =   0.5;





    // let mut args: Vec<_>        =   std::env::args().collect();
    // let numrows                 =   & args[1];
    // let numrows                 =   numrows.parse::<usize>().unwrap();

    //  ERDOS-RENYI EXAMPLE
    //  =======================================================    
    use rand::Rng;
 
    // parameters to generate the matrix
    // let edge_probability        =   0.06;
    let lower                   =   false;
    let diag                    =   true;
    let simple                  =   false; // refers to the type of random matrix we generate
    let mut seed: u64;           //=   rand::thread_rng().gen();   <-- could try replacing with this to get randomly generated random seed ... but don't think we want that
    // let mut times               =   Vec::new(); // each entry in this vec will store the run times for one execution of matrix perm

    // the world
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();

    // run the tests
    // for numrows in (40 .. 41).step_by(5) 
    {

        println!("numrows = {:}", numrows);

        for _ in 0 .. 10 {

            seed = numrows as u64;
            // randomly generate a sparse matrix and permutation    
            let mut matrix_bale = bale::SparseMat::erdos_renyi_tri(numrows,edge_probability,lower,diag,seed,); 
    
            while   matrix_bale.nonzero.len() == 0 
                    || 
                    matrix_bale.rowcounts().min().unwrap()==0
                    ||
                    matrix_bale.colcounts().iter().cloned().min().unwrap()==0        
            { // re-generate the matrix until it has at least one structural nonzero
                seed += 1;
                matrix_bale = bale::SparseMat::erdos_renyi_tri(numrows,edge_probability,lower,diag,seed,); 
            }       
    
            // test the lamellar implementation of matrix perm
            let verbose = false;
            let verbose_debug = false;
    
            // let measurements = test_permutation( &world, & matrix_bale, &mut rperminv_bale, &mut cperminv_bale, verbose, verbose_debug, );        
            // matrix_bale.transpose();
            test_toposort( &world, &matrix_bale, verbose, verbose_debug );
            // times.push( measurements );
            
            world.wait_all();
            world.barrier();
            println!("====================================================");
        }                

        }



    // if my_pe == 0 {
    //     let table = Table::new(times.clone()).to_string();
    //     println!("{}",table.to_string());
    //     println!("Matrix transpose benchmarks complete.");        
    // }

}     


// +--------+---------+---------+-------+-----------------+----------------------+
// | numpes | numrows | numcols | nnz   | lamellar_serial | lamellar_distributed |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 100     | 100     | 500   | E-6 9.62        | E0 1.58              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 200     | 200     | 1998  | E-5 3.75        | E0 4.77              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 300     | 300     | 4574  | E-5 5.38        | E1 1.09              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 400     | 400     | 8050  | E-5 8.63        | E1 2.01              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 500     | 500     | 12620 | E-4 1.29        | E1 2.57              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 600     | 600     | 18008 | E-4 1.71        | E1 3.75              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 700     | 700     | 24562 | E-4 2.28        | E1 5.04              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 800     | 800     | 31798 | E-4 2.79        | E1 8.07              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 900     | 900     | 40622 | E-4 3.49        | E1 8.62              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 1      | 1000    | 1000    | 49416 | E-4 3.98        | E2 1.14              |
// +--------+---------+---------+-------+-----------------+----------------------+

// +--------+---------+---------+-------+-----------------+----------------------+
// | numpes | numrows | numcols | nnz   | lamellar_serial | lamellar_distributed |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 100     | 100     | 500   | E-5 1.14        | E-1 5.98             |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 200     | 200     | 1998  | E-5 3.15        | E0 1.41              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 300     | 300     | 4574  | E-5 5.60        | E0 2.81              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 400     | 400     | 8050  | E-5 8.80        | E0 5.49              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 500     | 500     | 12620 | E-4 2.45        | E0 8.15              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 600     | 600     | 18008 | E-4 1.64        | E1 1.14              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 700     | 700     | 24562 | E-4 2.30        | E1 1.50              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 800     | 800     | 31798 | E-4 2.73        | E1 1.94              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 900     | 900     | 40622 | E-4 3.41        | E1 2.54              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 2      | 1000    | 1000    | 49416 | E-4 3.92        | E1 2.89              |
// +--------+---------+---------+-------+-----------------+----------------------+

// +--------+---------+---------+-------+-----------------+----------------------+
// | numpes | numrows | numcols | nnz   | lamellar_serial | lamellar_distributed |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 100     | 100     | 500   | E-6 9.75        | E-1 1.49             |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 200     | 200     | 1998  | E-5 2.85        | E-1 3.23             |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 300     | 300     | 4574  | E-5 5.42        | E-1 6.61             |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 400     | 400     | 8050  | E-5 8.53        | E0 1.10              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 500     | 500     | 12620 | E-4 1.23        | E0 1.66              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 600     | 600     | 18008 | E-4 1.62        | E0 2.30              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 700     | 700     | 24562 | E-4 2.50        | E0 3.29              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 800     | 800     | 31798 | E-4 2.72        | E0 4.01              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 900     | 900     | 40622 | E-4 3.42        | E0 5.00              |
// +--------+---------+---------+-------+-----------------+----------------------+
// | 10     | 1000    | 1000    | 49416 | E-4 4.00        | E0 6.37              |
// +--------+---------+---------+-------+-----------------+----------------------+