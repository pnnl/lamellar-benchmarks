/// This is a script to check that the Delta stepping and Belman-Ford algorithms
/// return the same results. It is very "bare bones."  To run the test, first run
/// each of the following in the command line:
///
/// For a single PE:
///
/// ```
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_bellman_ford --rows-per-thread-per-pe 10 --avg-nnz-per-row 4 --random-seed 0 --graph-type random --write-to-json --debug
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_bellman_ford_irredundant_search --rows-per-thread-per-pe 10 --avg-nnz-per-row 4 --random-seed 0 --graph-type random --write-to-json --debug
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_bellman_ford_serial --rows-per-thread-per-pe 10 --avg-nnz-per-row 4 --random-seed 0 --graph-type random --write-to-json --debug
/// ```
///
/// For multiple PEs:
///
/// ```
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=4 srun --cpus-per-task=4 --cpu-bind=ldoms,v  -N 4 --ntasks-per-node=16 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_bellman_ford_irredundant_search --rows-per-thread-per-pe 10 --avg-nnz-per-row 4 --random-seed 0 --graph-type random --write-to-json --debug
/// ```
///
/// Then CD into sparse_matrix_am, and run the following
///
/// ```
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/lamellar_benchmarks_repo/target/release/sssp_unit_test
/// ```


use std::process::{Command, Output};
use serde_json::Value;
use std::fs;

fn main() {


    // check that input matrix is the same
    let matrix_a = read_json_file("sssp_unit_test_matrix_bellman_ford.json");
    let matrix_b = read_json_file("sssp_unit_test_matrix_bellman_ford_irredundant_search.json");
    let matrix_c = read_json_file("sssp_unit_test_matrix_bellman_ford_serial.json"); 

    if matrix_a != matrix_b {
        println!("Input matrices do not match: bellman_ford != bellman_ford_irredundant_search");
    }
    
    if matrix_a != matrix_c {
        println!("Input matrices do not match: bellman_ford != bellman_ford_serial");
    } 
    
    if (matrix_a==matrix_b) && (matrix_a==matrix_c) {
        println!("Input matrices match!");        
    }    


    // check that input edge weight is the same
    let weight_a = read_json_file("sssp_unit_test_weight_bellman_ford.json");
    let weight_b = read_json_file("sssp_unit_test_weight_bellman_ford_irredundant_search.json");
    let weight_c = read_json_file("sssp_unit_test_weight_bellman_ford_serial.json"); 

    if weight_a != weight_b {
        println!("Input weights do not match: bellman_ford != bellman_ford_irredundant_search");
    }
    if weight_a != weight_c {
        println!("Input weights do not match: bellman_ford != bellman_ford_serial");
    }  
    
    if (weight_a == weight_b) && (weight_a == weight_c) {
        println!("Input weights match!");        
    }        


    // Compare the contents of the JSON files
    let result_a = read_json_file("sssp_unit_test_data_bellman_ford.json");
    let result_b = read_json_file("sssp_unit_test_data_bellman_ford_irredundant_search.json");
    let result_c = read_json_file("sssp_unit_test_data_bellman_ford_serial.json");    

    // Compare results
    if result_a != result_b {
        println!("Results do not match: bellman_ford != bellman_ford_irredundant_search");
    }
    if result_a != result_c {
        println!("Results do not match: bellman_ford != belllman_ford_serial");
    } 
    if ( result_a == result_b ) && ( result_a == result_c )
    {
        println!("Results match!");        
    }
}


fn read_json_file(filename: &str) -> Value {
    let contents = fs::read_to_string(filename).expect("Unable to read file");
    serde_json::from_str(&contents).expect("Unable to parse JSON")
}
