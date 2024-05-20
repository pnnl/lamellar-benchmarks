/// This is a script to check that the Delta stepping and Belman-Ford algorithms
/// return the same results. It is very "bare bones."  To run the test, first run
/// each of the following in the command line:
///
/// ```
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --delta 0.3 --write-to-json
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_delta_step_semidistributed --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --delta 0.3 --write-to-json
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_bellman_ford --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --write-to-json
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_bellman_ford_irredundant_search --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --write-to-json
/// ```
///
/// Then run
///
/// ```
/// /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_unit_test
/// ```


use std::process::{Command, Output};
use serde_json::Value;
use std::fs;

fn main() {


    // check that input matrix is the same
    let matrix_a = read_json_file("sssp_unit_test_matrix_bellman_ford.json");
    let matrix_b = read_json_file("sssp_unit_test_matrix_bellman_ford_irredundant_search.json");
    let matrix_c = read_json_file("sssp_unit_test_matrix_delta_step.json"); 

    if matrix_a != matrix_b {
        println!("Input weights do not match: bellman_ford != bellman_ford_irredundant_search");
    } else if matrix_a != matrix_c {
        println!("Input weights do not match: bellman_ford != delta_step");
    } else {
        println!("Input matrices match!");        
    }    


    // check that input edge weight is the same
    let weight_a = read_json_file("sssp_unit_test_weight_bellman_ford.json");
    let weight_b = read_json_file("sssp_unit_test_weight_bellman_ford_irredundant_search.json");
    let weight_c = read_json_file("sssp_unit_test_weight_delta_step.json"); 

    if weight_a != weight_b {
        println!("Input weights do not match: bellman_ford != bellman_ford_irredundant_search");
    } else if weight_a != weight_c {
        println!("Input weights do not match: bellman_ford != delta_step");
    } else {
        println!("Input weights match!");        
    }        


    // Compare the contents of the JSON files
    let result_a = read_json_file("sssp_unit_test_data_bellman_ford.json");
    let result_b = read_json_file("sssp_unit_test_data_bellman_ford_irredundant_search.json");
    let result_c = read_json_file("sssp_unit_test_data_delta_step.json");    

    // Compare results
    if result_a != result_b {
        println!("Results do not match: bellman_ford != bellman_ford_irredundant_search");
    } else if result_a != result_c {
        println!("Results do not match: bellman_ford != delta_step");
    } else {
        println!("Results match!");        
    }
}


fn read_json_file(filename: &str) -> Value {
    let contents = fs::read_to_string(filename).expect("Unable to read file");
    serde_json::from_str(&contents).expect("Unable to parse JSON")
}
