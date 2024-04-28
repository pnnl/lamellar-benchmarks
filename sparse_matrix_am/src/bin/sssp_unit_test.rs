/// This is a script to check that the Delta stepping and Belman-Ford algorithms
/// return the same results. It is very "bare bones."  To run the test, first run
/// each of the following in the command line:
///
/// ```
/// RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=1 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive /people/roek189/learning_lamellar/_lamellar-benchmarks/target/release/sssp_delta_step --rows-per-thread-per-pe 10 --avg-nnz-per-row 8 --random-seed 0 --delta 0.3 --write-to-json
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

    // Compare the contents of the JSON files
    let result_a = read_json_file("sssp_unit_test_data_delta_step.json");
    let result_b = read_json_file("sssp_unit_test_data_bellman_ford_.json");

    // Compare results
    if result_a == result_b {
        println!("Results match!");
    } else {
        println!("Results do not match!");
    }
}


fn read_json_file(filename: &str) -> Value {
    let contents = fs::read_to_string(filename).expect("Unable to read file");
    serde_json::from_str(&contents).expect("Unable to parse JSON")
}
