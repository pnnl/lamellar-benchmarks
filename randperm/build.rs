use std::env;

fn main() {
    // Environment variables can only be *computed* in a build.rs.  
    //TODO:
    // Make a function benchmark_record that grabs a specific set:
    //    - features
    //    - git hash
    //    - All git files checked in true/false
    //    - Target platform information
    // Make a procedural macro in benchmark_record for use in the benchmark code to record those values in the benchmark binary.

    let features = env::var("CARGO_CFG_FEATURE").unwrap_or_default();
    println!("cargo::rustc-env=BENCHMARK_CONFIG_VALUES={}", features);
    
    // for (var, val) in env::vars() {
    //     print!("cargo:warning={}={}\n", var, val);
    // }
}

 