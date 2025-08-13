Randperm benchmark for the Lamellar Runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

Am implementation of the Randperm benchmark (https://github.com/jdevinney/bale/blob/master/src/bale_classic/apps/randperm_src/README.md) implemented in Rust for the Lamellar runtime


BUILD REQUIREMENTS
------------------
These benchmarks requires the following dependencies:

* [Lamellar](https://github.com/pnnl/lamellar-runtime) - now on [crates.io](https://crates.io/crates/lamellar)

* Crates listed in Cargo.toml

The OFI_DIR environment variable must be specified with the location of the OFI installation.
The ROFI_DIR environment variable must be specified with the location of the ROFI installation.

BUILDING PACKAGE
----------------
In the following, assume a root directory ${ROOT}
0. download Benchmarks to ${ROOT}/Benchmarks 
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-benchmarks`
0. download Lamellar to ${ROOT}/lamellar-runtime  -- or update Cargo.toml to point to the proper location
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`



1. cd into registered-am or remote-closure directory and Compile benchmark 

`cargo build (--release)`


TESTING
-------
The benchmarks are designed to be run with on multiple compute nodes (1 node is valid). Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentaiton for details on how to run command on different clusters. Lamellar grabs job information (size, distribution, etc.) from the jbo manager and runtime launcher (e.g., MPI, please refer to the BUILING REQUIREMENTS section for a list of tested software versions).

To run the benchmark through the slurm queue, first compile with `cargo build --release` then run the following:
- `srun -N 2 target/release/randperm`

*Note:* If using the "local" lamellae, simply execute the binary directly


HISTORY
-------
- version 0.7:
  - Async initialization
- version 0.1:
  - initial implementation
  
NOTES
-----

STATUS
------
Lamellar is still under development, thus not all intended features are yet
implemented. Benchmark will be updated to utilize new features.

CONTACTS
--------
Ryan Friese     - ryan.friese@pnnl.gov  

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
