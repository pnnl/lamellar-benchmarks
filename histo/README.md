Histo benchmark for the Lamellar Runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

Am implementation of the Histo benchmark (https://github.com/jdevinney/bale/tree/master/apps/histo_src) implemented in Rust for the Lamellar runtime

NEWS
----

Sept 2020: Update for Lamellar 0.2.1 release
July 2020: Second alpha release
Feb 2020: First alpha release

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

1. download Benchmarks to ${ROOT}/Benchmarks 
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-benchmarks`
2. download Lamellar to ${ROOT}/lamellar-runtime  -- or update Cargo.toml to point to the proper location
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`

3. cd into registered-am or remote-closure directory and Compile benchmark 

`cargo build (--release)`

    executables located at ./target/debug(release)/<benchmark variant>

    where `<benchmark variant>` in {`histo_dma, histo_static, histo_buffered_updates_dma, histo_buffered_updates_static`}.

OPTIONS
-------

For use with distributed HPC systems, lamellar installation may require additional steps.  See the Lamellar [documentation](https://github.com/pnnl/lamellar-runtime#using-lamellar) for details.

The user may also set the number of worker threads via a environmental variable.  See the Lamellare [documentation](https://github.com/pnnl/lamellar-runtime#environment-variables) for details.

TESTING
-------

The benchmarks are designed to be run with on multiple compute nodes (1 node is valid). Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentaiton for details on how to run command on different clusters. Lamellar grabs job information (size, distribution, etc.) from the jbo manager and runtime launcher (e.g., MPI, please refer to the BUILING REQUIREMENTS section for a list of tested software versions).

To run the benchmark through the slurm queue, first compile with `cargo build --release` then run one of the following:
- `srun -N 2 target/release/histo_buffered_safe_am`
- `srun -N 2 target/release/histo_buffered_unsafe_am`
- `srun -N 2 target/release/histo_darc`
- `srun -N 2 target/release/histo_lamellar_array_comparison`
- `srun -N 2 target/release/histo_safe_am`
- `srun -N 2 target/release/histo_unsafe_am`

*Note:* If using the "local" lamellae, simply execute the binary directly

HISTORY
-------
- version 0.7:
 - Update to match Lamellar 0.7.1 api
- version 0.2:
  - update to match Lamellar v0.2 api
  - implement registered AM version
- version 0.1:
  - initial implementations
  - use static array
  - use remote memory region
  
NOTES
-----

STATUS
------

Lamellar is still under development, thus not all intended features are yet
implemented. Benchmark will be updated to utilize new features.

CONTACTS
--------

Ryan Friese     - ryan.friese@pnnl.gov  
Roberto Gioiosa - roberto.gioiosa@pnnl.gov  
Mark Raugas     - mark.raugas@pnnl.gov  

License
-------

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details.

Acknowledgments
---------------

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
