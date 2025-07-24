HPCG benchmark for the Lamellar Runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

This benchmark is based on https://github.com/hpcg-benchmark/hpcg

TESTING
-------

The benchmarks are designed to be run with on multiple compute nodes (1 node is valid). Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentaiton for details on how to run command on different clusters. Lamellar grabs job information (size, distribution, etc.) from the jbo manager and runtime launcher (e.g., MPI, please refer to the BUILING REQUIREMENTS section for a list of tested software versions).

Before running any benchmarks, unzip the data file `./input_graphs/graph500-scale18-ef16_adj.tsv.tar.gz`

To run the benchmark through the slurm queue, first compile with `cargo build --release` then run one of the following:
- `srun -N 2 target/release/dot_product`


