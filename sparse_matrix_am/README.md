# Lamellar library for sparse matrices

This project contains two directories of note

- `data_structures` defines general data structures and operations
- `src` contains files for running benchmarks on these data structures

## Matrix permutation

### Example

- log into junction
- run `. lamellar-prep.rc`
- cd into `lamellar_benchmarks/sparse_matrix_distarr`
- `cargo build --release`
- in the following command, ensure that cpus per task * tasks per node ≥ num threads
- `RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=120 LAMELLAR_THREADS=1 srun --cpus-per-task=2 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive ./target/release/matrix_perm`

### Relevant files are

`sparse_matrix_distarr/data_structures/distributed`

- defines SparseMat data structure
- defines `test_permutation` which
  - accepts the data primitives of a bale sparse matrix and bale permutation as input
  - converts the bale data structure to a lamellar data structure, and performs matrix permutation via lamellar
  - (for sanity) also converts the bale data structure to a naive vec-of-rowvec structure, and applies a naive permutation method
  - check that all three permutation methods agree

`src/bin/matrix_perm`

- generates a sparse matrix and row/column permutations from a random seed
- applies the test_permutation function described above, to permute the matrix and check for correctness

### To do

- check for performance differences between cyclic versus block array structure
- look for batching as a way to improve performance
- in `matrix_perm.rx` we require that a matrix not only be nonzero, but that every *row* is nonzero.  And we easily get an error when we drop this requirement for rows.  Investigate where this error comes from.