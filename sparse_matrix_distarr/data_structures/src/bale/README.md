
# Contents 

The contents of this directory, `sparse_matrix/data_structures/bale` come from the [bale library](https://github.com/jdevinney/bale/blob/master/src/other_serial/Rust/sparsemat/src/lib.rs).  See the enclosed `LICENSE` file for licensing information.

# Use

The Lamellar developers wished to benchmark several tasks against the Bale serial Rust implementation as accuractely as possible.  However, certain features of the Bale serial Rust library made this difficult or impossible:

- generation of random matrices / vectors with random seeds is not possible (the feature is not implemented though some functions accept a random seed as input)
- certain data structures cannot be modified or updated

For this reason we have made minimal changes to the Bale library, without modifying essential algorithms such as matrix permutation.