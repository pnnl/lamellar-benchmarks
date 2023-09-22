# before running

`. lamellar-prep.rc` (to configure)

# to run an example on lamellar-runtim

cargo run --example <examplefiletorun>

# guidance

- always make sure LAMELLAR_THREADS is less than or equal to cpus-per-task (ideally equal)

# commands

RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=10 LAMELLAR_THREADS=1 srun --cpus-per-task=2 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=2 -A lamellar --mpi=pmi2 --exclusive ./target/release/matrix_perm
