

# for VARPAIR in (1,32) (1,64) (4,64) (8,64) (16,64) (32,64)
# do
#     RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=120 LAMELLAR_THREADS=1 srun --cpus-per-task=2 --cpu-bind=ldoms,v  -N 1 --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive ./target/release/matrix_perm
# done


declare -a elems=(
    "1 32"
    "1 64"
    "4 64"
    "8 64"
    "16 64"
    # "32 64"
)

for elem in "${elems[@]}"; do
    read -a strarr <<< "$elem"  # uses default whitespace IFS
    # nnodes = ${strarr[0]}
    # nthreads = ${strarr[1]}
    # $((num1 * num2))
    RUST_LIB_BACKTRACE=1 RUST_BACKTRACE=full LAMELLAR_DEADLOCK_TIMEOUT=120 LAMELLAR_THREADS=${strarr[1]} srun --cpus-per-task=${strarr[1]} --cpu-bind=ldoms,v  -N ${strarr[0]} --ntasks-per-node=1 -A lamellar --mpi=pmi2 --exclusive ./target/release/matrix_transpose_writestats
    # echo ${strarr[0]} ${strarr[1]} ${strarr[2]}
done