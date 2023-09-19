use lamellar::array::prelude::*;
use rand::Rng; //a crate for random number generation
use std::time::Instant;
const T_LEN: usize = 1_000_000; //global len
const L_UPDATES: usize = 100_000_000; //updates per pe
fn main() {
    let world = LamellarWorldBuilder::new().build();
    let table = LocalLockArray::<usize>::new(&world, T_LEN, Distribution::Block);
    let mut rng = rand::thread_rng();
    let rnd_i = (0..L_UPDATES)
        .map(|_| rng.gen_range(0, T_LEN))
        .collect::<Vec<_>>();
    world.barrier();
    let timer = Instant::now();
    world.block_on(table.batch_add(rnd_i, 1));
    world.barrier();
    println!("Elapsed time: {:?}", timer.elapsed());
    let sum = world.block_on(table.sum());
    assert_eq!(sum, L_UPDATES * world.num_pes());
}
