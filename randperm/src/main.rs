mod active_message;
mod array;
mod options;

use array::{ArrayDistribution, ArrayType};
use options::{IndexSize, RandPermCli};

use clap::{Parser, ValueEnum};
use std::time::Duration;

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Variant {
    CasDart,
    CasDartRemote,
    PushDart,
    CasDartGroup,
    CasDartGroupRemote,
    PushDartGroup,
    BufferedCasDart,
    BufferedCasDartRemote,
    BufferedPushDart,
    UnsafeArray,
    AtomicArray,
    LocalLockArray,
}

fn print_am_times(
    my_pe: usize,
    variant: &Variant,
    idx_size: &IndexSize,
    times: (Duration, Duration, Duration),
) {
    if my_pe == 0 {
        println!("{variant:?} {idx_size:?} {times:?} ",);
    }
}

fn print_array_times(
    cli: &RandPermCli,
    my_pe: usize,
    num_pes: usize,
    variant: &Variant,
    distribution: &ArrayDistribution,
    times: (Duration, Duration, Duration),
) {
    if my_pe == 0 {
        // alculate the size of indices used by the lamellar array,
        // which is based on the number of elements on each PE (not the total table size)
        let pe_table_size = cli.pe_table_size(num_pes);
        let index_size = usize::BITS as usize - pe_table_size.leading_zeros() as usize;
        let index_size = if index_size > 32 {
            64
        } else if index_size > 16 {
            32
        } else if index_size > 8 {
            16
        } else {
            8
        };
        println!("{variant:?} {distribution:?} {index_size:?} bit indices {times:?} ",);
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        // .with_executor(lamellar::ExecutorType::LamellarWorkStealing)
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::RandPermCli::parse();

    let iterations = cli.iterations;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let variants = match &cli.variants {
        Some(v) => v.clone(),
        None => vec![
            Variant::CasDart,
            Variant::CasDartRemote,
            Variant::PushDart,
            Variant::CasDartGroup,
            Variant::CasDartGroupRemote,
            Variant::PushDartGroup,
            Variant::BufferedCasDart,
            Variant::BufferedCasDartRemote,
            Variant::BufferedPushDart,
            Variant::UnsafeArray,
            Variant::AtomicArray,
            Variant::LocalLockArray,
        ],
    };

    let am_index_size = match &cli.am_index_size {
        Some(v) => v.clone(),
        None => vec![IndexSize::U32, IndexSize::Usize],
    };

    let array_distribution = match &cli.array_distribution {
        Some(v) => v.clone(),
        None => vec![ArrayDistribution::Block, ArrayDistribution::Cyclic],
    };

    for variant in variants {
        for _i in 0..iterations {
            match variant {
                Variant::CasDart => {
                    for idx_size in &am_index_size {
                        let times =
                            active_message::single_cas_am::rand_perm(&world, &cli, idx_size);
                        print_am_times(my_pe, &variant, &idx_size, times);
                    }
                }
                Variant::CasDartRemote => {
                    let times = active_message::single_cas_am_remote::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::PushDart => {
                    let times = active_message::single_push_am::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::CasDartGroup => {
                    for idx_size in &am_index_size {
                        let times = active_message::cas_am_group::rand_perm(&world, &cli, idx_size);
                        print_am_times(my_pe, &variant, &idx_size, times);
                    }
                }
                Variant::CasDartGroupRemote => {
                    let times = active_message::cas_am_group_remote::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::PushDartGroup => {
                    let times = active_message::push_am_group::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::BufferedCasDart => {
                    for idx_size in &am_index_size {
                        let times = active_message::buffered_cas_am::rand_perm(
                            &world, &cli, false, idx_size,
                        );
                        print_am_times(my_pe, &variant, &idx_size, times);
                    }
                }
                Variant::BufferedCasDartRemote => {
                    let times = active_message::buffered_cas_am_remote::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::BufferedPushDart => {
                    let times = active_message::buffered_push_am::rand_perm(&world, &cli);
                    print_am_times(my_pe, &variant, &IndexSize::None, times);
                }
                Variant::UnsafeArray => {
                    for distribution in &array_distribution {
                        let times =
                            array::rand_perm(&world, &cli, ArrayType::Unsafe, &distribution);
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
                Variant::AtomicArray => {
                    for distribution in &array_distribution {
                        let times =
                            array::rand_perm(&world, &cli, ArrayType::Atomic, &distribution);
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
                Variant::LocalLockArray => {
                    for distribution in &array_distribution {
                        let times =
                            array::rand_perm(&world, &cli, ArrayType::LocalLock, &distribution);
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
            }
        }
    }
}
