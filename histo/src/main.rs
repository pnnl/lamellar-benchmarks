mod active_message;
mod array;
mod options;

use array::{ArrayDistribution, ArrayType};
use options::{HistoCli, IndexSize};

use clap::{Parser, ValueEnum};
use rand::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// #[global_allocator]
// static ALLOC: dhat::Alloc = dhat::Alloc;
use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(ValueEnum, Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum Variant {
    UnsafeAM,
    SafeAm,
    UnsafeBufferedAm,
    SafeBufferedAm,
    UnsafeAmGroup,
    SafeAmGroup,
    UnsafeArray,
    AtomicArray,
    LocalLockArray,
}

fn print_am_times(
    cli: &HistoCli,
    my_pe: usize,
    num_pes: usize,
    variant: &Variant,
    idx_size: &IndexSize,
    times: (Duration, Duration, Duration, Duration),
) {
    if my_pe == 0 {
        let payload_size = match idx_size {
            IndexSize::Usize => std::mem::size_of::<usize>() + std::mem::size_of::<usize>(),
            IndexSize::U32 => std::mem::size_of::<usize>() + std::mem::size_of::<u32>(),
        };
        let l_num_updates = cli.pe_updates(num_pes);
        let g_num_updates = cli.total_updates(num_pes);
        println!(
            "{} {} lmups {:>9.2?}, gmups {:>9.2?}, lGB/s {:>7.2?}, gGB/s {:>7.2?}, {times:>6.4?}",
            format!("{:<1$}", format!("{variant:?}"), cli.max_variant_len()),
            format!(
                "{:<1$}",
                format!("{idx_size:?}"),
                cli.max_index_size_len() + cli.max_array_distribution_len()
            ),
            (l_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            (g_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            ((l_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32(),
            ((g_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32(),
        );
    }
}

fn print_array_times(
    cli: &HistoCli,
    my_pe: usize,
    num_pes: usize,
    variant: &Variant,
    distribution: &ArrayDistribution,
    times: (Duration, Duration, Duration, Duration),
) {
    if my_pe == 0 {
        // alculate the size of indices used by the lamellar array,
        // which is based on the number of elements on each PE (not the total table size)
        let pe_table_size = cli.pe_table_size(num_pes);
        let index_size = usize::BITS as usize - pe_table_size.leading_zeros() as usize;
        let (index_size, index_name) = if index_size > 32 {
            (std::mem::size_of::<usize>(), "usize")
        } else if index_size > 16 {
            (std::mem::size_of::<u32>(), "u32")
        } else if index_size > 8 {
            (std::mem::size_of::<u16>(), "u16")
        } else {
            (std::mem::size_of::<u8>(), "u8")
        };
        let payload_size = std::mem::size_of::<usize>() + index_size;
        let l_num_updates = cli.pe_updates(num_pes);
        let g_num_updates = cli.total_updates(num_pes);

        println!(
            "{} {} lmups {:>9.2?}, gmups {:>9.2?}, lGB/s {:>7.2?}, gGB/s {:>7.2?}, {times:>6.4?}",
            format!("{:<1$}", format!("{variant:?}"), cli.max_variant_len()),
            format!(
                "{:<1$}",
                format!("{distribution:?} {index_name}"),
                cli.max_index_size_len() + cli.max_array_distribution_len()
            ),
            (l_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            (g_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            ((l_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32(),
            ((g_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32(),
        );
    }
}

fn print_results(
    cli: &HistoCli,
    my_pe: usize,
    num_pes: usize,
    variant: &str,
    sub_variant: &str,
    times: &[(Duration, Duration, Duration, Duration)],
) {
    let g_num_updates = cli.total_updates(num_pes) as f32 / 1_000_000.0;
    if my_pe == 0 {
        let gups = times
            .iter()
            .map(|t| g_num_updates / t.3.as_secs_f32())
            .collect::<Vec<f32>>();
        println!(
            "summary: {variant} {sub_variant} {gups:>6.2?} min: {:>6.2} avg: {:>6.2} max: {:>6.2}",
            gups.iter().fold(f32::INFINITY, |a, &b| a.min(b)),
            gups.iter().sum::<f32>() / gups.len() as f32,
            gups.iter().fold(f32::NEG_INFINITY, |a, &b| a.max(b))
        );
    }
}

fn main() {
    // let _profiler = dhat::Profiler::new_heap();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cli = options::HistoCli::parse();

    let global_count = cli.total_table_size(num_pes);
    let l_num_updates = cli.pe_updates(num_pes);
    let iterations = cli.iterations;

    if my_pe == 0 {
        cli.describe(num_pes);
    }

    let variants = match &cli.variants {
        Some(v) => v.clone(),
        None => vec![
            Variant::UnsafeAM,
            Variant::SafeAm,
            Variant::UnsafeBufferedAm,
            Variant::SafeBufferedAm,
            Variant::UnsafeAmGroup,
            Variant::SafeAmGroup,
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

    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    let mut results = HashMap::new();

    for variant in variants {
        let variant_results = results.entry(variant).or_insert(HashMap::new());

        for _i in 0..iterations {
            // create new random indicies for each iteration
            let rand_index = Arc::new(
                (0..l_num_updates)
                    .into_iter()
                    .map(|_| rng.gen_range(0, global_count))
                    .collect::<Vec<usize>>(),
            );

            match variant {
                Variant::UnsafeAM => {
                    for idx_size in &am_index_size {
                        let times =
                            active_message::am::histo(&world, &cli, &rand_index, false, idx_size);
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::SafeAm => {
                    for idx_size in &am_index_size {
                        let times =
                            active_message::am::histo(&world, &cli, &rand_index, true, idx_size);
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::UnsafeBufferedAm => {
                    for idx_size in &am_index_size {
                        let times = active_message::buffered_am::histo(
                            &world,
                            &cli,
                            &rand_index,
                            false,
                            idx_size,
                        );
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::SafeBufferedAm => {
                    for idx_size in &am_index_size {
                        let times = active_message::buffered_am::histo(
                            &world,
                            &cli,
                            &rand_index,
                            true,
                            idx_size,
                        );
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::UnsafeAmGroup => {
                    for idx_size in &am_index_size {
                        let times = active_message::am_group::histo(
                            &world,
                            &cli,
                            &rand_index,
                            false,
                            idx_size,
                        );
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::SafeAmGroup => {
                    for idx_size in &am_index_size {
                        let times = active_message::am_group::histo(
                            &world,
                            &cli,
                            &rand_index,
                            true,
                            idx_size,
                        );
                        variant_results
                            .entry(format!("{idx_size:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_am_times(&cli, my_pe, num_pes, &variant, &idx_size, times);
                    }
                }
                Variant::UnsafeArray => {
                    for distribution in &array_distribution {
                        let times = array::histo(
                            &world,
                            &cli,
                            &rand_index,
                            ArrayType::Unsafe,
                            distribution,
                        );
                        variant_results
                            .entry(format!("{distribution:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
                Variant::AtomicArray => {
                    for distribution in &array_distribution {
                        let times = array::histo(
                            &world,
                            &cli,
                            &rand_index,
                            ArrayType::Atomic,
                            distribution,
                        );
                        variant_results
                            .entry(format!("{distribution:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
                Variant::LocalLockArray => {
                    for distribution in &array_distribution {
                        let times = array::histo(
                            &world,
                            &cli,
                            &rand_index,
                            ArrayType::LocalLock,
                            distribution,
                        );
                        variant_results
                            .entry(format!("{distribution:?}"))
                            .or_insert(Vec::new())
                            .push(times.clone());
                        print_array_times(&cli, my_pe, num_pes, &variant, &distribution, times);
                    }
                }
            }
        }
    }

    for (variant, variant_results) in results {
        for (sub_variant, times) in variant_results {
            print_results(
                &cli,
                my_pe,
                num_pes,
                &format!("{:<1$}", format!("{variant:?}"), cli.max_variant_len()),
                &format!(
                    "{:<1$}",
                    format!("{sub_variant}"),
                    cli.max_index_size_len() + cli.max_array_distribution_len()
                ),
                &times,
            )
        }
    }
}
