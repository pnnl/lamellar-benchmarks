mod active_message;
mod array;
mod options;

use array::{ArrayType,ArrayDistribution};
use options::{HistoCli,IndexSize};


use clap::{Parser,ValueEnum};
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(ValueEnum, Debug,Clone,Copy)]
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


fn print_am_times(cli: &HistoCli,my_pe: usize, num_pes: usize, variant: &Variant, idx_size: &IndexSize, times: (Duration, Duration, Duration, Duration)){
    if my_pe == 0 {
        let payload_size= match idx_size {
            IndexSize::Usize => std::mem::size_of::<usize>() + std::mem::size_of::<usize>(),
            IndexSize::U32 => std::mem::size_of::<usize>() + std::mem::size_of::<u32>(),   
        };
        let l_num_updates = cli.pe_updates(num_pes);
        let g_num_updates = cli.total_updates(num_pes);
        println!( 
            "{variant:?} {idx_size:?} {times:?} lmups {:?} gmups {:?} lGB/s {:?} gGB/s {:?}",
            (l_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            (g_num_updates as f32 / 1_000_000.0)  / times.3.as_secs_f32(),
            ((l_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32() ,
            ((g_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32() ,
        );
    }
}

fn print_array_times(cli: &HistoCli,my_pe: usize, num_pes: usize, variant: &Variant, distribution: &ArrayDistribution, times: (Duration, Duration, Duration, Duration)){
    if my_pe ==0{
        // alculate the size of indices used by the lamellar array,
        // which is based on the number of elements on each PE (not the total table size)
        let pe_table_size = cli.pe_table_size(num_pes);
        let index_size = usize::BITS as usize - pe_table_size.leading_zeros() as usize;
        let index_size = if index_size > 32 { 64 } else if index_size > 16 { 32 } else if index_size > 8 { 16 } else { 8 };
        let payload_size = std::mem::size_of::<usize>() + index_size; 
        let l_num_updates = cli.pe_updates(num_pes);
        let g_num_updates = cli.total_updates(num_pes);
        println!( 
            "{variant:?} {distribution:?} {index_size:?} bit indices {times:?} lmups {:?} gmups {:?} lGB/s {:?} gGB/s {:?}",
            (l_num_updates as f32 / 1_000_000.0) / times.3.as_secs_f32(),
            (g_num_updates as f32 / 1_000_000.0)  / times.3.as_secs_f32(),
            ((l_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32() ,
            ((g_num_updates * payload_size) as f32 / 1_000_000_000.0) / times.3.as_secs_f32() ,
        );
    }

}

fn main() {
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

    let  variants = match &cli.variants{
        Some(v) => v.clone(),
        None => vec![Variant::UnsafeAM,Variant::SafeAm,Variant::UnsafeBufferedAm,Variant::SafeBufferedAm,Variant::UnsafeAmGroup,Variant::SafeAmGroup,Variant::UnsafeArray, Variant::AtomicArray, Variant::LocalLockArray],
    };

    let am_index_size = match &cli.am_index_size {
        Some(v) => v.clone(),
        None => vec![IndexSize::U32,IndexSize::Usize],
    };

    let array_distribution = match &cli.array_distribution {
        Some(v) => v.clone(),
        None => vec![ArrayDistribution::Block,ArrayDistribution::Cyclic],
    }; 


    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    // for index_size in [IndexSize::U32, IndexSize::Usize] {
    //     for safe in [false, true] {
    for variant in variants {
        for _i in 0..iterations {
            // create new random indicies for each iteration
            let rand_index = Arc::new(
                (0..l_num_updates)
                    .into_iter()
                    .map(|_| rng.gen_range(0, global_count))
                    .collect::<Vec<usize>>(),
            );

            match variant{
                Variant::UnsafeAM => {
                    for idx_size in &am_index_size{
                        let times  = active_message::am::histo(&world, &cli, &rand_index,false, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                }
                Variant::SafeAm => {
                    for idx_size in &am_index_size{
                        let times  = active_message::am::histo(&world, &cli, &rand_index,true, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                    
                }
                Variant::UnsafeBufferedAm => {
                    for idx_size in &am_index_size{
                        let times  = active_message::buffered_am::histo(&world, &cli, &rand_index,false, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                    
                }
                Variant::SafeBufferedAm => {
                    for idx_size in &am_index_size{
                        let times  = active_message::buffered_am::histo(&world, &cli, &rand_index,true, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                    
                }
                Variant::UnsafeAmGroup => {
                    for idx_size in &am_index_size{
                        let times  = active_message::am_group::histo(&world, &cli, &rand_index,false, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                    
                }
                Variant::SafeAmGroup => {
                    for idx_size in &am_index_size{
                        let times  = active_message::am_group::histo(&world, &cli, &rand_index,true, idx_size) ;
                        print_am_times(&cli,my_pe,num_pes,&variant,&idx_size,times);
                    }
                    
                }
                Variant::UnsafeArray => {
                    for distribution in &array_distribution{
                        let times = array::histo(&world, &cli, &rand_index, ArrayType::Unsafe, distribution);
                        print_array_times(&cli,my_pe,num_pes,&variant,&distribution,times);
                    }
                }
                Variant::AtomicArray => {
                    for distribution in &array_distribution{
                        let times = array::histo(&world, &cli, &rand_index, ArrayType::Atomic, distribution);
                        print_array_times(&cli,my_pe,num_pes,&variant,&distribution,times);
                    }
                }
                Variant::LocalLockArray => {
                    for distribution in &array_distribution{
                        let times = array::histo(&world, &cli, &rand_index, ArrayType::LocalLock, distribution);
                        print_array_times(&cli,my_pe,num_pes,&variant,&distribution,times);
                    }
                }
            }
        }
    }
}
