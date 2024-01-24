mod active_message;
mod graph;
mod options;

use graph::{Graph, GraphType};
use options::TcCli;

use clap::{Parser, ValueEnum};
use std::time::Duration;

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Variant {
    Buffered,
    AmGroup,
    Single,
}

fn print_am_times(
    my_pe: usize,
    variant: &Variant,
    buf_size: usize,
    times: (Duration, Duration, Duration),
) {
    if my_pe == 0 {
        println!("{variant:?} buf_size: {buf_size:?} {times:?} ",);
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let cli = TcCli::parse();

    let iterations = cli.iterations;

    if my_pe == 0 {
        cli.describe();
    }

    let file = &cli.graph_file;
    let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());

    let variants = match &cli.variants {
        Some(v) => v.clone(),
        None => vec![Variant::Buffered, Variant::AmGroup, Variant::Single],
    };

    for variant in variants {
        for _i in 0..iterations {
            match variant {
                Variant::Buffered => {
                    for buf_size in &cli.buffer_size {
                        let times = active_message::buffered::triangle_count(
                            &world, &cli, &graph, *buf_size,
                        );
                        print_am_times(my_pe, &variant, *buf_size, times);
                    }
                }
                Variant::AmGroup => {
                    for buf_size in &cli.buffer_size {
                        let times = active_message::am_group::triangle_count(
                            &world, &cli, &graph, *buf_size,
                        );
                        print_am_times(my_pe, &variant, *buf_size, times);
                    }
                }
                Variant::Single => {
                    let times = active_message::single::triangle_count(&world, &cli, &graph);
                    print_am_times(my_pe, &variant, 0, times);
                }
            }
        }
    }
}
