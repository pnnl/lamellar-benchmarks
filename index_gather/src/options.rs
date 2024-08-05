use crate::array::ArrayDistribution;
use crate::Variant;
use clap::{Args, Parser, ValueEnum};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct IndexGatherCli {
    #[command(flatten)]
    pub table_size: TableSize,

    #[command(flatten)]
    updates: Updates,

    #[arg(short, long, default_value_t = 3)]
    pub iterations: usize,

    #[arg(short, long, default_value_t = 1)]
    pub launch_threads: usize,

    #[arg(short, long, env = "LAMELLAR_BATCH_OP_SIZE", default_value_t = 10000)]
    pub buffer_size: usize,

    #[arg(value_enum, long,num_args(0..))]
    pub am_index_size: Option<Vec<IndexSize>>,

    #[arg(value_enum, short, long, num_args(0..))]
    pub variants: Option<Vec<Variant>>,

    #[arg( long,num_args(0..))]
    pub array_distribution: Option<Vec<ArrayDistribution>>,
}

#[derive(Debug, Args)]
#[group(required = false, multiple = false)]
pub struct Updates {
    /// Specify the number of updates per PE
    #[arg(long, default_value_t = 1000)]
    local_updates: usize,

    /// Specify the total number of updates
    #[arg(long)]
    global_updates: Option<usize>,
}

#[derive(Debug, Args)]
#[group(required = false, multiple = false)]
pub struct TableSize {
    /// Specify the size of the table per PE
    #[arg(long, default_value_t = 1000)]
    local_size: usize,

    /// Specify the global size of the table
    #[arg(long)]
    global_size: Option<usize>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum IndexSize {
    /// 32 bit sized indices
    U32,
    /// 64 bit sized indices
    Usize,
}

impl IndexGatherCli {
    pub fn describe(&self, num_pes: usize) {
        println!("global size: {}", self.total_table_size(num_pes));
        println!("size per pe: {}", self.pe_table_size(num_pes));
        println!("total updates: {}", self.total_updates(num_pes));
        println!("updates per pe: {}", self.pe_updates(num_pes));
        println!("iterations: {}", self.iterations);
        println!("launch threads: {}", self.launch_threads);
        println!("buffer size: {}", self.buffer_size);
        println!("variants: {:?}", self.variants);
        println!("am index size: {:?}", self.am_index_size);
        println!("array distribution: {:?}", self.array_distribution);
    }

    pub fn total_table_size(&self, num_pes: usize) -> usize {
        if let Some(gu) = self.table_size.global_size {
            gu
        } else {
            self.table_size.local_size * num_pes
        }
    }

    pub fn pe_table_size(&self, num_pes: usize) -> usize {
        if let Some(gu) = self.table_size.global_size {
            (gu as f32 / num_pes as f32).ceil() as usize // round up just to be safe that we have enough space on each pe
        } else {
            self.table_size.local_size
        }
    }

    pub fn total_updates(&self, num_pes: usize) -> usize {
        if let Some(gu) = self.updates.global_updates {
            gu
        } else {
            self.updates.local_updates * num_pes
        }
    }

    pub fn pe_updates(&self, num_pes: usize) -> usize {
        if let Some(gu) = self.updates.global_updates {
            gu / num_pes
        } else {
            self.updates.local_updates
        }
    }

    pub fn max_variant_len(&self) -> usize {
        if let Some(variants) = &self.variants {
            variants
                .iter()
                .map(|v| format! {"{v:?}"}.len())
                .max()
                .unwrap()
        } else {
            16 // UnsafeBufferedAm
        }
    }

    pub fn max_index_size_len(&self) -> usize {
        if let Some(am_index_size) = self
            .am_index_size
            .iter()
            .map(|v| format! {"{v:?}"}.len())
            .max()
        {
            am_index_size
        } else {
            5 //usize
        }
    }

    pub fn max_array_distribution_len(&self) -> usize {
        if let Some(array_distribution) = self
            .array_distribution
            .iter()
            .map(|v| format! {"{v:?}"}.len())
            .max()
        {
            array_distribution
        } else {
            6 //cyclic
        }
    }
}
