use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct TcCli {
    #[arg(short, long)]
    pub graph_file: String,

    #[arg(short, long, default_value_t = 3)]
    pub iterations: usize,

    #[arg(short, long, default_value_t = 1)]
    pub launch_threads: usize,

    #[arg(short, long, env = "LAMELLAR_OP_BATCH")]
    pub buffer_size: usize,
}

impl TcCli {
    pub fn describe(&self) {
        println!("graph file: {}", self.graph_file);
        println!("iterations: {}", self.iterations);
        println!("launch threads: {}", self.launch_threads);
        println!("buffer size: {}", self.buffer_size);
    }
}
