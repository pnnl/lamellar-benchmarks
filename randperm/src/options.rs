use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct RandpermCli {
    #[arg(short = 's', long, default_value_t = 1000)]
    pub global_size: usize,

    #[arg(short, long, default_value_t = 3)]
    pub iterations: usize,

    #[arg(short, long, default_value_t = 1)]
    pub launch_threads: usize,

    #[arg(short = 'f', long, default_value_t = 2)]
    pub target_factor: usize,

    #[arg(short, long, env = "LAMELLAR_OP_BATCH")]
    pub buffer_size: usize,
}

impl RandpermCli {
    pub fn describe(&self, num_pes: usize) {
        println!("global size: {}", self.global_size);
        println!("size per pe: {}", self.global_size / num_pes);
        println!("iterations: {}", self.iterations);
        println!("launch threads: {}", self.launch_threads);
        println!("target factor: {}", self.target_factor);
        println!("buffer size: {}", self.buffer_size);
    }
}
