// this code sets up a distributed counter array - a 'histogram' (like all other codes)
// It builds a vector of atomic counters that’s shared across all processes/PEs using Lamellar’s Darc (a distributed, ref-counted container).
// Then, each PE creates a list of random indices into the histogram.
// For each random index, the program sends a Lamellar active message to the PE responsible for that bin, 
// asking it to atomically increment the counter at that offset. This simulates a typical irregular, communication-heavy HPC workload.
// Then it measures the local run time, global run time, and MUPs.
// Tests: communication between nodes, synchronization, and throughput. 

use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;

use rand::prelude::*;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use benchmark_record;

const COUNTS_LOCAL_LEN: usize = 10000000;

//===== HISTO BEGIN ======

#[lamellar::AmData(Clone, Debug)]
struct HistoAM {
    offset: usize,
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::am]
impl LamellarAM for HistoAM {
    async fn exec(self) {
        self.counts[self.offset].fetch_add(1, Ordering::Relaxed);
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: Vec<usize>,
    counts: Darc<Vec<AtomicUsize>>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        for idx in &self.rand_index {
            let rank = idx % lamellar::num_pes;
            let offset = idx / lamellar::num_pes;
            let _ = lamellar::world
                .exec_am_pe(
                    rank,
                    HistoAM {
                        offset: offset,
                        counts: self.counts.clone(),
                    },
                )
                .spawn();
        }
    }
}

fn histo(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    mut rand_index: Vec<usize>,
    counts: &Darc<Vec<AtomicUsize>>,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        let split_index = rand_index.len() - (end - start);
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.split_off(split_index),
            counts: counts.clone(),
        }));
    }
    launch_tasks
}

//===== HISTO END ======

fn main() {

    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let mut result_record = benchmark_record::BenchmarkInformation::new();

    let global_count = COUNTS_LOCAL_LEN * num_pes;
    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let mut counts_data = Vec::with_capacity(COUNTS_LOCAL_LEN);
    for _ in 0..COUNTS_LOCAL_LEN {
        counts_data.push(AtomicUsize::new(0));
    }
    let counts = Darc::new(&world, counts_data)
        .block()
        .expect("unable to create darc");
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let rand_index = (0..l_num_updates)
        .into_iter()
        .map(|_| rng.gen_range(0, global_count))
        .collect::<Vec<usize>>();

    //create multiple launch tasks, that iterated through portions of rand_index in parallel
    let num_threads = match std::env::var("LAMELLAR_THREADS") {
        Ok(n) => n.parse::<usize>().unwrap(),
        Err(_) => 1,
    };
    let num_threads = std::cmp::max(num_threads / 2, 1);
    world.barrier();
    let now = Instant::now();
    let launch_tasks = histo(l_num_updates, num_threads, &world, rand_index, &counts);

    result_record.with_output("issue_start_time", now.elapsed().as_secs_f64().to_string());

    world.block_on(async move {
        for task in launch_tasks {
            task.await;
        }
    });

    result_record.with_output("issue_complete_time", now.elapsed().as_secs_f64().to_string());
    world.wait_all();
    result_record.with_output("local_run_time", now.elapsed().as_secs_f64().to_string());
    world.barrier();
    

    let global_time = now.elapsed().as_secs_f64();
    let total_updates = (l_num_updates * num_pes) as f64;
    let global_mups = (total_updates / 1_000_000.0) / global_time;
    let mb_sent = world.MB_sent();
    let mb_per_sec = mb_sent / global_time;

    result_record.with_output("global_execution_time (secs)", global_time.to_string());
    result_record.with_output("MUPS", global_mups.to_string());
    result_record.with_output("MB_sent", mb_sent.to_string());
    result_record.with_output("MB_per_sec", mb_per_sec.to_string());

    println!(
        "pe {:?} sum {:?}",
        my_pe,
        counts
            .iter()
            .map(|e| e.load(Ordering::Relaxed))
            .sum::<usize>()
    );

    if my_pe == 0 {
        result_record.write(&benchmark_record::default_output_path());
        println!("Benchmark Results:");
        result_record.display(Some(3));
    }
}
