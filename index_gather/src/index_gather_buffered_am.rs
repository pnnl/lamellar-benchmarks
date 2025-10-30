use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;
use rand::prelude::*;
use std::future::Future;
use std::time::Instant;
use benchmark_record::BenchmarkInformation;

const COUNTS_LOCAL_LEN: usize = 1_000_000; // this will be 800MB on each PE

// ===== INDEX_GATHER (Buffered AM) =====

#[lamellar::AmData(Clone, Debug)]
struct IndexGatherBufferedAM {
    buff: std::vec::Vec<usize>,
    counts: SharedMemoryRegion<usize>,
}

#[lamellar::am]
impl LamellarAM for IndexGatherBufferedAM {
    async fn exec(self) -> Vec<usize> {
        let counts_slice = unsafe { self.counts.as_slice().unwrap() };
        self.buff
            .iter()
            .map(|i| counts_slice[*i])
            .collect::<Vec<usize>>()
    }
}

#[lamellar::AmLocalData(Clone, Debug)]
struct LaunchAm {
    rand_index: OneSidedMemoryRegion<usize>,
    counts: SharedMemoryRegion<usize>,
    buffer_amt: usize,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec(self) {
        let num_pes = lamellar::num_pes;
        let mut buffs: std::vec::Vec<std::vec::Vec<usize>> =
            vec![Vec::with_capacity(self.buffer_amt); num_pes];
        let task_group = LamellarTaskGroup::new(lamellar::team.clone());

        for idx in unsafe { self.rand_index.as_slice().unwrap() } {
            let rank = idx % num_pes;
            let offset = idx / num_pes;

            buffs[rank].push(offset);
            if buffs[rank].len() >= self.buffer_amt {
                let buff = buffs[rank].clone();
                task_group
                    .exec_am_pe(
                        rank,
                        IndexGatherBufferedAM {
                            buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .await;
                buffs[rank].clear();
            }
        }

        // send any remaining buffered updates
        for rank in 0..num_pes {
            let buff = buffs[rank].clone();
            if !buff.is_empty() {
                task_group
                    .exec_am_pe(
                        rank,
                        IndexGatherBufferedAM {
                            buff,
                            counts: self.counts.clone(),
                        },
                    )
                    .await;
            }
        }
    }
}

fn histo(
    l_num_updates: usize,
    num_threads: usize,
    world: &LamellarWorld,
    rand_index: &OneSidedMemoryRegion<usize>,
    counts: &SharedMemoryRegion<usize>,
    buffer_amt: usize,
) -> Vec<impl Future<Output = ()>> {
    let slice_size = l_num_updates as f32 / num_threads as f32;
    let mut launch_tasks = vec![];
    for tid in 0..num_threads {
        let start = (tid as f32 * slice_size).round() as usize;
        let end = ((tid + 1) as f32 * slice_size).round() as usize;
        launch_tasks.push(world.exec_am_local(LaunchAm {
            rand_index: rand_index.sub_region(start..end),
            counts: counts.clone(),
            buffer_amt,
        }));
    }
    launch_tasks
}

// ===== MAIN =====

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let counts = world.alloc_shared_mem_region(COUNTS_LOCAL_LEN);
    let global_count = COUNTS_LOCAL_LEN * num_pes;

    let l_num_updates = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let buffer_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);

    let num_threads = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| match std::env::var("LAMELLAR_THREADS") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 1,
        });

    // === Initialize Benchmark Record ===
    let mut bench = BenchmarkInformation::new();
    bench.with_output("updates_total", (l_num_updates * num_pes).to_string());
    bench.with_output("updates_per_pe", l_num_updates.to_string());
    bench.with_output("table_size_per_pe", COUNTS_LOCAL_LEN.to_string());

    let rand_index = world.alloc_one_sided_mem_region(l_num_updates);
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);

    let counts = counts.block();

    unsafe {
        for elem in counts.as_mut_slice().unwrap().iter_mut() {
            *elem = 0;
        }
        for elem in rand_index.as_mut_slice().unwrap().iter_mut() {
            // NOTE: rand 0.8+ API
            *elem = rng.gen_range(0, global_count);
        }
    }

    // === Execute benchmark ===
    world.barrier();
    let now = Instant::now();

    let launch_tasks = histo(
        l_num_updates,
        num_threads,
        &world,
        &rand_index,
        &counts,
        buffer_amt,
    );

    world.block_on(async move {
        for task in launch_tasks {
            task.await;
        }
    });

    world.wait_all();
    world.barrier();

    let global_time = now.elapsed().as_secs_f64();

    // === Collect metrics ===
    bench.with_output("num_pes", num_pes.to_string());
    bench.with_output("num_threads", num_threads.to_string());
    bench.with_output("global_execution_time_secs", global_time.to_string());

    let global_mups = ((l_num_updates * num_pes) as f64 / 1_000_000.0) / global_time;
    bench.with_output("MUPS", global_mups.to_string());

    let mb_sent = world.MB_sent();
    bench.with_output("MB_sent", mb_sent.to_string());
    bench.with_output("MB_per_sec", (mb_sent / global_time).to_string());
    bench.with_output("GB_s_injection_rate", (8.0 * (l_num_updates * 2) as f64 * 1.0E-9 / global_time).to_string());
 

    // Optional: sanity metric (sum of counts)
    let pe_sum: u64 = unsafe { counts.as_slice().unwrap().iter().sum::<usize>() as u64 };
    bench.with_output("pe_sum", pe_sum.to_string());

    if my_pe == 0 {
        println!("Global time: {:.3}s, MUPS: {:.3}", global_time, global_mups);
        bench.write(&benchmark_record::default_output_path("benchmarking"));
    }
}
