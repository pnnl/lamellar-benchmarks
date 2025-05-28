use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use lamellar_graph::{Graph, GraphData, GraphType};

use std::sync::atomic::{AtomicUsize, Ordering};

#[lamellar::AmData]
struct CntAm {
    final_cnt: Darc<AtomicUsize>,
    cnt: usize,
}

#[lamellar::am]
impl LamellarAM for CntAm {
    async fn exec() {
        println!("here 1");
        self.final_cnt.fetch_add(self.cnt, Ordering::Relaxed);
    }
}

#[lamellar::AmLocalData]
struct LaunchAm {
    graph: Graph,
    start: u32,
    end: u32,
    final_cnt: Darc<AtomicUsize>,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec() {
        let task_group = LamellarTaskGroup::new(lamellar::world.clone());
        let graph_data = self.graph.data();
        for node_0 in (self.start..self.end).filter(|n| self.graph.node_is_local(n)) {
            task_group
                .exec_am_all(TcAm {
                    graph: graph_data.clone(),
                    node: node_0,
                    neighbors: graph_data
                        .neighbors_iter(&node_0)
                        .take_while(|n| n < &&node_0)
                        .map(|n| *n)
                        .collect::<Vec<u32>>(), //only send neighbors that are less than node_0 as an optimization
                    final_cnt: self.final_cnt.clone(),
                })
                .await;
        }
    }
}

#[lamellar::AmData]
struct TcAm {
    graph: Darc<GraphData>, //allows us to access the graph data on other pes (with out the data explicitly being allocated in RDMA registered memory)
    node: u32,
    neighbors: Vec<u32>,
    final_cnt: Darc<AtomicUsize>,
}

impl TcAm {
    fn sorted_intersection_count<'a>(
        set0: impl Iterator<Item = &'a u32> + Clone,
        mut set1: impl Iterator<Item = &'a u32> + Clone,
    ) -> usize {
        let mut count = 0;
        if let Some(mut node_1) = set1.next() {
            for node_0 in set0 {
                while node_1 < node_0 {
                    node_1 = match set1.next() {
                        Some(node_1) => node_1,
                        None => return count,
                    };
                }
                if node_0 == node_1 {
                    count += 1;
                }
            }
        }
        count
    }
}

#[lamellar::am]
impl LamellarAM for TcAm {
    async fn exec() {
        // println!("here");
        let mut cnt = 0;
        for node_1 in self
            .neighbors
            .iter()
            .filter(|n| self.graph.node_is_local(n))
        {
            //check to make sure node_1 is local to this pe
            let neighs_1 = self
                .graph
                .neighbors_iter(node_1)
                .take_while(|n| n < &node_1);
            cnt += TcAm::sorted_intersection_count(self.neighbors.iter(), neighs_1);
        }
        self.final_cnt.fetch_add(cnt, Ordering::SeqCst);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];
    let launch_threads = if args.len() > 2 {
        match &args[2].parse::<usize>() {
            Ok(x) => *x,
            Err(_) => 2,
        }
    } else {
        2
    };

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    //this loads, reorders, and distributes the graph to all PEs
    let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());
    graph.dump_to_bin(&format!("{file}.bin"));
    let final_cnt = Darc::new(&world, AtomicUsize::new(0)).block().unwrap(); // initialize our local counter (which is accessible to all PEs)

    if my_pe == 0 {
        println!("num nodes {:?}", graph.num_nodes())
    };

    world.barrier();
    let timer = std::time::Instant::now();

    // this section of code creates and executes a number of "LaunchAMs" so that we
    // can use multiple threads to initiate the triangle counting active message.
    let batch_size = (graph.num_nodes() as f32) / (launch_threads as f32);
    let mut reqs = vec![];
    for tid in 0..launch_threads {
        let start = (tid as f32 * batch_size).round() as u32;
        let end = ((tid + 1) as f32 * batch_size).round() as u32;
        reqs.push(world.exec_am_local(LaunchAm {
            graph: graph.clone(),
            start: start,
            end: end,
            final_cnt: final_cnt.clone(),
        }));
    }

    //we explicitly wait for all the LaunchAMs to finish so we can explicity calculate the issue time.
    // calling wait_all() here will block until all the AMs including the LaunchAMs and the TcAMs have finished.
    world.block_on(async move {
        for req in reqs {
            req.await;
        }
    });
    if my_pe == 0 {
        println!("issue time: {:?}", timer.elapsed().as_secs_f64())
    };
    // at this point all the triangle counting active messages have been initiated.

    world.wait_all(); //wait for all the triangle counting active messages to finish locally
    if my_pe == 0 {
        println!("local time: {:?}", timer.elapsed().as_secs_f64())
    };

    world.barrier(); //wait for all the triangle counting active messages to finish on all PEs
    if my_pe == 0 {
        println!("local cnt {:?}", final_cnt.load(Ordering::SeqCst))
    };

    if my_pe != 0 {
        world.block_on(world.exec_am_pe(
            //send the local triangle counting result to the PE 0
            0,
            CntAm {
                final_cnt: final_cnt.clone(),
                cnt: final_cnt.load(Ordering::SeqCst),
            },
        ));
    }
    world.barrier(); //at this point the final triangle counting result is available on PE 0

    if my_pe == 0 {
        println!(
            "triangles counted: {:?} global time: {:?}",
            final_cnt.load(Ordering::SeqCst),
            timer.elapsed().as_secs_f64()
        )
    };
}
