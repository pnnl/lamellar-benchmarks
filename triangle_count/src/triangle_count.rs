use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use lamellar_graph::{Graph, GraphData, GraphType};

use std::sync::atomic::{AtomicUsize, Ordering};

/// Add `cnt` to `final_cnt`, where `cnt` refers to the number of new triangles counted,
/// and `final_cnt` is the running total.
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

/// Sends out active messages for all vertices between `self.start` and `self.end` that are local
/// to the current PE.
/// 
/// As part of the counting process, each PE will eventually send an active message of type
/// `TcAm` to every other PE (including itself).
/// See the docstrings for [`TcAm`] for details on what that message does.  Concretely, if 
/// `x` is a PE, then for each vertex local to `x`, `x` will send one `TcAm`  to every PE.  The 
/// active message `LaunchAM` is designed to facilitate this process by sending out multiple
/// `TcAm's in parallel.  The user can partition their local vertices between several different
/// intervals `start1 .. end1`, 'start2 .. end2', .. and use a `LaunchAM` to sending out messages 
/// from each interval, independently.
/// 
/// **NB** Assumes that neighbors are sorted in ascending order.
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
            task_group.exec_am_all(TcAm {
                graph: graph_data.clone(),
                neighbors: graph_data
                    .neighbors_iter(&node_0)
                    .take_while(|n| n < &&node_0)
                    .map(|n| *n)
                    .collect::<Vec<u32>>(), //only send neighbors that are less than node_0 as an optimization
                final_cnt: self.final_cnt.clone(),
            });
        }
    }
}

/// Active message sent to every other pe.  
/// 
/// For each vertex v on the current pe, we send one active message of this form
/// to every other pe.  Thus the current pe will send a total of
/// (number of other pe's) x (number of nodes local to the current pe)
/// active messages, all together.
/// 
/// The most important part of the message is `neighbors`, the
/// list of neighbors of `v`.  We don't have to include `v` itself in this message,
/// because this information turns out to be unnecessary for the purpose of 
/// triangle counting.  See the implementaiton `impl LamellarAM for TcAm` for
/// details.
/// 
/// **NB** When initializing this message for a vertex `v`, we only write
/// vertices less than `v` to the `neighbors`.  This avoids over-counting.
#[lamellar::AmData]
pub struct TcAm {
    graph: Darc<GraphData>, //allows us to access the graph data on other pes (with out the data explicitly being allocated in RDMA registered memory)
    neighbors: Vec<u32>,
    final_cnt: Darc<AtomicUsize>,
}

impl TcAm {
    /// Return the cardinality of the maximum common subsequence of two monotonically
    /// increasing sequences.
    /// 
    /// Given two monotonically increasing sequences, `s` and `t`, returns the maximum
    /// `m` such that there exist `I = ( i_m < .. < i_m )` and `J = ( j_m < .. < j_m )`
    /// such that `s[I] = t[J]`.
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
    /// Count triangles composed of 
    /// 
    /// (i)   the node associated with the active message, 
    /// (ii)  a node local to the pe that receives the message, and
    /// (iii) one other node
    /// 
    /// The active message contains a list of neighbors adjacent to a give vertex, v.
    /// The value of v turns out to be irrelevant, for the purposes of triangle counting,
    /// however.  When the message activates, we
    /// (1) iterate over the neighbors of v
    /// (2) if a neighbor n is also local to the pe that receives the message,
    ///     then we count number of vertices in the intersection of three sets:
    ///     { neighbors of v }
    ///     { neighbors of n }
    ///     { vertices numbered < n }
    ///     We include the third set in the intersection to avoid double counting.
    ///     
    ///     **NB** The constructor that builds the TcAM only adds vertices numbered less 
    ///            than `v` to TcAm.neighbors.  Therefore every element of the three-way
    ///            intersection above is a strict lower bound of both n and v.
    /// (3) add this count to the running total of triangles
    /// 
    /// **NB** Assumes that neighbor lists appear in sorted order.  The parser used
    ///        in `Graph::new` generates graphs that satisfy this condition.
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

    // collect arguments from the command line
    let args: Vec<String> = std::env::args().collect();
    
    // determine the path to the source file for graph data
    let file = &args[1];

    // set number of threads
    let launch_threads = if args.len() > 2 {
        match &args[2].parse::<usize>() {
            Ok(x) => *x,
            Err(_) => 2,
        }
    } else {
        2
    };

    // initialize a world
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    
    // load, reorder, and distribute the graph to all PEs
    let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());
    
    // save to binary format; this is useful in contexts where one wishes to run many experiments, and avoid the cost of loading/parsing from .tsf format
    graph.dump_to_bin(&format!("{file}.bin"));

    // initialize our local counter (which is accessible to all PEs)
    let final_cnt = Darc::new(&world, AtomicUsize::new(0)).unwrap(); 

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

    // we explicitly wait for all the LaunchAMs to finish so we can explicity calculate the issue time.
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
