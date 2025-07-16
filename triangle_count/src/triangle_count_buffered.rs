use lamellar::active_messaging::prelude::*;
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use lamellar_graph::{Graph, GraphData, GraphType};

#[lamellar::AmLocalData]
struct LaunchAm {
    graph: Graph,
    start: u32,
    end: u32,
    final_cnt: AtomicArray<usize>, //Instead of Darc<AtomicUsize> (as in the non buffered version), we can also use a atomic array to keep track of the counts.
    buf_size: usize,
}

#[lamellar::local_am]
impl LamellarAM for LaunchAm {
    async fn exec() {
        let task_group = LamellarTaskGroup::new(lamellar::world.clone());
        let graph_data = self.graph.data();
        let mut buffer = vec![];
        let mut cur_len = 0;
        for node_0 in (self.start..self.end).filter(|n| self.graph.node_is_local(n)) {
            let neighs = graph_data
                .neighbors_iter(&node_0)
                .take_while(|n| n < &&node_0)
                .map(|n| *n)
                .collect::<Vec<u32>>();
            cur_len += neighs.len();
            buffer.push((node_0, neighs)); // pack the node and neighbors into the buffer
            if cur_len > self.buf_size {
                task_group.exec_am_all(BufferedTcAm {
                    graph: graph_data.clone(),
                    data: buffer,
                    final_cnt: self.final_cnt.clone(),
                });
                buffer = vec![];
                cur_len = 0;
            }
        }
        if cur_len > 0 {
            //send the remaining data
            task_group.exec_am_all(BufferedTcAm {
                graph: graph_data.clone(),
                data: buffer,
                final_cnt: self.final_cnt.clone(),
            });
        }
    }
}

#[lamellar::AmData]
struct BufferedTcAm {
    graph: Darc<GraphData>,
    data: Vec<(u32, Vec<u32>)>,
    final_cnt: AtomicArray<usize>,
}

impl BufferedTcAm {
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
impl LamellarAM for BufferedTcAm {
    async fn exec() {
        let mut cnt = 0;
        for (_node_0, neighbors) in &self.data {
            // this loop is not present in the non-buffered version
            for node_1 in neighbors.iter().filter(|n| self.graph.node_is_local(n)) {
                //check to make sure node_1 is local to this pe
                let neighs_1 = self
                    .graph
                    .neighbors_iter(node_1)
                    .take_while(|n| n < &node_1);
                cnt += BufferedTcAm::sorted_intersection_count(neighbors.iter(), neighs_1);
            }
        }
        self.final_cnt.local_data().at(0).fetch_add(cnt); //we only need to update our local portion of the count, and we know each pe only has a single element of the cnt array
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

    let final_cnt = AtomicArray::new(world.team(), world.num_pes(), Distribution::Block); // convert it to an atomic array (which is accessible to all PEs)

    if my_pe == 0 {
        println!("num nodes {:?}", graph.num_nodes())
    };
    // this section of code creates and executes a number of "LaunchAMs" so that we
    // can use multiple threads to initiate the triangle counting active message.
    let batch_size = (graph.num_nodes() as f32) / (launch_threads as f32);

    for buf_size in [10, 100, 1000, 10000, 100000, 1000000].iter() {
        // for buf_size in [100000].iter() {
        if my_pe == 0 {
            println!("using buf_size: {:?}", buf_size);
        }
        world.barrier();
        let timer = std::time::Instant::now();
        let mut reqs = vec![];
        for tid in 0..launch_threads {
            let start = (tid as f32 * batch_size).round() as u32;
            let end = ((tid + 1) as f32 * batch_size).round() as u32;
            reqs.push(world.exec_am_local(LaunchAm {
                graph: graph.clone(),
                start: start,
                end: end,
                final_cnt: final_cnt.clone(),
                buf_size: *buf_size,
            }));
        }

        //we explicitly wait for all the LaunchAMs to finish so we can explicity calculate the issue time.
        // calling wait_all() here will block until all the AMs including the LaunchAMs and the TcAMs have finished.
        world.block_on(async move {
            for req in reqs {
                req.await;
            }
        });

        let issue_time = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!("issue time: {:?}", issue_time)
        };
        // at this point all the triangle counting active messages have been initiated.

        world.wait_all(); //wait for all the triangle counting active messages to finish locally

        let local_time = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!("local time: {:?}", local_time)
        };

        world.barrier(); //wait for all the triangle counting active messages to finish on all PEs

        let global_time = timer.elapsed().as_secs_f64();
        let final_cnt_sum = world.block_on(final_cnt.sum()); //reduce the final count across all PEs
        if my_pe == 0 {
            println!(
                "triangles counted: {:?}\nglobal time: {:?}",
                final_cnt_sum,
                global_time
            );

            println!("{{\"graph_file\":\"{}\",\"num_nodes\":{},\"launch_threads\":{},\"buf_size\":{},\"triangle_count\":{},\"issue_time_secs\":{:.6},\"local_time_secs\":{:.6},\"global_time_secs\":{:.6},\"mb_sent\":{:.6},\"mb_per_sec\":{:.6}}}",
                file,
                graph.num_nodes(),
                launch_threads,
                buf_size,
                final_cnt_sum,
                issue_time,
                local_time,
                global_time,
                world.MB_sent(),
                world.MB_sent() / global_time
            );

            println!();
        }
        world.block_on(final_cnt.dist_iter().for_each(|x| x.store(0))); //reset the final count array
    }
}
