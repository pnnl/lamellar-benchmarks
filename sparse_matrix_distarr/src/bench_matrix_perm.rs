// 20230217
// THIS FILE MAY CONTAIN USEFUL MATERIAL FOR LOADING GRAPHS
// However it hasn't been used in a while and I don't know of other uses for it.













// use data_structures::*;
// use data_structures::distributed::*;

// fn main() {

//     // collect arguments from the command line
//     let args: Vec<String> = std::env::args().collect();
    
//     // determine the path to the source file for graph data
//     let file = &args[1];

//     // set number of threads
//     let launch_threads = if args.len() > 2 {
//         match &args[2].parse::<usize>() {
//             Ok(x) => *x,
//             Err(_) => 2,
//         }
//     } else {
//         2
//     };

//     // initialize a world
//     let world = lamellar::LamellarWorldBuilder::new().build();
//     let my_pe = world.my_pe();
    
//     // load, reorder, and distribute the graph to all PEs
//     let graph: Graph = Graph::new(file, GraphType::MapGraph, world.clone());
    
//     // save to binary format; this is useful in contexts where one wishes to run many experiments, and avoid the cost of loading/parsing from .tsf format
//     graph.dump_to_bin(&format!("{file}.bin"));

//     // initialize our local counter (which is accessible to all PEs)
//     let final_cnt = Darc::new(&world, AtomicUsize::new(0)).unwrap(); 

//     if my_pe == 0 {
//         println!("num nodes {:?}", graph.num_nodes())
//     };

//     world.barrier();
//     let timer = std::time::Instant::now();

//     // this section of code creates and executes a number of "LaunchAMs" so that we
//     // can use multiple threads to initiate the triangle counting active message.
//     let batch_size = (graph.num_nodes() as f32) / (launch_threads as f32);
//     let mut reqs = vec![];
//     for tid in 0..launch_threads {
//         let start = (tid as f32 * batch_size).round() as u32;
//         let end = ((tid + 1) as f32 * batch_size).round() as u32;
//         reqs.push(world.exec_am_local(LaunchAm {
//             graph: graph.clone(),
//             start: start,
//             end: end,
//             final_cnt: final_cnt.clone(),
//         }));
//     }

//     // we explicitly wait for all the LaunchAMs to finish so we can explicity calculate the issue time.
//     // calling wait_all() here will block until all the AMs including the LaunchAMs and the TcAMs have finished.
//     world.block_on(async move {
//         for req in reqs {
//             req.await;
//         }
//     });
//     if my_pe == 0 {
//         println!("issue time: {:?}", timer.elapsed().as_secs_f64())
//     };
//     // at this point all the triangle counting active messages have been initiated.

//     world.wait_all(); //wait for all the triangle counting active messages to finish locally
//     if my_pe == 0 {
//         println!("local time: {:?}", timer.elapsed().as_secs_f64())
//     };

//     world.barrier(); //wait for all the triangle counting active messages to finish on all PEs
//     if my_pe == 0 {
//         println!("local cnt {:?}", final_cnt.load(Ordering::SeqCst))
//     };

//     if my_pe != 0 {
//         world.block_on(world.exec_am_pe(
//             //send the local triangle counting result to the PE 0
//             0,
//             CntAm {
//                 final_cnt: final_cnt.clone(),
//                 cnt: final_cnt.load(Ordering::SeqCst),
//             },
//         ));
//     }
//     world.barrier(); //at this point the final triangle counting result is available on PE 0

//     if my_pe == 0 {
//         println!(
//             "triangles counted: {:?} global time: {:?}",
//             final_cnt.load(Ordering::SeqCst),
//             timer.elapsed().as_secs_f64()
//         )
//     };
// }
