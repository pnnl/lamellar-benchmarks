#[macro_use]
extern crate lazy_static;

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
mod graph;

static GLOBAL_TC_CNT: AtomicU64 = AtomicU64::new(0);
static LOCAL_TC_CNT: AtomicU64 = AtomicU64::new(0);

// lazy_static! {
//     static ref GLOBAL_G: RwLock<graph::Graph> = RwLock::new(graph::Graph::new());
// }

fn blocking_triangles_cnt(n0 :u32 ,n0_neigh: &[u32],offsets: &[u32],edges: &[u32],my_pe: usize, num_pes: usize) -> u64{
    let mut cnt = 0;
    let mut timer = Instant::now();
    for n1_idx in 0..n0_neigh.len(){
        while n0_neigh[n1_idx] == std::u32::MAX{
            std::thread::yield_now();
            if timer.elapsed().as_secs_f32() > 5.0{
                println!("pe: {:} n0: {:?} -> {:?} idx: {:?}",my_pe,n0,n0_neigh,n1_idx);
                timer = Instant::now();
            }
        }
        let n1 = n0_neigh[n1_idx];
        if n1 as usize % num_pes == my_pe{
            if n1 > n0{
                break;
            }
            let n1_local_idx = n1 as usize / num_pes;
            let mut n1_it = 0;
            let n1_neigh = &edges[offsets[n1_local_idx] as usize..offsets[n1_local_idx+1] as usize];
            for n2 in n1_neigh {
                if n2 > &n1{
                    break;
                }
                while n0_neigh[n1_it] == std::u32::MAX{
                    std::thread::yield_now();
                    if timer.elapsed().as_secs_f32() > 5.0{
                        println!("pe: {:} n0: {:?} -> {:?} idx: {:?} it: {:?}",my_pe,n0,n0_neigh,n1_idx,n1_it);
                        timer = Instant::now();
                    }
                }
                while n0_neigh[n1_it] < *n2 {
                    n1_it += 1;
                }
                if *n2 == n0_neigh[n1_it] {
                    cnt += 1;
                }
            }
        }
    }    
    return cnt;
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];
    let (my_pe, num_pes) = lamellar::init();
    println!("{:?} {:?}", my_pe, num_pes);
    
    let mut g = graph::Graph::new();
    g.set_rank(my_pe as u32);

    let mut now = Instant::now();
    g.load_tsv(file).expect("error loading graph");
    lamellar::barrier();
    println!("load time {:?}", now.elapsed());

    now = Instant::now();
    g.relabel_csr();
    g.trim();
    let num_nodes = g.num_nodes();
    let (temp_offsets,temp_edges,max_edges) = g.get_dist_offsets_and_edges(my_pe,num_pes);
    let mut main_offsets:Vec<lamellar::LamellarMemoryRegion<u32>> = vec![];//lamellar::alloc_mem_region(num_nodes);
    for pe_offsets in temp_offsets{
        let global_offsets = lamellar::alloc_mem_region(pe_offsets.len());
        unsafe { std::ptr::copy_nonoverlapping(pe_offsets.as_ptr(),global_offsets.as_mut_slice().as_mut_ptr(),pe_offsets.len()) };
        main_offsets.push(global_offsets);
    }
    let main_edges = lamellar::alloc_mem_region(max_edges);
    unsafe { std::ptr::copy_nonoverlapping(temp_edges.as_ptr(),main_edges.as_mut_slice().as_mut_ptr(),temp_edges.len()) };
    drop(g);
    drop(temp_edges);
    lamellar::barrier();
    println!("{:?} relabel time {:?}", my_pe, now.elapsed());
    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    now = Instant::now();
    let mut sum: f32 = 0.0;
    let buf_size = 50;
    for start_node in (0..num_nodes).step_by(num_pes * buf_size){
        for node in (start_node..start_node+num_pes){
        // for node in (0..num_nodes).step_by(num_pes * buf_size){//(my_pe..(num_nodes as usize)).step_by(num_pes) {
            let tt = Instant::now();
            let offsets = main_offsets.clone();
            let edges = main_edges.clone();
            lamellar::exec_on_pe(my_pe,lamellar::FnOnce!([start_node,node,num_nodes,my_pe,num_pes,offsets,edges,buf_size] move || {
                let mut local_cnt: u64=0;
                // let mut neighs = vec![];
                let pe = node % num_pes;
                let pe_offsets = offsets[pe].as_slice();
                let end_node = std::cmp::min(start_node+num_pes * buf_size, num_nodes);
                let nodes: Vec<u32> = (node..end_node)
                .step_by(num_pes)
                .map(|i| i as u32)
                .collect(); //nodes to get..
                let start_node_local_idx = node/num_pes;
                let end_node_local_idx = *nodes.last().unwrap() as usize/num_pes;
                let idx = pe_offsets[start_node_local_idx];
                let size = pe_offsets[end_node_local_idx+1]-idx;
                let mut pe_edges = vec![std::u32::MAX;size as usize];
                if size > 0 {
                    unsafe { edges.get(pe,idx as usize,&mut pe_edges) };
                }
                // let relative_idx =idx;
                let mut node_idx = start_node_local_idx;
                for node in nodes{
                    let n0_neigh = &pe_edges[(pe_offsets[node_idx]-idx) as usize..(pe_offsets[node_idx+1]-idx) as usize];
                    node_idx+=1;
                    local_cnt += blocking_triangles_cnt(node as u32,n0_neigh,offsets[my_pe].as_slice(),edges.as_slice(),my_pe,num_pes);
                }
                
                LOCAL_TC_CNT.fetch_add(local_cnt,Ordering::SeqCst);
            }));
            sum += tt.elapsed().as_secs_f32();
        }
    }


    println!("{:?} local issue time {:?} {:?}", my_pe, now.elapsed(), sum);
    lamellar::wait_all();
    println!("{:?} local time {:?}", my_pe, now.elapsed());
    lamellar::barrier();
    let my_cnt = LOCAL_TC_CNT.load(Ordering::SeqCst);
    lamellar::exec_on_pe(
        0,
        lamellar::FnOnce!([my_cnt,my_pe] move || {
            println!("{:?} {:?} {:?} {:?}",GLOBAL_TC_CNT.load(Ordering::SeqCst),my_cnt,lamellar::local_pe(),my_pe);
            GLOBAL_TC_CNT.fetch_add(my_cnt as u64,Ordering::SeqCst);
        }),
    );
    lamellar::wait_all();
    lamellar::barrier();
    let g_time = now.elapsed().as_secs_f32();
    if my_pe == 0 {
        println!(
            "global time {:?}",
            g_time
        );
    }
    lamellar::barrier();
    if my_pe == 0 {
        println!(
            "{:?} triangle count: {:?} {:?}",
            my_pe,
            GLOBAL_TC_CNT.load(Ordering::SeqCst),
            LOCAL_TC_CNT.load(Ordering::SeqCst)
        );
    }
    lamellar::finit();
}
