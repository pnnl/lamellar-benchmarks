use lamellar::{ActiveMessaging, LamellarAM, LamellarMemoryRegion, RemoteMemoryRegion};

#[macro_use]
extern crate lazy_static;

use graph::Graph;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static GLOBAL_TC_CNT: AtomicU64 = AtomicU64::new(0);
static LOCAL_TC_CNT: AtomicU64 = AtomicU64::new(0);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct TcAM {
    start_node: u32,
    node: u32,
    num_nodes: u32,
    pe: usize,
    offsets: Vec<LamellarMemoryRegion<u32>>,
    edges: LamellarMemoryRegion<u32>,
    buf_size: u32,
}

#[lamellar::am]
impl LamellarAM for TcAM {
    fn exec(self) {
        let mut local_cnt: u64 = 0;
        // let mut neighs = vec![];
        let num_pes = lamellar::num_pes as u32;
        let pe = (self.node % num_pes) as usize;
        let pe_offsets = self.offsets[pe].as_slice();
        let end_node = std::cmp::min(self.start_node + num_pes * self.buf_size, self.num_nodes);
        let nodes: Vec<u32> = (self.node..end_node)
            .step_by(num_pes as usize)
            .map(|i| i as u32)
            .collect(); //nodes to get..
        if nodes.len() > 0 {
            let start_node_local_idx = (self.node / num_pes) as usize;
            let end_node_local_idx = *nodes.last().unwrap() as usize / num_pes as usize;
            let idx = pe_offsets[start_node_local_idx];
            let size = pe_offsets[end_node_local_idx + 1] - idx;
            // let mut pe_edges = vec![std::u32::MAX;size as usize]; // update impl lib to handle this...
            let mut pe_edges = std::vec::Vec::<u32>::new();
            for _i in 0..size {
                pe_edges.push(std::u32::MAX);
            }
            if size > 0 {
                unsafe { self.edges.get(pe, idx as usize, &mut pe_edges) };
            }
            // let relative_idx =idx;
            let mut node_idx = start_node_local_idx;
            for node in nodes.clone() {
                let n0_neigh = &pe_edges[(pe_offsets[node_idx] - idx) as usize
                    ..(pe_offsets[node_idx + 1] - idx) as usize];
                node_idx += 1;
                local_cnt += blocking_triangles_cnt(
                    node as u32,
                    n0_neigh,
                    self.offsets[self.pe].as_slice(),
                    self.edges.as_slice(),
                    self.pe,
                    num_pes as usize,
                );
            }

            LOCAL_TC_CNT.fetch_add(local_cnt, Ordering::SeqCst);
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct CntAM {
    pe: usize,
    cnt: u64,
}
#[lamellar::am]
impl LamellarAM for CntAM {
    fn exec(self) {
        println!(
            "{:?} {:?} {:?} {:?}",
            GLOBAL_TC_CNT.load(Ordering::SeqCst),
            self.cnt,
            lamellar::current_pe,
            self.pe
        );
        GLOBAL_TC_CNT.fetch_add(self.cnt, Ordering::SeqCst);
    }
}

fn blocking_triangles_cnt(
    n0: u32,
    n0_neigh: &[u32],
    offsets: &[u32],
    edges: &[u32],
    my_pe: usize,
    num_pes: usize,
) -> u64 {
    let mut cnt = 0;
    for n1_idx in 0..n0_neigh.len() {
        while n0_neigh[n1_idx] == std::u32::MAX {
            std::thread::yield_now();
        }
        let n1 = n0_neigh[n1_idx];
        if n1 as usize % num_pes == my_pe {
            if n1 > n0 {
                break;
            }
            let n1_local_idx = n1 as usize / num_pes;
            let mut n1_it = 0;
            let n1_neigh =
                &edges[offsets[n1_local_idx] as usize..offsets[n1_local_idx + 1] as usize];
            for n2 in n1_neigh {
                if n2 > &n1 {
                    break;
                }
                while n0_neigh[n1_it] == std::u32::MAX {
                    std::thread::yield_now();
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

lazy_static! {
    static ref GLOBAL_G: RwLock<Graph> = RwLock::new(Graph::new());
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct GraphAM {
    edges: Vec<Vec<u32>>,
}

#[lamellar::am]
impl LamellarAM for GraphAM {
    fn exec(self) {
        if lamellar::current_pe != 0 {
            let mut g = GLOBAL_G.write();
            for edges in &self.edges {
                g.add_edges(edges);
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];
    let buf_size = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 10);

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("{:?} {:?}", my_pe, num_pes);
    

    let mut g = GLOBAL_G.write();
    g.set_pe(my_pe as u32);
    let mut now = Instant::now();
    if my_pe == 0 {
        g.load_tsv(file).expect("error loading graph");
        println!("load time {:?}", now.elapsed());

        now = Instant::now();
        g.relabel_csr();
        println!(
            "{:?} relabel time {:?} num_nodes: {:?}",
            my_pe,
            now.elapsed(),
            g.num_nodes()
        );
        now = Instant::now();
        let mut edges = vec![];
        let mut size = 0;
        for node in 0..g.num_nodes() {
            if size > 10 * 1024 * 1024 {
                println!("node: {:?} {:?} {:?}", node, edges.len(), size);
                //64K entries * 4bytes == 128KB per message
                let req = world.exec_am_all(GraphAM { edges: edges });
                req.get_all();
                edges = vec![];
                size = 0;
            }
            edges.push(g.neighbors_vec(node));
            size += g.neighbors_vec(node).len();
        }
        edges.push(vec![]); // to set the last offset
        let req = world.exec_am_all(GraphAM { edges: edges });
        req.get_all();
    }
    drop(g);
    world.barrier();
    let g = GLOBAL_G.read();
    let num_nodes = g.num_nodes();
    println!("{:?} distribute time {:?}", my_pe, now.elapsed());
    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    let (temp_offsets, temp_edges, max_edges) = g.get_dist_offsets_and_edges(my_pe, num_pes);
    let mut main_offsets: Vec<LamellarMemoryRegion<u32>> = vec![]; //world.alloc_mem_region(num_nodes);
    for pe_offsets in temp_offsets {
        let global_offsets = world.alloc_mem_region::<u32>(pe_offsets.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                pe_offsets.as_ptr(),
                global_offsets.as_mut_slice().as_mut_ptr(),
                pe_offsets.len(),
            )
        };
        main_offsets.push(global_offsets);
    }
    let main_edges = world.alloc_mem_region::<u32>(max_edges);
    unsafe {
        std::ptr::copy_nonoverlapping(
            temp_edges.as_ptr(),
            main_edges.as_mut_slice().as_mut_ptr(),
            temp_edges.len(),
        )
    };
    drop(g);
    drop(temp_edges);
    world.barrier();
    println!("{:?} relabel time {:?}", my_pe, now.elapsed());
    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    now = Instant::now();
    let mut sum: f32 = 0.0;
    for start_node in (0..num_nodes).step_by(num_pes * buf_size) {
        for node in start_node..start_node + num_pes {
            let tt = Instant::now();
            world.exec_am_pe(
                my_pe,
                TcAM {
                    start_node: start_node as u32,
                    node: node as u32,
                    num_nodes: num_nodes as u32,
                    pe: my_pe,
                    offsets: main_offsets.clone(),
                    edges: main_edges.clone(),
                    buf_size: buf_size as u32,
                },
            );
            sum += tt.elapsed().as_secs_f32();
        }
    }

    println!("{:?} local issue time {:?} {:?}", my_pe, now.elapsed(), sum);
    world.wait_all();
    println!("{:?} local time {:?}", my_pe, now.elapsed());
    world.barrier();
    let my_cnt = LOCAL_TC_CNT.load(Ordering::SeqCst);
    world.exec_am_pe(
        0,
        CntAM {
            pe: my_pe,
            cnt: my_cnt,
        },
    );
    world.wait_all();
    world.barrier();
    let g_time = now.elapsed().as_secs_f32();
    if my_pe == 0 {
        println!("global time {:?}", g_time);
    }
    world.barrier();
    if my_pe == 0 {
        println!(
            "{:?} triangle count: {:?} {:?}",
            my_pe,
            GLOBAL_TC_CNT.load(Ordering::SeqCst),
            LOCAL_TC_CNT.load(Ordering::SeqCst)
        );
    }
}
