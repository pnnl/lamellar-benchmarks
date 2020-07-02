use lamellar::{ActiveMessaging, LamellarWorld, RemoteClosures};

#[macro_use]
extern crate lazy_static;

use graph::Graph;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static GLOBAL_TC_CNT: AtomicU64 = AtomicU64::new(0);
static LOCAL_TC_CNT: AtomicU64 = AtomicU64::new(0);

lazy_static! {
    static ref GLOBAL_G: RwLock<Graph> = RwLock::new(Graph::new());
}

fn update_graph(edges: Vec<Vec<u32>>) {
    if GLOBAL_G.read().my_pe() != 0 {
        let mut g = GLOBAL_G.write();
        for e in edges {
            g.add_edges(&e);
        }
    }
}

fn init_graph(file: &String, my_pe: usize, world: &LamellarWorld) {
    GLOBAL_G.write().set_pe(my_pe as u32);
    let mut now = Instant::now();
    if my_pe == 0 {
        let mut g = GLOBAL_G.write();
        g.load_tsv(file).expect("error loading graph");
        println!("load time {:?}", now.elapsed());

        now = Instant::now();
        g.relabel_csr();
        drop(g);
        let g = GLOBAL_G.read();
        println!(
            "{:?} relabel time {:?} num_nodes: {:?}",
            my_pe,
            now.elapsed(),
            g.num_nodes()
        );
        let mut edges: Vec<Vec<u32>> = vec![];
        let mut size = 0;
        for node in 0..g.num_nodes() {
            if size > 10 * 1024 * 1024 {
                println!("node: {:?} {:?} {:?}", node, edges.len(), size);
                let req = world.exec_closure_all(lamellar::FnOnce!([edges] move || {
                    update_graph(edges);
                }));
                req.get_all();
                edges = vec![];
                size = 0;
            }
            edges.push(g.neighbors_vec(node));
            size += g.neighbors_vec(node).len();
        }
        edges.push(vec![]); // to set the last offset
        println!("edges {:?} {:?}", edges.len(), size);
        let req = world.exec_closure_all(lamellar::FnOnce!([edges] move || {
            update_graph(edges);
        }));
        req.get_all();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("{:?} {:?}", my_pe, num_pes);

    let mut now = Instant::now();
    init_graph(&file, my_pe, &world);
    world.barrier();
    println!("{:?} distribute time {:?}", my_pe, now.elapsed());

    let g = GLOBAL_G.read();
    let num_nodes = g.num_nodes();
    drop(g);
    println!("{:?} num nodes {:?}", my_pe, num_nodes);

    now = Instant::now();
    let mut sum: f32 = 0.0;
    let buf_size = 100;
    // let mut sub_time = 0f64;
    for node in (my_pe..(num_nodes as usize)).step_by(num_pes * buf_size) {
        let g = GLOBAL_G.read();
        let tt = Instant::now();
        let nodes: Vec<u32> = (node..std::cmp::min(node + buf_size * num_pes, num_nodes))
            .step_by(num_pes)
            .map(|i| i as u32)
            .collect();
        let mut neighs: Vec<Vec<u32>> = Vec::new();
        // println!("{:?} {:?}",std::cmp::min((node+buf_size),num_nodes),nodes.clone());
        for n in nodes.clone() {
            neighs.push(g.neighbors_less_than(n as u32));
        }
        world.exec_closure_all(lamellar::FnOnce!([nodes,neighs,num_pes] move || {
            let g = GLOBAL_G.read();
            let cnt = g.triangles_for_node_neighs_csr_dist2(nodes,neighs,num_pes as u32);
            drop(g);
            LOCAL_TC_CNT.fetch_add(cnt as u64,Ordering::SeqCst);
        }));
        sum += tt.elapsed().as_secs_f32();
    }
    println!("{:?} local issue time {:?} {:?}", my_pe, now.elapsed(), sum);
    world.barrier();
    world.wait_all();
    println!("{:?} local time {:?}", my_pe, now.elapsed());
    world.barrier();
    let my_cnt = LOCAL_TC_CNT.load(Ordering::SeqCst);
    world.exec_closure_pe(
        0,
        lamellar::FnOnce!([my_cnt,my_pe] move || {
            println!("{:?} {:?} {:?} ",GLOBAL_TC_CNT.load(Ordering::SeqCst),my_cnt,my_pe);
            GLOBAL_TC_CNT.fetch_add(my_cnt as u64,Ordering::SeqCst);
        }),
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
