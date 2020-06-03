#[macro_use]
extern crate lazy_static;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;
mod graph;

static GLOBAL_TC_CNT: AtomicU64 = AtomicU64::new(0);
static LOCAL_TC_CNT: AtomicU64 = AtomicU64::new(0);

lazy_static! {
    static ref GLOBAL_G: RwLock<graph::Graph> = RwLock::new(graph::Graph::new());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file = &args[1];

    let (my_pe, num_pes) = lamellar::init();
    println!("{:?} {:?}", my_pe, num_pes);

    let mut g = GLOBAL_G.write().unwrap();
    g.set_rank(my_pe as u32);

    let mut now = Instant::now();
    g.load_tsv(file).expect("error loading graph");
    lamellar::barrier();
    println!("load time {:?}", now.elapsed());

    now = Instant::now();
    g.relabel_csr();
    let num_nodes = g.num_nodes();
    drop(g);
    lamellar::barrier();
    println!("{:?} relabel time {:?}", my_pe, now.elapsed());

    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    now = Instant::now();
    let mut sum: f32 = 0.0;
    let buf_size = 100;
    // let mut sub_time = 0f64;
    for node in (my_pe..(num_nodes as usize)).step_by(num_pes * buf_size) {
        let g = GLOBAL_G.read().unwrap();
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
        lamellar::exec_all(lamellar::FnOnce!([nodes,neighs,num_pes] move || {
            let g = GLOBAL_G.read().unwrap();
            let cnt = g.triangles_for_node_neighs_csr_dist2(nodes,neighs,num_pes as u32);
            drop(g);
            LOCAL_TC_CNT.fetch_add(cnt as u64,Ordering::SeqCst);
        }));
        sum += tt.elapsed().as_secs_f32();
    }
    println!("{:?} local issue time {:?} {:?}", my_pe, now.elapsed(), sum);
    // let data = bincode::serialize(&sys_t).unwrap();
    // lamellar::exec_all(lamellar::FnOnce!([data] move || {
    //     let duration: std::time::SystemTime = bincode::deserialize(&data).unwrap();
    //     let now =  SystemTime::now();
    //     if let Ok(timer) = now.duration_since(duration){
    //         println!("{:?} maybe this means ive issued everything? {:?}",lamellar::local_pe(), timer.as_secs_f64());
    //     }
    // }));
    lamellar::barrier();
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
