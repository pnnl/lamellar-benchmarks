#[macro_use]
extern crate lazy_static;

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
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

    let mut g = GLOBAL_G.write();
    g.set_rank(my_pe as u32);

    let mut now = Instant::now();
    g.load_tsv(file).expect("error loading graph");
    lamellar::barrier();
    println!("load time {:?}", now.elapsed());

    now = Instant::now();
    g.relabel_csr();
    g.trim();
    let num_nodes = g.num_nodes();
    drop(g);
    lamellar::barrier();
    println!("{:?} relabel time {:?}", my_pe, now.elapsed());

    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    now = Instant::now();
    let mut sum: f32 = 0.0;

    for node in (my_pe..(num_nodes as usize)).step_by(num_pes) {
        // if node > 10{
        //     break;
        // }
        let g = GLOBAL_G.read();
        // let neighs = g.neighbors_less_than(node as u32);
        let neighs = g.neighbors_vec(node);
        // println!("neighs len {:?}", neighs.len());
        let tt = Instant::now();
        lamellar::exec_all(lamellar::FnOnce!([neighs,node,num_pes] move || {
            let g = GLOBAL_G.read();
            let cnt = g.triangles_for_node_neighs_csr_dist(node as u32, &neighs, num_pes as u32);
            // println!{"node: {:?} cnt: {:?}",node,cnt};
            drop(g);
            LOCAL_TC_CNT.fetch_add(cnt as u64,Ordering::SeqCst);
        }));
        sum += tt.elapsed().as_secs_f32();
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
