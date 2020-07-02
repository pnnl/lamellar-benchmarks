use lamellar::{ActiveMessaging, LamellarAM};

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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct TcAM {
    neighs: Vec<u32>,
    node: u32,
}

#[lamellar::am]
impl LamellarAM for TcAM {
    fn exec(self) {
        let g = GLOBAL_G.read();
        let cnt =
            g.triangles_for_node_neighs_csr_dist(self.node, &self.neighs, lamellar::num_pes as u32);
        LOCAL_TC_CNT.fetch_add(cnt as u64, Ordering::SeqCst);
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
        for node in 0..g.num_nodes() {
            if edges.len() > 64 * 1024 {
                //64K entries * 4bytes == 128KB per message
                let req = world.exec_am_all(GraphAM { edges: edges });
                req.am_get_all();
                edges = vec![];
            }
            edges.push(g.neighbors_vec(node));
        }
        edges.push(vec![]); // to set the last offset
        let req = world.exec_am_all(GraphAM { edges: edges });
        req.am_get_all();
    }
    drop(g);
    world.barrier();
    let g = GLOBAL_G.read();
    let num_nodes = g.num_nodes();
    drop(g);
    println!("{:?} distribute time {:?}", my_pe, now.elapsed());

    println!("{:?} num nodes {:?}", my_pe, num_nodes);
    now = Instant::now();
    let mut sum: f32 = 0.0;

    for node in (my_pe..(num_nodes as usize)).step_by(num_pes) {
        let g = GLOBAL_G.read();
        let neighs = g.neighbors_vec(node);
        let tt = Instant::now();
        world.exec_am_all(TcAM {
            node: node as u32,
            neighs: neighs,
        });
        sum = tt.elapsed().as_secs_f32();
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
