use lamellar::{
    ActiveMessaging, Darc, LamellarTaskGroup, LamellarTeam, LamellarWorld, LocalMemoryRegion,
    LocalRwDarc, RemoteMemoryRegion,
};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
// use std::marker::PhantomData;

use std::collections::BTreeMap;
use std::collections::HashMap;

pub mod mapgraph;
use crate::mapgraph::{MapGraph, MapGraphIter};

pub trait Element:
    'static
    + std::fmt::Debug
    + Clone
    + Send
    + Sync
    + serde::ser::Serialize
    + for<'de> serde::Deserialize<'de>
{
}
impl<
        T: 'static
            + std::fmt::Debug
            + Clone
            + Send
            + Sync
            + serde::ser::Serialize
            + for<'de> serde::Deserialize<'de>,
    > Element for T
{
}

#[derive(Debug, serde::Deserialize, Eq, PartialEq)]
struct Edge {
    e0: u32,
    e1: u32,
}

trait GraphOps {
    fn add_local_neighbors(
        &mut self,
        node: u32,
        neighbors: LocalMemoryRegion<u32>,
    ) -> LocalMemoryRegion<u32>;
    fn add_remote_neighbors(&mut self, node: u32, neighbors: LocalMemoryRegion<u32>);
    fn neighbors(&self, node: &u32) -> std::slice::Iter<'_, u32>;
    fn lamellar_neighbors(&self, node: &u32) -> LocalMemoryRegion<u32>;
    fn num_nodes(&self) -> usize;
    fn node_is_local(&self, node: &u32) -> bool;
}

pub enum GraphType {
    MapGraph,
}

pub enum GraphData {
    MapGraph(MapGraph),
}

pub enum GraphIter<'a> {
    MapGraph(MapGraphIter<'a>),
}

impl<'a> Iterator for GraphIter<'a> {
    type Item = &'a u32;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            GraphIter::MapGraph(iter) => iter.next(),
        }
    }
}

impl GraphOps for GraphData {
    fn add_local_neighbors(
        &mut self,
        node: u32,
        neighbors: LocalMemoryRegion<u32>,
    ) -> LocalMemoryRegion<u32> {
        match self {
            GraphData::MapGraph(graph) => graph.add_local_neighbors(node, neighbors),
        }
    }
    fn add_remote_neighbors(&mut self, node: u32, neighbors: LocalMemoryRegion<u32>) {
        match self {
            GraphData::MapGraph(graph) => graph.add_remote_neighbors(node, neighbors),
        }
    }
    fn neighbors(&self, node: &u32) -> std::slice::Iter<'_, u32> {
        match self {
            GraphData::MapGraph(graph) => graph.neighbors(node),
        }
    }
    fn lamellar_neighbors(&self, node: &u32) -> LocalMemoryRegion<u32> {
        match self {
            GraphData::MapGraph(graph) => graph.lamellar_neighbors(node),
        }
    }
    fn num_nodes(&self) -> usize {
        match self {
            GraphData::MapGraph(graph) => graph.num_nodes(),
        }
    }
    fn node_is_local(&self, node: &u32) -> bool {
        match self {
            GraphData::MapGraph(graph) => graph.node_is_local(node),
        }
    }
}

impl GraphData {
    pub fn iter(&self) -> GraphIter<'_> {
        match self {
            GraphData::MapGraph(graph) => GraphIter::MapGraph(graph.iter()),
        }
    }
    pub fn neighbors_iter(&self, node: &u32) -> std::slice::Iter<'_, u32> {
        match self {
            GraphData::MapGraph(graph) => graph.neighbors(node),
        }
    }
    pub fn local_neighbors(&self, node: &u32) -> LocalMemoryRegion<u32> {
        match self {
            GraphData::MapGraph(graph) => graph.lamellar_neighbors(node),
        }
    }
    pub fn node_is_local(&self, node: &u32) -> bool {
        match self {
            GraphData::MapGraph(graph) => graph.node_is_local(node),
        }
    }
}

#[lamellar::AmLocalData]
struct RelabelMapAm {
    start_index: usize,
    nodes: Vec<u32>,
    relabeled: LocalMemoryRegion<u32>,
}
#[lamellar::local_am]
impl LamellarAM for RelabelMapAm {
    async fn exec() {
        let relabled = unsafe { self.relabeled.as_mut_slice().unwrap() };
        for (i, node) in self.nodes.iter().enumerate() {
            relabled[*node as usize] = (i + self.start_index) as u32;
        }
    }
}

#[lamellar::AmLocalData]
struct RelabelAm {
    nodes: Vec<(Vec<u32>, LocalMemoryRegion<u32>, usize)>,
    relabeled: LocalMemoryRegion<u32>,
}
#[lamellar::local_am]
impl LamellarAM for RelabelAm {
    async fn exec() {
        let relabled = self.relabeled.as_slice().unwrap();
        for nodes in &self.nodes {
            let old_nodes = &nodes.0;
            let new_nodes = unsafe { nodes.1.as_mut_slice().unwrap() };
            for i in 0..old_nodes.len() {
                new_nodes[i] = relabled[old_nodes[i] as usize];
            }
            new_nodes.sort_unstable();
        }
    }
}

#[lamellar::AmData]
struct LocalNeighborsAM {
    graph: LocalRwDarc<GraphData>,
    node_and_neighbors: Vec<(u32, LocalMemoryRegion<u32>)>,
}

#[lamellar::am]
impl LamellarAM for LocalNeighborsAM {
    async fn exec() {
        let mut graph = self.graph.write();
        let mut remotes: Vec<(u32, LocalMemoryRegion<u32>)> = vec![];
        for (node, neighbors) in &self.node_and_neighbors {
            remotes.push((*node, graph.add_local_neighbors(*node, neighbors.clone())));
        }
        lamellar::world.exec_am_all(RemoteNeighborsAM {
            graph: self.graph.clone(),
            node_and_neighbors: remotes,
        });
    }
}

#[lamellar::AmData]
struct RemoteNeighborsAM {
    graph: LocalRwDarc<GraphData>,
    node_and_neighbors: Vec<(u32, LocalMemoryRegion<u32>)>,
}
#[lamellar::am]
impl LamellarAM for RemoteNeighborsAM {
    async fn exec() {
        let mut graph = self.graph.write();
        for (node, neighbors) in &self.node_and_neighbors {
            graph.add_remote_neighbors(*node, neighbors.clone());
        }
    }
}

#[derive(Clone)]
pub struct Graph {
    graph: Darc<GraphData>,
    world: LamellarWorld,
    pub my_pe: usize,
}

impl Graph {
    pub fn new(fpath: &str, graph_type: GraphType, world: LamellarWorld) -> Graph {
        let my_pe = world.my_pe();
        let graph = match graph_type {
            _map_graph => GraphData::MapGraph(MapGraph::new(world.team().clone())),
        };
        let graph = LocalRwDarc::try_new(world.team(), graph).unwrap(); // we are creating with the world team so should be valid on all pes

        Graph::load(fpath, &world, &graph).expect("error reading graph");
        if my_pe == 0 {
            println!("Done loading graph!");
        }
        let g = Graph {
            world: world,
            graph: graph.into_darc(),
            my_pe: my_pe,
        };
        if my_pe == 0 {
            println!("Done creating graph!");
        }
        g.barrier();
        g
    }

    fn load(
        fpath: &str,
        world: &LamellarWorld,
        graph: &LocalRwDarc<GraphData>,
    ) -> Result<(), Box<dyn Error>> {
        if world.my_pe() == 0 {
            if !Graph::parse(fpath, b'\t', world, graph).is_ok() {
                Graph::parse(fpath, b' ', world, graph)?;
            }
        }
        world.barrier();
        Ok(())
    }

    fn parse(
        fpath: &str,
        delim: u8,
        world: &LamellarWorld,
        graph: &LocalRwDarc<GraphData>,
    ) -> Result<usize, Box<dyn Error>> {
        let path = Path::new(&fpath);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delim)
            .from_path(&path)?;

        let mut num_neighbors_sorted: BTreeMap<u32, Vec<u32>> = BTreeMap::new(); //sorts nodes by number of neighbors

        let mut temp_neighbor_list: Vec<Vec<u32>> = vec![];

        let mut cur_node = 1;
        let mut edges = vec![];
        let mut num_edges = 0;
        let mut num_nodes = 0;
        for result in rdr.deserialize() {
            let edge: Edge = result?;
            if cur_node != edge.e1 {
                num_edges += edges.len();
                num_neighbors_sorted
                    .entry(edges.len() as u32)
                    .or_insert(vec![])
                    .push(cur_node - 1);
                temp_neighbor_list.push(edges);

                cur_node = edge.e1;
                num_nodes += 1;
                edges = vec![];
                if cur_node % 100000 == 0 {
                    println!("{:?} nodes loaded", cur_node);
                }
            }
            edges.push(edge.e0 - 1);
        }
        num_neighbors_sorted
            .entry(edges.len() as u32)
            .or_insert(vec![])
            .push(cur_node - 1);
        num_edges += edges.len();
        temp_neighbor_list.push(edges);
        num_nodes += 1;

        println!(
            "num_nodes {:?} {:?} num_edges {:?}",
            num_nodes,
            temp_neighbor_list.len(),
            num_edges
        );

        let relabeled = world.alloc_local_mem_region::<u32>(num_nodes);

        let mut end_index = num_nodes;
        let start = std::time::Instant::now();
        for (_amt, nodes) in num_neighbors_sorted.iter_mut() {
            let mut new_nodes = vec![];
            std::mem::swap(nodes, &mut new_nodes);
            let start_index = end_index - new_nodes.len();
            world.exec_am_local(RelabelMapAm {
                start_index: start_index,
                nodes: new_nodes,
                relabeled: relabeled.clone(),
            });
            end_index = start_index;
        }
        println!(
            "reorder map issue time: {:?}",
            start.elapsed().as_secs_f64()
        );

        world.wait_all();
        println!("reorder map time: {:?}", start.elapsed().as_secs_f64());

        let mut temp_nodes = vec![];
        let mut neigh_list = vec![];
        let mut size = 0;
        let mut i = 0;
        for nodes in temp_neighbor_list.drain(..) {
            if size > num_edges / 10 {
                world.exec_am_local(RelabelAm {
                    nodes: temp_nodes,
                    relabeled: relabeled.clone(),
                });
                temp_nodes = vec![];
                size = 0;
            }
            let nodes_len = nodes.len();
            size += nodes_len;
            let temp = world.alloc_local_mem_region::<u32>(nodes_len);
            neigh_list.push(temp.clone());
            temp_nodes.push((nodes, temp, i));
            i += 1;
        }
        if size > 0 {
            world.exec_am_local(RelabelAm {
                nodes: temp_nodes,
                relabeled: relabeled.clone(),
            });
        }
        println!("reorder issue time: {:?}", start.elapsed().as_secs_f64());
        world.wait_all();
        println!("reorder  time: {:?}", start.elapsed().as_secs_f64());
        let task_group = LamellarTaskGroup::new(world.team());
        let mut pe_neigh_lists: HashMap<usize, Vec<(u32, LocalMemoryRegion<u32>)>> = HashMap::new();
        for pe in 0..world.num_pes() {
            pe_neigh_lists.insert(pe, vec![]);
        }
        for old_node in 0..neigh_list.len() {
            let new_node = relabeled.as_slice().unwrap()[old_node] as usize;
            let pe = new_node % world.num_pes();
            pe_neigh_lists
                .get_mut(&pe)
                .unwrap()
                .push((new_node as u32, neigh_list[old_node].clone()));
        }
        for (pe, neigh_lists) in pe_neigh_lists.iter_mut() {
            task_group.exec_am_pe(
                *pe,
                LocalNeighborsAM {
                    graph: graph.clone(),
                    node_and_neighbors: neigh_lists.clone(),
                },
            );
        }
        neigh_list.clear();
        println!("distribute issue time: {:?}", start.elapsed().as_secs_f64());
        world.wait_all();
        println!("distribute time: {:?}", start.elapsed().as_secs_f64());
        Ok(num_nodes)
    }

    pub fn data(&self) -> Darc<GraphData> {
        self.graph.clone()
    }

    pub fn neighbors(&self, node: &u32) -> std::slice::Iter<'_, u32> {
        self.graph.neighbors(node)
    }

    pub fn iter(&self) -> GraphIter<'_> {
        self.graph.iter()
    }

    pub fn barrier(&self) {
        self.world.barrier();
    }

    pub fn team(&self) -> Arc<LamellarTeam> {
        self.world.team()
    }

    pub fn num_pes(&self) -> usize {
        self.world.num_pes()
    }

    pub fn my_pe(&self) -> usize {
        self.world.my_pe()
    }

    pub fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    pub fn node_is_local(&self, node: &u32) -> bool {
        //probably should abstract this out to the graphops trait
        *node as usize % self.num_pes() == self.my_pe()
    }
}
