// use csv;
use lamellar::{LamellarTeam, LocalMemoryRegion, RemoteMemoryRegion};
use std::collections::HashMap;
use std::sync::Arc;

// use parking_lot::RwLock;

use crate::GraphOps;
// use crate::Element;

pub struct MapGraph {
    team: Arc<LamellarTeam>,
    neighbors: HashMap<u32, LocalMemoryRegion<u32>>,
    // num_nodes: usize,
}
pub struct MapGraphIter<'a> {
    iter: std::collections::hash_map::Keys<'a, u32, LocalMemoryRegion<u32>>,
}
// pub struct MapGraphRangeIter<'a>{
//     neighbors: &'a HashMap<u32,LocalMemoryRegion<u32>>,
//     cur: usize,
//     end: usize,
// }

// struct DistributeNeighborsAM{
//     node: u32,
//     neighbors: LocalMemoryRegion<u32>
// }

impl MapGraph {
    pub fn new(team: Arc<LamellarTeam>) -> MapGraph {
        MapGraph {
            team: team,
            neighbors: HashMap::new(),
            // num_nodes: 0,
        }
    }
    pub fn iter(&self) -> MapGraphIter<'_> {
        MapGraphIter {
            iter: self.neighbors.keys(),
        }
    }
    // pub fn range_iter(&self,start:usize, end:usize) -> MapGraphRangeIter<'_> {
    //     MapGraphRangeIter{
    //         neighbors: &self.neighbors,
    //         cur: start,
    //         end: end,
    //     }
    // }
}

impl GraphOps for MapGraph {
    fn add_local_neighbors(
        &mut self,
        node: u32,
        neighbors: LocalMemoryRegion<u32>,
    ) -> LocalMemoryRegion<u32> {
        let lmr_neighbors = self.team.alloc_local_mem_region(neighbors.len());
        unsafe {
            let neigh_slice = lmr_neighbors.as_mut_slice().unwrap();
            if neighbors.len() > 0 {
                neigh_slice[neighbors.len() - 1] = std::u32::MAX;
                neighbors.iget(0, lmr_neighbors.clone());
            }
        }
        self.neighbors.insert(node, lmr_neighbors.clone());
        lmr_neighbors
    }
    fn add_remote_neighbors(&mut self, node: u32, neighbors: LocalMemoryRegion<u32>) {
        self.neighbors.insert(node, neighbors);
    }
    fn neighbors(&self, node: &u32) -> std::slice::Iter<'_, u32> {
        if let Some(n) = self.neighbors.get(node) {
            match n.as_slice() {
                Ok(n) => n.iter(),
                Err(_) => panic!(
                    "node {:?} is not local to pe {:?}",
                    node,
                    self.team.world_pe_id()
                ),
            }
        } else {
            panic!("node {:?} does not exist in graph", node);
        }
    }
    fn lamellar_neighbors(&self, node: &u32) -> LocalMemoryRegion<u32> {
        if let Some(n) = self.neighbors.get(node) {
            n.clone()
        } else {
            panic!("node {:?} does not exist in graph", node);
        }
    }

    fn num_nodes(&self) -> usize {
        self.neighbors.len()
    }

    fn node_is_local(&self, node: &u32) -> bool {
        //probably should abstract this out to the graphops trait
        *node as usize % self.team.num_pes() == self.team.team_pe_id().unwrap()
    }
}

impl<'a> Iterator for MapGraphIter<'a> {
    type Item = &'a u32;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| x)
    }
}

// impl <'a> Iterator for MapGraphRangeIter<'a> {
//     type Item = &'a u32;
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.cur == self.end{
//             None
//         }
//         else{
//             let cur = self.cur as u32;
//             self.cur+=1;
//             self.neighbors.get(&cur).map(|x| x)
//         }
//     }
// }

impl Drop for MapGraph {
    fn drop(&mut self) {
        for (_node, lmr) in self.neighbors.drain() {
            drop(lmr)
        }
    }
}
