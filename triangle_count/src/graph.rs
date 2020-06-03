extern crate csv;
extern crate rayon;

use std::cmp;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Mutex};

// type Edge = (u32, u32);

#[derive(Debug,  serde::Deserialize, Eq, PartialEq)]
struct Edge {
    e0: u32,
    e1: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Graph {
    edges: Vec<u32>,
    offsets: Vec<u32>,
    num_nodes: usize,
    my_rank: u32,
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            edges: Vec::new(),
            offsets: Vec::new(),
            num_nodes: 0,
            my_rank: 0,
        }
    }

    #[allow(dead_code)]
    pub fn set_rank(&mut self, rank: u32) {
        self.my_rank = rank;
    }

    pub fn load_tsv(&mut self, fpath: &str) -> Result<(), Box<dyn Error>> {
        if let Ok(_) = self.tab_sep(fpath) { Ok(())}
        else { self.space_sep(fpath) }
    }

    fn tab_sep(&mut self, fpath: &str) -> Result<(), Box<dyn Error>> {
        let path = Path::new(&fpath);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'\t')
            .from_path(&path)?;

        let mut cur_node = 0;
        for result in rdr.deserialize() {
            let edge: Edge = result?;
            if cur_node != edge.e1 {
                self.num_nodes += 1;
                cur_node = edge.e1;
                self.offsets.push(self.edges.len() as u32);
            }
            self.edges.push(edge.e0 - 1);
        }
        self.offsets.push(self.edges.len() as u32);
        println!("{:#?} nodes", self.num_nodes);
        Ok(())
    }

    fn space_sep(&mut self, fpath: &str) -> Result<(), Box<dyn Error>> {
        let path = Path::new(&fpath);
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b' ')
            .from_path(&path)?;

        let mut cur_node = 0;
        for result in rdr.deserialize() {
            let edge: Edge = result?;
            if cur_node != edge.e1 {
                self.num_nodes += 1;
                cur_node = edge.e1;
                self.offsets.push(self.edges.len() as u32);
            }
            self.edges.push(edge.e0 - 1);
        }
        self.offsets.push(self.edges.len() as u32);
        println!("{:#?} nodes", self.num_nodes);
        Ok(())
    }

    pub fn num_nodes(&self) -> usize {
        return self.num_nodes;
    }

    fn calc_offsets(&mut self, degrees: &Vec<u32>) -> Vec<u32> {
        const BLOCK_SIZE: usize = 1 << 20;
        let num_blocks: usize = (self.num_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let mut local_sums = vec![0; num_blocks];
        local_sums.iter_mut().enumerate().for_each(|(block, e)| {
            let mut lsum: usize = 0;
            let block_end: usize = cmp::min((block + 1) * BLOCK_SIZE, self.num_nodes);
            for i in block * BLOCK_SIZE..block_end {
                lsum += degrees[i] as usize;
            }
            *e = lsum;
        });

        let mut bulk_sums = vec![0; num_blocks + 1];
        let mut total = 0;

        for (i, e) in local_sums.iter_mut().enumerate() {
            bulk_sums[i] = total;
            total += *e;
        }
        bulk_sums[num_blocks] = total;
        let mut offsets = vec![0 as u32; self.num_nodes + 1];
        offsets
            .chunks_mut(BLOCK_SIZE)
            .enumerate()
            .for_each(|(block, chunk)| {
                let mut local_total = bulk_sums[block];
                let start_i = block * BLOCK_SIZE;
                for (i, e) in chunk.iter_mut().enumerate() {
                    *e = local_total as u32;
                    if i + start_i < degrees.len() {
                        local_total += degrees[i + start_i] as usize;
                    }
                }
            });
        offsets
    }

    pub fn relabel_csr(&mut self) {
        let mut degree_map = vec![(0, 0); self.num_nodes];
        let offsets_ref = &self.offsets;
        degree_map.iter_mut().enumerate().for_each(|(i, e)| {
            *e = (offsets_ref[i + 1] - offsets_ref[i], i);
        });
        degree_map.sort_unstable_by_key(|k| cmp::Reverse(k.0));

        let mut degrees = vec![0 as u32; self.num_nodes];

        let arc_new_ids = Arc::new(Mutex::new(vec![0 as u32; self.num_nodes]));

        degrees.iter_mut().enumerate().for_each(|(i, e)| {
            *e = degree_map[i].0;
            let new_ids = arc_new_ids.clone();
            let mut c_new_ids = new_ids.lock().unwrap();
            c_new_ids[degree_map[i].1] = i as u32;
        });
        let new_ids = arc_new_ids.clone();
        let c_new_ids = new_ids.lock().unwrap();
        let offsets = self.calc_offsets(&degrees);

        let mut edges = vec![0; self.edges.len()];

        for n in 0..self.num_nodes {
            let mut cur_offset: usize = offsets[c_new_ids[n] as usize] as usize;

            for neigh in self.neighbors(n) {
                edges[cur_offset] = c_new_ids[*neigh as usize];
                cur_offset += 1;
            }
            let slice = &mut edges[offsets[c_new_ids[n] as usize] as usize
                ..offsets[c_new_ids[n] as usize + 1] as usize];
            slice.sort();
        }
        println!("here5");

        self.offsets = offsets;
        self.edges = edges;
    }

    pub fn trim(&mut self) {
        let mut edges = vec![];
        let mut cur_offset = 0;
        let mut offsets = vec![cur_offset];
        for n in 0..self.num_nodes {
            for neigh in self.neighbors_less_than(n as u32) {
                edges.push(neigh);
                cur_offset += 1;
            }
            offsets.push(cur_offset);
        }
        self.offsets = offsets;
        self.edges = edges;
    }

    pub fn get_dist_offsets_and_edges(&self, my_pe:usize, num_pes: usize)->(Vec<Vec<u32>>,Vec<u32>,usize){
        let mut offsets = vec![vec![0];num_pes];
        let mut edges = vec![];
        let mut cur_offsets = vec![0;num_pes];
        for n in 0..self.num_nodes{
            let pe = n % num_pes;
            for neigh in self.neighbors(n){
                if my_pe == pe {
                    edges.push(*neigh)
                }
                cur_offsets[pe]+=1;
            }
            offsets[pe].push(cur_offsets[pe]);
        }
        let max_edges = *cur_offsets.iter().max().unwrap();
        (offsets,edges,max_edges as usize)
    }

    //all my neighbors
    pub fn neighbors(&self, node: usize) -> &[u32] {
        &self.edges[self.offsets[node] as usize..self.offsets[node + 1] as usize]
    }

    pub fn neighbors_vec(&self, node: usize) -> Vec<u32> {
        self.edges[self.offsets[node] as usize..self.offsets[node + 1] as usize].to_vec()
    }

    //get neighbors less than me
    pub fn neighbors_less_than(&self, node: u32) -> Vec<u32> {
        let res = &self.edges
            [self.offsets[node as usize] as usize..self.offsets[node as usize + 1] as usize]
            .binary_search(&node);
        let i = match res {
            Ok(i) => i,
            Err(i) => i,
        };
        // println!(
        //     "node: {:?} o1 {:?} o2 {:?} i {:?} {:?}",
        //     node,
        //     self.offsets[node as usize],
        //     self.offsets[node as usize + 1],
        //     self.offsets[node as usize] + *i as u32,
        //     *i
        // );
        self.edges[self.offsets[node as usize] as usize
            ..(self.offsets[node as usize] + *i as u32) as usize]
            .to_vec()
    }

    #[allow(dead_code)]
    pub fn triangles_for_node_neighs_csr_dist(
        &self,
        n0: u32,
        n0_neigh: &Vec<u32>,
        numranks: u32,
    ) -> u32 {
        let mut cnt = 0;
        for n1 in n0_neigh {
            if n1 % numranks == self.my_rank {
                if n1 > &n0 {
                    break;
                }
                let mut n1_it = 0;
                let n1_neigh = self.neighbors(*n1 as usize);
               
                for n2 in n1_neigh {
                    if n2 > n1 {
                        break;
                    }
                    while n0_neigh[n1_it] < *n2 {
                        n1_it += 1;
                    }
                    if *n2 == n0_neigh[n1_it] {
                        // println!("({:?}, {:?}, {:?})",n0,n1,n2);
                        cnt += 1;
                    }
                }
                // println!("n0: {:?} => {:?}",n0,n0_neigh);
                // println!("n1: {:?} => {:?}",n1,n1_neigh);
            }
        }
        return cnt;
    }
    #[allow(dead_code)]
    pub fn triangles_for_node_neighs_csr_dist2(
        &self,
        nodes: Vec<u32>,
        neighs: Vec<Vec<u32>>,
        numranks: u32,
    ) -> u32 {
        let mut cnt = 0;
        for idx in 0..nodes.len() {
            let n0 = nodes[idx];
            let n0_neigh = &neighs[idx];
            for n1 in n0_neigh {
                if n1 % numranks == self.my_rank {
                    if n1 > &n0 {
                        break;
                    }
                    let mut n1_it = 0;
                    let n1_neigh = self.neighbors(*n1 as usize);
                    for n2 in n1_neigh {
                        if n2 > &n1 {
                            break;
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
        }
        return cnt;
    }
}
