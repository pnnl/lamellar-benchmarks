

use petgraph::Graph;
use petgraph::algo::bellman_ford;
use petgraph::prelude::*;

use itertools::izip;
use std::any::type_name;


pub fn dijkstra( num_nodes: usize, indices_row: &Vec<usize>, indices_col: &Vec<usize>, weights: &Vec<f64>, ) -> Vec< f64 > {
    
    let mut g           =   Graph::<(), f64>::new();

    for p in 0 .. num_nodes {
        g.add_node(());
    }


    // g.extend_with_edges( izip!(indices_row, indices_col, weights) );

    let v = vec![
                (0u32, 1, 2.0),
                (0, 3, 4.0),
                (1, 2, 1.0),
                (1, 5, 7.0),
                (2, 4, 5.0),
                (4, 5, 1.0),
                (3, 4, 1.0),
            ]; 

    let v: Vec<(u32, u32, f64)> = indices_row
                                    .iter()
                                    .zip(indices_col.iter())
                                    .zip(weights.iter())
                                    .map(|((&a_val, &b_val), &c_val)| (a_val as u32, b_val as u32, c_val))
                                    .collect();            

    // let a: () = v[0].0;

    g.extend_with_edges(&v);            

    // g.extend_with_edges(&[
    //     (0, 1, 2.0),
    //     (0, 3, 4.0),
    //     (1, 2, 1.0),
    //     (1, 5, 7.0),
    //     (2, 4, 5.0),
    //     (4, 5, 1.0),
    //     (3, 4, 1.0),
    // ]);    

    // for (edge_num, index_row) in indices_row.iter().cloned().enumerate() {
    //     let index_col   =   indices_col[ edge_num ].clone();
    //     let weight      =   weights[     edge_num ].clone();
    //     g.add_edge( index_row, index_col, weight )
    // }

    bellman_ford(&g, 0.into() )
        .unwrap()
        .distances
        .clone()

}

// let mut g               =   Graph::new();
// let a = g.add_node(()); // node with no weight
// let b = g.add_node(());
// let c = g.add_node(());
// let d = g.add_node(());
// let e = g.add_node(());
// let f = g.add_node(());
// g.extend_with_edges(&[
//     (0, 1, 2.0),
//     (0, 3, 4.0),
//     (1, 2, 1.0),
//     (1, 5, 7.0),
//     (2, 4, 5.0),
//     (4, 5, 1.0),
//     (3, 4, 1.0),
// ]);

// // Graph represented with the weight of each edge
// //
// //     2       1
// // a ----- b ----- c
// // | 4     | 7     |
// // d       f       | 5
// // | 1     | 1     |
// // \------ e ------/

// let path = bellman_ford(&g, a);
// assert!(path.is_ok());
// let path = path.unwrap();
// assert_eq!(path.distances, vec![    0.0,     2.0,    3.0,    4.0,     5.0,     6.0]);
// assert_eq!(path.predecessors, vec![None, Some(a),Some(b),Some(a), Some(d), Some(e)]);

// // Node f (indice 5) can be reach from a with a path costing 6.
// // Predecessor of f is Some(e) which predecessor is Some(d) which predecessor is Some(a).
// // Thus the path from a to f is a <-> d <-> e <-> f

// let graph_with_neg_cycle = Graph::<(), f32, Undirected>::from_edges(&[
//         (0, 1, -2.0),
//         (0, 3, -4.0),
//         (1, 2, -1.0),
//         (1, 5, -25.0),
//         (2, 4, -5.0),
//         (4, 5, -25.0),
//         (3, 4, -1.0),
// ]);

// assert!(bellman_ford(&graph_with_neg_cycle, NodeIndex::new(0)).is_err());


fn example() {
    let mut g = Graph::new();
    let a = g.add_node(()); // node with no weight
    let b = g.add_node(());
    let c = g.add_node(());
    let d = g.add_node(());
    let e = g.add_node(());
    let f = g.add_node(());
    g.extend_with_edges(&[
        (0, 1, 2.0),
        (0, 3, 4.0),
        (1, 2, 1.0),
        (1, 5, 7.0),
        (2, 4, 5.0),
        (4, 5, 1.0),
        (3, 4, 1.0),
    ]);

    // Graph represented with the weight of each edge
    //
    //     2       1
    // a ----- b ----- c
    // | 4     | 7     |
    // d       f       | 5
    // | 1     | 1     |
    // \------ e ------/

    let path = bellman_ford(&g, a);
    assert!(path.is_ok());
    let path = path.unwrap();
    assert_eq!(path.distances, vec![    0.0,     2.0,    3.0,    4.0,     5.0,     6.0]);
    assert_eq!(path.predecessors, vec![None, Some(a),Some(b),Some(a), Some(d), Some(e)]);

    // Node f (indice 5) can be reach from a with a path costing 6.
    // Predecessor of f is Some(e) which predecessor is Some(d) which predecessor is Some(a).
    // Thus the path from a to f is a <-> d <-> e <-> f

    let graph_with_neg_cycle = Graph::<(), f32, Undirected>::from_edges(&[
            (0, 1, -2.0),
            (0, 3, -4.0),
            (1, 2, -1.0),
            (1, 5, -25.0),
            (2, 4, -5.0),
            (4, 5, -25.0),
            (3, 4, -1.0),
    ]);

    assert!(bellman_ford(&graph_with_neg_cycle, NodeIndex::new(0)).is_err());
}


// use petgraph::graph::{DiGraph, NodeIndex};
// use petgraph::algo::dijkstra;
// use std::collections::HashMap;

// fn chat_example(num_nodes: usize, indices_row: Vec<usize>, indices_col: Vec<usize>, weights: Vec<f64>) -> Vec<f64> {
//     // Create a directed graph
//     let mut graph = DiGraph::<(), f64>::new();

//     // Add nodes to the graph
//     let mut node_indices: HashMap<(usize, usize), NodeIndex> = HashMap::new();
//     for i in 0..num_nodes {
//         let node_index = graph.add_node(());
//         node_indices.insert((i, i), node_index);
//     }

//     // Add edges to the graph
//     for (i, &row) in indices_row.iter().enumerate() {
//         let col = indices_col[i];
//         let weight = weights[i];
//         let src_node = *node_indices.get(&(row, row)).unwrap();
//         let dest_node = *node_indices.get(&(col, col)).unwrap();
//         graph.add_edge(src_node, dest_node, weight);
//     }

//     // Perform Dijkstra's algorithm
//     let source_node = *node_indices.get(&(0, 0)).unwrap(); // Assuming the source node is the first node
//     let result = dijkstra(&graph, source_node, None, |e| *e.weight());

//     // Extract distances from the result
//     let mut distances = vec![f64::INFINITY; num_nodes];
//     for (node_index, distance) in result {
//         distances[node_index.index()] = distance;
//     }

//     distances
// }

// fn main() {
//     let num_nodes = 4;
//     let indices_row = vec![0, 0, 1, 1, 2, 2, 3, 3];
//     let indices_col = vec![1, 2, 2, 3, 0, 3, 0, 1];
//     let weights = vec![1.0, 2.0, 1.0, 3.0, 1.0, 2.0, 2.0, 3.0];

//     let distances = dijkstra(num_nodes, indices_row, indices_col, weights);
//     println!("Distances from source node: {:?}", distances);
// }