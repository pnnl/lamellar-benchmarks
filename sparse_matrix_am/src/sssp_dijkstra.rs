use std::collections::BinaryHeap;
use std::cmp::Ordering;
use ordered_float::OrderedFloat;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Edge {
    target: usize,
    weight: OrderedFloat<f64>,
}

#[derive(Debug, PartialEq, Eq)]
struct State {
    node: usize,
    cost: OrderedFloat<f64>,
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn dijkstra(graph: &[Vec<Edge>]) -> Vec<OrderedFloat<f64>> {
    let n = graph.len();
    let mut dist: Vec<OrderedFloat<f64>> = vec![OrderedFloat(f64::INFINITY); n];
    let mut heap = BinaryHeap::new();

    dist[0] = OrderedFloat(0.0);
    heap.push(State { node: 0, cost: OrderedFloat(0.0) });

    while let Some(State { node, cost }) = heap.pop() {
        if cost > dist[node] {
            continue;
        }

        for edge in &graph[node] {
            let next = State { node: edge.target, cost: cost + edge.weight };
            if next.cost < dist[next.node] {
                dist[next.node] = next.cost;
                heap.push(next);
            }
        }
    }

    dist
}


/// Solves the single source shortest path problem, for node 0.
///
/// Input is a directed adjacency matrix formatted such that
/// the graph has a directed edge `(indices_row[p], indices_col[p])` with
/// weight `p` for all `p`.
pub fn dijkstra_from_row_col_weight( 
        indices_row:    & Vec<usize>, 
        indices_col:    & Vec<usize>, 
        weights:        & Vec<f64>,
        num_vertices:   usize,
    )  ->  Vec<OrderedFloat<f64>>
{
    println!("indices_row {:?}", indices_row );
    println!("indices_col {:?}", indices_col );    
    println!("weights {:?}", weights );        
    let mut graph: Vec<Vec<Edge>> = vec![ vec![] ; num_vertices ];
    for p in 0 .. indices_row.len() {        
        let source  =   indices_row[p].clone();
        let target  =   indices_col[p].clone();
        let weight  =   OrderedFloat(weights[p].clone());
        graph[ source ].push( Edge{ target, weight } )
    }
    return dijkstra( &graph )
}    

// fn main() {
//     let graph = vec![
//         vec![Edge { target: 1, weight: OrderedFloat(4.0) }, Edge { target: 2, weight: OrderedFloat(1.0) }],
//         vec![Edge { target: 3, weight: OrderedFloat(1.0) }],
//         vec![Edge { target: 1, weight: OrderedFloat(2.0) }, Edge { target: 3, weight: OrderedFloat(5.0) }],
//         vec![],
//     ];

//     let shortest_paths = dijkstra(&graph);
//     println!("Shortest paths from node 0:");
//     for (node, dist) in shortest_paths.iter().enumerate() {
//         println!("Node {}: {}", node, if dist.0 == f64::INFINITY { "INF" } else { &dist.to_string() });
//     }
// }