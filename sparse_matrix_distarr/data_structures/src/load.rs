use std::path::Path;


/// Assumes that the file is formatted as
/// ```
/// head0   tail0
/// head1   tail1
/// ...     ...
/// ```
pub fn load_tsv(
        fpath: &str,
    ) 
    ->
    Result< Vec<Vec<u32>>, csv::Error >
    {

    let path = Path::new(&fpath);

    // let mut cur_node = 1;
    // let mut num_edges: usize = 0;
    // let mut num_nodes = 0;

    // let start = std::time::Instant::now();
    // let mut indices;
    // let mut edges = EdgeList::Set(HashSet::new());
    // temp_neighbor_list = vec![];

    let mut temp_neighbor_list: Vec<Vec<u32>>;        

    let delim = b'\t';

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delim)
        .from_path(&path)?;

    let mut vecvec = Vec::new();
        
    for result in rdr.deserialize() {
        let edge: [u32;2] = result?;
        let head = edge[1] as usize;
        let tail = edge[0];
        while vecvec.len() < head + 1 { vecvec.push(Vec::new()) }
        vecvec[head].push(tail)
        // if cur_node != edge.e1 {
        //     num_edges += edges.len();
        //     temp_neighbor_list.push(edges);

        //     cur_node = edge.e1;
        //     num_nodes += 1;
        //     edges = EdgeList::Set(HashSet::new());
        //     if cur_node % 100000 == 0 {
        //         println!("{:?} nodes loaded", cur_node);
        //     }
        // }
        // edges.push(edge.e0 - 1);
    }

    // num_edges += edges.len();
    // temp_neighbor_list.push(edges);
    // num_nodes += 1;
    // indices = (0..num_nodes).collect::<Vec<_>>();
    // indices.sort_by_key(|&i| -(temp_neighbor_list[i].len() as isize));
    //would be nice to do this multithreaded 
    return Ok(vecvec)

}



#[cfg(test)]
pub mod tests {

    use crate::serial;
    use sparsemat as bale;
    use lamellar::LamellarWorldBuilder;    

    use super::*;


    //  ----------------------------------------------------------------
    //  3 X 3 IDENTITY MATRIX
    //  ---------------------------------------------------------------- 


    /// Test Lamellar permutation of the 3x3 identity matrix, where both permutations are identity
    /// 
    /// The test consists of applying the function `test_permutation` to the matrix.
    /// 
    /// This test is primarily intended as a sanity check.
    #[test]
    fn test_load_csv() {
        let x = load_tsv("/Users/roek189/Library/CloudStorage/OneDrive-PNNL/Desktop/a/r/c/l/r/lamellar/lamellar-benchmarks/triangle_count/input_graphs/graph500-scale18-ef16_adj.tsv");
        println!("DONE!!!!");
        assert_eq!(1,2);
    }
}