
use ordered_float::OrderedFloat;
use sprs::{TriMat, CsMat};

// eats a reference to a vec and returns a vec of strings where all floats have exactly four significant figures
pub fn format_f64_vec_as_string( floats: &Vec<f64> ) -> String {

    // Specify the number of significant figures
    let significant_figures = 4;

    // Collect formatted strings
    return floats.iter()
        .map(|num| format!("{:.1$e}", num, significant_figures - 1))
        .collect::<Vec<_>>()
        .join(" ")
}


// eats a reference to a vec and returns a vec of strings where all floats have exactly four significant figures
pub fn format_ofloat_vec_as_string( floats: &Vec<OrderedFloat<f64>> ) -> String {

    // Specify the number of significant figures
    let significant_figures = 4;

    // Collect formatted strings
    return floats.iter()
        .map(|num| format!("{:.1$e}", num.into_inner(), significant_figures - 1))
        .collect::<Vec<_>>()
        .join(" ")
}






/// Converts a `sprs::TriMat<OrderedFloat<f64>>` to a dense vec-of-vec representation
pub fn to_dense(triplet: &TriMat<OrderedFloat<f64>>) -> Vec<Vec<f64>> {
    let shape = triplet.shape();
    let mut dense = vec![vec![0.0; shape.1]; shape.0];
    for (&val, (row, col) ) in triplet.triplet_iter() {
        dense[row][col] = val.into_inner();
    }
    dense
}

// /// Formats a 2D vector representing a dense matrix into a string with aligned columns.
// ///
// /// This function ensures that the elements in each column are aligned based on the 
// /// widest element in the matrix. Each row of the matrix is joined with spaces, and 
// /// rows are separated by newlines. The resulting string is trimmed of any trailing 
// /// newlines for clean output.
// ///
// /// # Arguments
// ///
// /// * `matrix` - A reference to a 2D vector (`Vec<Vec<f64>>`) representing the dense matrix.
// ///
// /// # Returns
// ///
// /// A `String` where each line represents a row of the matrix, with columns aligned.
// pub fn format_aligned_dense(matrix: &[Vec<f64>]) -> String {
//     // Find the widest element for column alignment
//     let max_width = matrix.iter()
//         .flat_map(|row| row.iter())
//         .map(|val| format!("{:.3}", val).len())
//         .max()
//         .unwrap_or(0);

//     // Build the formatted string
//     let mut result = String::new();
//     for row in matrix {
//         let row_string = row.iter()
//             .map(|val| format!("{:>width$.3}", val, width = max_width))
//             .collect::<Vec<String>>()
//             .join(" ");
//         result.push_str(&row_string);
//         result.push('\n');
//     }
//     result.trim_end().to_string() // Trim the trailing newline
// }


/// Formats a 2D vector representing a dense matrix into a string with aligned columns.
///
/// If `suppress_zeros` is true, zero entries are replaced with spaces to maintain alignment.
///
/// # Arguments
///
/// * `matrix` - A reference to a 2D vector (`Vec<Vec<f64>>`) representing the dense matrix.
/// * `suppress_zeros` - A boolean flag. If true, zero entries are replaced with spaces.
///
/// # Returns
///
/// A `String` where each line represents a row of the matrix, with columns aligned.
///
/// # Example
///
/// ```
/// let dense = vec![
///     vec![1.0, 0.0, 3.14],
///     vec![0.0, 123.456, 0.0],
///     vec![0.0, 0.0, 0.0],
/// ];
///
/// let matrix_string = format_aligned_dense(&dense, true);
///
/// assert_eq!(matrix_string, "  1.000              3.140\n        123.456          \n                        ");
/// ```
pub fn format_aligned_dense(matrix: &[Vec<f64>], suppress_zeros: bool) -> String {
    // Find the widest element for column alignment
    let max_width = matrix.iter()
        .flat_map(|row| row.iter())
        .map(|val| format!("{:.3}", val).len())
        .max()
        .unwrap_or(0);

    // Build the formatted string
    let mut result = String::new();
    for row in matrix {
        let row_string = row.iter()
            .map(|&val| {
                if suppress_zeros && val == 0.0 {
                    // Replace zero with spaces of the same width
                    " ".repeat(max_width)
                } else {
                    format!("{:>width$.3}", val, width = max_width)
                }
            })
            .collect::<Vec<String>>()
            .join(" ");
        result.push_str(&row_string);
        result.push('\n');
    }
    result.trim_end().to_string() // Trim the trailing newline
}
