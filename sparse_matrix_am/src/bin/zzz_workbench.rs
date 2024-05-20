use prettytable::{Table, row, cell};

fn main() {
    // // Define the sparse matrix in the (row_indices, col_indices, coefficients) format
    // let row_indices = vec![0, 2, 4, 6];
    // let col_indices = vec![1, 3, 0, 7];
    // let coefficients = vec![3, 5, -1, 2];
    
    // // Initialize an 8x8 sparse matrix with zeros
    // let mut sparse_matrix = vec![vec![0; 8]; 8];
    
    // // Populate the matrix with the given non-zero values
    // for i in 0..row_indices.len() {
    //     let row = row_indices[i];
    //     let col = col_indices[i];
    //     let value = coefficients[i];
    //     sparse_matrix[row][col] = value;
    // }

    // // Create a PrettyTable
    // let mut table = Table::new();
    
    // // Add the header row
    // let header_row = row![" ", "0", "1", "2", "3", "4", "5", "6", "7"];
    // table.add_row(header_row);

    // // Iterate over rows to fill the table
    // for (i, row) in sparse_matrix.iter().enumerate() {
    //     let mut row_vec = vec![cell!(i.to_string())];  // First cell of the row is the row index
    //     for &value in row {
    //         row_vec.push(cell!(value.to_string()));
    //     }
    //     table.add_row(row_vec);
    // }

    // // Print the table to the terminal
    // table.printstd();
}
