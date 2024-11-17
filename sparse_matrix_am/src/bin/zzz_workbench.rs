use std::env;

fn main() {
    // Collect the command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the required arguments are provided
    if args.len() != 5 {
        eprintln!("Usage: {} --arg1 <value1> --arg2 <value2>", args[0]);
        std::process::exit(1);
    }

    // Parse the arguments
    let mut arg1 = 0;
    let mut arg2 = 0;

    for i in 1..args.len() {
        if args[i] == "--arg1" {
            arg1 = args[i + 1].parse::<i32>().expect("Invalid value for --arg1");
        } else if args[i] == "--arg2" {
            arg2 = args[i + 1].parse::<i32>().expect("Invalid value for --arg2");
        }
    }

    // Example debug or info statements
    println!("Starting computation with arg1={} and arg2={}", arg1, arg2);

    // Perform some computation with the arguments
    let result = arg1 * arg2; // Example computation

    // More debug or info statements
    println!("Computation completed.");

    // Print the result last
    println!("{}", result);
}



// use prettytable::{Table, row, cell};

// fn main() {
//     // // Define the sparse matrix in the (row_indices, col_indices, coefficients) format
//     // let row_indices = vec![0, 2, 4, 6];
//     // let col_indices = vec![1, 3, 0, 7];
//     // let coefficients = vec![3, 5, -1, 2];
    
//     // // Initialize an 8x8 sparse matrix with zeros
//     // let mut sparse_matrix = vec![vec![0; 8]; 8];
    
//     // // Populate the matrix with the given non-zero values
//     // for i in 0..row_indices.len() {
//     //     let row = row_indices[i];
//     //     let col = col_indices[i];
//     //     let value = coefficients[i];
//     //     sparse_matrix[row][col] = value;
//     // }

//     // // Create a PrettyTable
//     // let mut table = Table::new();
    
//     // // Add the header row
//     // let header_row = row![" ", "0", "1", "2", "3", "4", "5", "6", "7"];
//     // table.add_row(header_row);

//     // // Iterate over rows to fill the table
//     // for (i, row) in sparse_matrix.iter().enumerate() {
//     //     let mut row_vec = vec![cell!(i.to_string())];  // First cell of the row is the row index
//     //     for &value in row {
//     //         row_vec.push(cell!(value.to_string()));
//     //     }
//     //     table.add_row(row_vec);
//     // }

//     // // Print the table to the terminal
//     // table.printstd();
//     println!("2");
// }


