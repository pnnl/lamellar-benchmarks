



use serde_json::to_writer;
use std::env;
use std::fs::File;
use std::io::Result;

fn main() {
    // Example vector
    let data = vec![1, 2, 3, 4, 5];

    // Write the vector to a JSON file
    if let Err(err) = write_to_json_file("output.json", &data) {
        eprintln!("Error: {}", err);
    }
}

fn write_to_json_file(filename: &str, data: &[i32]) -> Result<()> {
    // Get the current directory
    let current_dir = env::current_dir()?;

    // Construct the path to the JSON file relative to the current directory
    let file_path = current_dir.join(filename);

    // Create a new file at the specified path
    let file = File::create(file_path)?;

    // Serialize the data to JSON and write it to the file
    to_writer(file, data)?;

    Ok(())
}
