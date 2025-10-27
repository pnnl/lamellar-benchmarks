# Benchmark record utility



## Example (basic) use:
-  Create a record at the start of the program: `let mut result_record = benchmark_record::BenchmarkInformation::new();`
- Record results as they become available: `result_record.with_output("result_name", "value as a string");`
- Save to disk: `result_record.write(&benchmark_record::default_output_path());`
- Print to screen: `result_record.display(Some(3));`

The `result_record` gathers the contextual information at construction time. 
If some information is *required*, check for it after construction.
The record acts as an accumulator of result information using the `.with_output` method.
If you write to same key twice, the first value with be lost!

The `default_output_path` function will return a name based on the current executable and timestamp.
It writes in a JSON lines format and appends values (so you can write multiple times from the same program).
The `display` function prints to the screen.  The optional integer value controls the indenentation (`None` prints on one line).