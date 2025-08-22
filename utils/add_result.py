#!/usr/bin/env python3
"""
Script to add benchmark results to a benchmark results repository.
- Takes benchmark results from stdin or a file
- Uses git short hash from the benchmark data itself
- Organizes results by benchmark type and build type (release/debug)
- Appends to existing files with the same hash
"""
import argparse
import json
import os
import sys

def read_input(input_file=None):
    """Read input from file or stdin."""
    if input_file:
        try:
            with open(input_file, 'r') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if not sys.stdin.isatty():
            return sys.stdin.read()
        else:
            print("Error: No input provided. Please pipe data to this script or use --input-file", file=sys.stderr)
            sys.exit(1)

def parse_json_input(content):
    """Parse JSON or JSONL input."""
    if not content.strip():
        print("Error: Empty input", file=sys.stderr)
        sys.exit(1)
    
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    
    results = []
    for i, line in enumerate(content.strip().split('\n')):
        if not line.strip():
            continue
        try:
            results.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON at line {i+1}: {e}", file=sys.stderr)
            print(f"Line content: {line}", file=sys.stderr)
            sys.exit(1)
    
    if not results:
        print("Error: No valid JSON found in input", file=sys.stderr)
        sys.exit(1)
    
    return results

def extract_git_hash(data):
    """Extract git hash from benchmark data."""
    if isinstance(data, list):
        for item in data:
            hash_value = extract_git_hash_from_item(item)
            if hash_value:
                return hash_value
        print("Warning: Could not find git hash in any of the provided benchmark results.", file=sys.stderr)
        return "unknown"
    else:
        hash_value = extract_git_hash_from_item(data)
        if hash_value:
            return hash_value
        else:
            print("Warning: Could not find git hash in the benchmark result.", file=sys.stderr)
            return "unknown"

def extract_git_hash_from_item(item):
    """Extract git hash from a single benchmark record."""
    try:
        if (isinstance(item, dict) and
            "_metadata" in item and
            "git_info" in item["_metadata"] and
            "commit_hash_short" in item["_metadata"]["git_info"]):
            hash_value = item["_metadata"]["git_info"]["commit_hash_short"]
            if hash_value:
                return hash_value
    except (KeyError, TypeError):
        pass
    return None

def add_result(benchmarks_results_root, benchmark_name, build_type, data):
    """Add a benchmark result to the repository."""
    git_hash = extract_git_hash(data)
    
    benchmark_dir = os.path.join(benchmarks_results_root, benchmark_name, build_type)
    os.makedirs(benchmark_dir, exist_ok=True)
    
    filename = f"{benchmark_name}-{git_hash}.jsonl"
    filepath = os.path.join(benchmark_dir, filename)
    
    if os.path.exists(filepath):
        print(f"File {filepath} already exists. Appending...", file=sys.stderr)
        try:
            with open(filepath, 'r') as file:
                existing_content = file.read().strip()
            
            with open(filepath, 'a') as file:
                if existing_content and not existing_content.endswith('\n'):
                    file.write('\n')
                
                if isinstance(data, list):
                    for item in data:
                        file.write(json.dumps(item) + '\n')
                else:
                    file.write(json.dumps(data) + '\n')
            
            print(f"Successfully appended to {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Error appending to file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Creating new file {filepath}", file=sys.stderr)
        try:
            with open(filepath, 'w') as file:
                if isinstance(data, list):
                    for item in data:
                        file.write(json.dumps(item) + '\n')
                else:
                    file.write(json.dumps(data) + '\n')
            
            print(f"Successfully created {filepath}", file=sys.stderr)
        except Exception as e:
            print(f"Error creating file: {e}", file=sys.stderr)
            sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Add benchmark results to a repository.")
    parser.add_argument("--benchmarks-results-root", required=True,
                      help="Path to the benchmarks results repository.")
    parser.add_argument("--benchmark-name", required=True,
                      help="Name of the benchmark (e.g., 'histo', 'randperm').")
    parser.add_argument("--build-type", required=True, choices=["release", "debug"],
                      help="Build type of the benchmark (release or debug).")
    parser.add_argument("--input-file",
                      help="Path to input file containing benchmark results. "
                           "If not provided, read from stdin.")
    
    args = parser.parse_args()
    
    content = read_input(args.input_file)
    data = parse_json_input(content)
    
    add_result(args.benchmarks_results_root, args.benchmark_name, args.build_type, data)

if __name__ == "__main__":
    main()