#!/usr/bin/env python3
"""
Filter stdin to output only lines containing valid JSON objects or arrays.
Ignores JSON primitives (numbers, strings, booleans) unless they appear alone on a line.
"""
import json
import sys
import argparse

def is_valid_json_object_or_array(text):
    """Check if text contains a valid JSON object or array (not primitives)."""
    text = text.strip()
    if not text:
        return False
    
    if not (text.startswith('{') or text.startswith('[')):
        return False
    
    try:
        parsed = json.loads(text)
        return isinstance(parsed, (dict, list))
    except (json.JSONDecodeError, ValueError):
        return False

def extract_json_objects(input_stream, output_stream):
    """Extract JSON objects and arrays from input, handling multi-line JSON."""
    current_json = ""
    brace_count = 0
    bracket_count = 0
    in_string = False
    escape_next = False
    started_json = False
    
    for line in input_stream:
        line = line.rstrip('\n\r')
        
        if is_valid_json_object_or_array(line):
            try:
                parsed = json.loads(line)
                compact_json = json.dumps(parsed, separators=(',', ':'))
                output_stream.write(compact_json)
                output_stream.write('\n')
            except:
                output_stream.write(line)
                output_stream.write('\n')
            continue
        
        for char in line:
            if escape_next:
                escape_next = False
                current_json += char
                continue
                
            if char == '\\' and in_string:
                escape_next = True
                current_json += char
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                current_json += char
                continue
                
            if not in_string:
                if char == '{':
                    if brace_count == 0 and bracket_count == 0:
                        current_json = ""
                        started_json = True
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                elif char == '[':
                    if brace_count == 0 and bracket_count == 0:
                        current_json = ""
                        started_json = True
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
            
            current_json += char
            
            if (not in_string and brace_count == 0 and bracket_count == 0 and 
                current_json.strip() and started_json):
                if is_valid_json_object_or_array(current_json):
                    try:
                        parsed = json.loads(current_json)
                        compact_json = json.dumps(parsed, separators=(',', ':'))
                        output_stream.write(compact_json)
                        output_stream.write('\n')
                    except:
                        output_stream.write(current_json.strip())
                        output_stream.write('\n')
                current_json = ""
                started_json = False

def filter_json_lines(input_stream=None, output_stream=None):
    """Filter input stream to output only valid JSON objects and arrays."""
    if input_stream is None:
        input_stream = sys.stdin
    if output_stream is None:
        output_stream = sys.stdout
    
    try:
        extract_json_objects(input_stream, output_stream)
    except KeyboardInterrupt:
        sys.exit(0)
    except BrokenPipeError:
        sys.exit(0)

def main():
    parser = argparse.ArgumentParser(
        description='Filter stdin to output only lines containing valid JSON objects or arrays',
        epilog='''
        Examples:
        %(prog)s < data.jsonl                     # Filter from file
        cat mixed.txt | %(prog)s                  # Filter from pipe  
        echo '{"test": true}' | %(prog)s          # Filter from command
        
        Input: Reads from standard input (stdin) only
        Output: Valid JSON objects/arrays to stdout (or file with -o)
        
        Handles both single-line and multi-line JSON objects/arrays.
        Ignores JSON primitives (numbers, strings, booleans) unless they appear alone.
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--strict', 
        action='store_true',
        help='Use strict JSON parsing (default: lenient)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file for filtered JSONL records (default: stdout)'
    )
    
    args = parser.parse_args()
    
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as output_file:
                filter_json_lines(output_stream=output_file)
        except IOError as e:
            print(f"Error writing to file '{args.output}': {e}", file=sys.stderr)
            sys.exit(1)
    else:
        filter_json_lines()

if __name__ == '__main__':
    main()