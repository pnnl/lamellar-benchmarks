#!/usr/bin/env python3
"""
Runner script that enhances jsonl_filter.py output with essential metadata.
Adds git version, run date, system stats, and package versions to each JSON line.
Outputs in JSON Lines format (one JSON object per line).
REFUSES TO RUN if there are uncommitted changes in the git repository.
"""
import json
import sys
import subprocess
import datetime
import platform
import argparse
import os
import re

def check_git_status():
    """Check git status and refuse to run if there are uncommitted changes."""
    try:
        subprocess.check_output(
            ['git', 'rev-parse', '--git-dir'],
            stderr=subprocess.DEVNULL
        )
        
        status_output = subprocess.check_output(
            ['git', 'status', '--porcelain'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        if status_output:
            uncommitted_lines = [line for line in status_output.split('\n') 
                               if line and not line.startswith('??')]
            
            if uncommitted_lines:
                print("ERROR: Repository has uncommitted changes:", file=sys.stderr)
                for line in uncommitted_lines:
                    print(f"  {line}", file=sys.stderr)
                print("\nPlease commit your changes before running benchmarks.", file=sys.stderr)
                sys.exit(1)
                
        return True
        
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("WARNING: Not in a git repository or git not available", file=sys.stderr)
        return False

def get_git_info():
    """Get essential git repository information."""
    try:
        commit_hash = subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        commit_message = subprocess.check_output(
            ['git', 'show', '-s', '--format=%s', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        commit_date = subprocess.check_output(
            ['git', 'show', '-s', '--format=%ci', 'HEAD'],
            stderr=subprocess.DEVNULL
        ).decode('utf-8').strip()
        
        return {
            'commit_hash': commit_hash,
            'commit_hash_short': commit_hash[:8] if commit_hash else None,
            'commit_message': commit_message,
            'commit_date': commit_date
        }
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {
            'commit_hash': None,
            'commit_hash_short': None,
            'commit_message': None,
            'commit_date': None,
            'error': 'Git not available or not in a git repository'
        }

def get_processor_info():
    """Get processor information."""
    processor_info = {
        'processor_version': None,
        'physical_processors': None,
        'logical_processors': None,
        'cores_per_processor': None
    }
    
    try:
        import psutil
        processor_info['logical_processors'] = psutil.cpu_count(logical=True)
        processor_info['physical_processors'] = psutil.cpu_count(logical=False)
    except ImportError:
        pass
    
    if platform.system() == 'Linux':
        try:
            with open('/proc/cpuinfo', 'r') as f:
                cpuinfo = f.read()
            
            model_match = re.search(r'model name\s*:\s*(.+)', cpuinfo)
            if model_match:
                processor_info['processor_version'] = model_match.group(1).strip()
            
            physical_ids = set()
            for line in cpuinfo.split('\n'):
                if line.startswith('physical id'):
                    physical_ids.add(line.split(':')[1].strip())
            if physical_ids:
                processor_info['physical_processors'] = len(physical_ids)
                
        except (IOError, OSError):
            pass
    
    if (processor_info['logical_processors'] and 
        processor_info['physical_processors'] and 
        processor_info['physical_processors'] > 0):
        processor_info['cores_per_processor'] = (
            processor_info['logical_processors'] // processor_info['physical_processors']
        )
    
    return processor_info

def get_package_versions():
    """Get version of Lamellar"""
    versions = {}
    
    try:
        if os.path.exists('Cargo.toml'):
            cargo_output = subprocess.check_output(
                ['cargo', 'tree', '--depth=1'],
                stderr=subprocess.DEVNULL
            ).decode('utf-8')
            
            for line in cargo_output.split('\n'):
                if line.startswith('├──') or line.startswith('└──'):
                    dependency = line[4:].strip()
                    
                    if dependency.startswith('lamellar '):
                        versions['lamellar_version'] = dependency
                    
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    env_vars = ['LAMELLAR_VERSION']
    for var in env_vars:
        value = os.environ.get(var)
        if value:
            versions[var.lower()] = value
    
    return versions

def get_system_stats():
    """Get essential system statistics."""
    stats = {
        'os': platform.system(),
        'os_version': platform.release()
    }
    
    stats.update(get_processor_info())
    
    try:
        import psutil
        memory = psutil.virtual_memory()
        stats['memory_total_gb'] = round(memory.total / (1024**3), 2)
        
    except ImportError:
        stats['psutil_available'] = False
        stats['note'] = 'Install psutil for memory stats'
    except Exception as e:
        stats['system_stats_error'] = f'Failed to get system stats: {str(e)}'
    
    return stats

def get_metadata(benchmark_type=None):
    """Collect all metadata."""
    metadata = {
        'run_date': datetime.datetime.now().isoformat(),
        'run_timestamp': datetime.datetime.now().timestamp(),
        'git_info': get_git_info(),
        'system_stats': get_system_stats(),
        'package_versions': get_package_versions()
    }
    
    if benchmark_type:
        metadata['benchmark_type'] = benchmark_type
    
    return metadata

def write_jsonl_line(data, output_stream):
    """Write a single JSON object as a JSONL line."""
    try:
        json_line = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        output_stream.write(json_line)
        output_stream.write('\n')
        output_stream.flush()
    except (TypeError, ValueError) as e:
        print(f"Error serializing JSON: {e}", file=sys.stderr)
        print(f"Data: {data}", file=sys.stderr)

def run_jsonl_filter_and_enhance(jsonl_filter_args, metadata, output_stream):
    """Run jsonl_filter.py and enhance its output with metadata."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jsonl_filter_path = os.path.join(script_dir, 'jsonl_filter.py')
    
    cmd = [sys.executable, jsonl_filter_path] + jsonl_filter_args
    
    try:
        process = subprocess.Popen(
            cmd,
            stdin=sys.stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1
        )
        
        for line_bytes in process.stdout:
            line = line_bytes.decode('utf-8').strip()
            if not line:
                continue
                
            try:
                original_data = json.loads(line)
                
                if isinstance(original_data, dict):
                    enhanced_data = original_data.copy()
                    enhanced_data['_metadata'] = metadata
                elif isinstance(original_data, list):
                    enhanced_data = {
                        'data': original_data,
                        'data_type': 'array',
                        '_metadata': metadata
                    }
                else:
                    enhanced_data = {
                        'data': original_data,
                        'data_type': type(original_data).__name__,
                        '_metadata': metadata
                    }
                
                write_jsonl_line(enhanced_data, output_stream)
                
            except json.JSONDecodeError as e:
                print(f"Warning: Invalid JSON from jsonl_filter.py: {line}", file=sys.stderr)
                print(f"JSON Error: {e}", file=sys.stderr)
                error_record = {
                    'error': 'Invalid JSON from jsonl_filter.py',
                    'original_line': line,
                    'json_error': str(e),
                    '_metadata': metadata
                }
                write_jsonl_line(error_record, output_stream)
        
        _, stderr_output_bytes = process.communicate()
        
        stderr_output = stderr_output_bytes.decode('utf-8') if stderr_output_bytes else ""
        
        if process.returncode != 0:
            if stderr_output:
                print(f"jsonl_filter.py stderr: {stderr_output}", file=sys.stderr)
            sys.exit(process.returncode)
        elif stderr_output:
            print(f"jsonl_filter.py stderr: {stderr_output}", file=sys.stderr)
            
    except FileNotFoundError:
        print(f"Error: jsonl_filter.py not found at {jsonl_filter_path}", file=sys.stderr)
        print("Make sure jsonl_filter.py is in the same directory as append_metadata.py", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        if 'process' in locals():
            process.terminate()
        sys.exit(0)
    except Exception as e:
        print(f"Error running jsonl_filter.py: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='Run jsonl_filter.py and enhance output with essential metadata in JSONL format',
        epilog='''
        This script runs jsonl_filter.py and adds essential metadata including:
        - Git info (commit hash, date, message)
        - System stats (memory total, processors, OS)
        - Package versions (Lamellar)
        This script REFUSES to run if there are uncommitted changes in the repository.
        Output is in JSON Lines format (one JSON object per line).
        Metadata is added as a '_metadata' field to each JSON object.
        Requirements:
        - jsonl_filter.py must be in the same directory
        - Clean git repository (no uncommitted changes)
        - Optional: psutil package for memory stats (pip install psutil)
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--no-metadata',
        action='store_true',
        help='Skip adding metadata (just run jsonl_filter.py normally)'
    )
    
    parser.add_argument(
        '--skip-git-check',
        action='store_true',
        help='Skip git status check (allows running with uncommitted changes)'
    )
    
    parser.add_argument(
        '--strict',
        action='store_true',
        help='Use strict JSON parsing (passed to jsonl_filter.py)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file for enhanced JSONL records (default: stdout)'
    )
    
    parser.add_argument(
        '--benchmark-type',
        type=str,
        help='Specify the benchmark type (e.g., histo_buffered_safe_am)'
    )
    
    args = parser.parse_args()
    
    if not args.no_metadata and not args.skip_git_check:
        print("Checking git repository status...", file=sys.stderr)
        check_git_status()
    
    jsonl_filter_args = []
    if args.strict:
        jsonl_filter_args.append('--strict')
    
    if args.no_metadata:
        if args.output:
            jsonl_filter_args.extend(['-o', args.output])
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            jsonl_filter_path = os.path.join(script_dir, 'jsonl_filter.py')
            cmd = [sys.executable, jsonl_filter_path] + jsonl_filter_args
            subprocess.run(cmd, stdin=sys.stdin)
        except FileNotFoundError:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            jsonl_filter_path = os.path.join(script_dir, 'jsonl_filter.py')
            print(f"Error: jsonl_filter.py not found at {jsonl_filter_path}", file=sys.stderr)
            sys.exit(1)
        return
    
    print("Collecting essential metadata...", file=sys.stderr)
    metadata = get_metadata()
    
    # Add benchmark type to metadata if provided
    if args.benchmark_type:
        metadata['benchmark_type'] = args.benchmark_type
    
    commit_info = metadata['git_info']['commit_hash_short'] or 'unknown'
    print(f"Processing with metadata from commit {commit_info}", file=sys.stderr)
    
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as output_file:
                run_jsonl_filter_and_enhance(jsonl_filter_args, metadata, output_file)
            print(f"Enhanced JSONL output written to: {args.output}", file=sys.stderr)
        except IOError as e:
            print(f"Error writing to file '{args.output}': {e}", file=sys.stderr)
            sys.exit(1)
    else:
        run_jsonl_filter_and_enhance(jsonl_filter_args, metadata, sys.stdout)

if __name__ == '__main__':
    main()