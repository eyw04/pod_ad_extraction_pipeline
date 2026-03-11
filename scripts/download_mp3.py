#!/usr/bin/env python3
"""
Script to download MP3/M4A audio files from URLs listed in a CSV file.
The CSV file should contain at least a URL column (or 'url', 'link', 'mp3_url', 'audio_url', etc.)
Optionally, it can have a filename column to specify custom output names.
"""

import csv
import os
import sys
import argparse
import time
from pathlib import Path
from urllib.parse import urlparse
import requests
from tqdm import tqdm


def find_url_column(headers):
    """Find the URL column in CSV headers."""
    url_keywords = ['url', 'link', 'mp3', 'download', 'source']
    for header in headers:
        header_lower = header.lower().strip()
        if any(keyword in header_lower for keyword in url_keywords):
            return header
    # If no obvious URL column, try first column
    return headers[0] if headers else None


def find_filename_column(headers):
    """Find the filename column in CSV headers."""
    filename_keywords = ['filename', 'name', 'file', 'output', 'title']
    for header in headers:
        header_lower = header.lower().strip()
        if any(keyword in header_lower for keyword in filename_keywords):
            return header
    return None


def get_filename_from_url(url, default_name="download"):
    """Extract filename from URL or generate a default name."""
    parsed = urlparse(url)
    path = parsed.path
    if path:
        filename = os.path.basename(path)
        if filename and (filename.endswith('.mp3') or filename.endswith('.m4a')):
            return filename
    # Generate filename from URL hash or use default
    return f"{default_name}.mp3"


def download_file(url, output_path, chunk_size=8192):
    """Download a file from URL to output_path."""
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=os.path.basename(output_path), leave=False) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
        
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}", file=sys.stderr)
        return False


def redownload_periodically(rows, url_column, filename_column, output_dir, interval=30, duration=600):
    """
    Re-download audio files periodically every 'interval' seconds for 'duration' seconds.
    Saves files to 'output_dir' and replaces existing files with the same name.
    
    Args:
        rows: List of CSV row dictionaries
        url_column: Name of the URL column
        filename_column: Name of the filename column (or None)
        output_dir: Path to output directory
        interval: Seconds between downloads (default: 30)
        duration: Total duration in seconds (default: 600 = 10 minutes)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*50}")
    print(f"Starting periodic re-downloads")
    print(f"  Interval: {interval} seconds")
    print(f"  Duration: {duration} seconds ({duration // 60} minutes)")
    print(f"  Output directory: {output_dir}")
    print(f"  Total downloads: {duration // interval}")
    print(f"{'='*50}\n")
    
    # Wait 30 seconds before starting
    print(f"Waiting {interval} seconds before first re-download...")
    time.sleep(interval)
    
    start_time = time.time()
    download_count = 0
    
    while (time.time() - start_time) < duration:
        download_count += 1
        elapsed = time.time() - start_time
        remaining = duration - elapsed
        
        print(f"\n[Re-download #{download_count}] Time elapsed: {elapsed:.1f}s, Remaining: {remaining:.1f}s")
        
        downloaded = 0
        failed = 0
        
        for idx, row in enumerate(rows, 1):
            url = row.get(url_column, '').strip()
            
            if not url:
                continue
            
            # Determine output filename (same logic as main download)
            if filename_column and row.get(filename_column):
                filename = row[filename_column].strip()
                if not filename.endswith('.mp3') and not filename.endswith('.m4a'):
                    if url.lower().endswith('.m4a'):
                        filename += '.m4a'
                    else:
                        filename += '.mp3'
            else:
                filename = get_filename_from_url(url, f"file_{idx}")
            
            output_path = output_dir / filename
            
            # Download the file (will overwrite if it already exists)
            if download_file(url, output_path):
                downloaded += 1
            else:
                failed += 1
                # Remove partial file if download failed
                if output_path.exists():
                    output_path.unlink()
        
        print(f"  Downloaded: {downloaded}, Failed: {failed}")
        
        # Check if we should continue
        elapsed = time.time() - start_time
        if elapsed >= duration:
            break
        
        # Wait for next interval (unless we've reached the duration)
        time_until_next = min(interval, duration - elapsed)
        if time_until_next > 0:
            print(f"  Waiting {time_until_next:.1f} seconds until next download...")
            time.sleep(time_until_next)
    
    print(f"\n{'='*50}")
    print(f"Periodic re-downloads complete!")
    print(f"  Total re-download cycles: {download_count}")
    print(f"{'='*50}")


def main():
    parser = argparse.ArgumentParser(
        description="Download MP3/M4A audio files from URLs in a CSV file"
    )
    parser.add_argument(
        '--csv',
        default='10_sample.csv',
        help='Path to CSV file containing audio URLs (default: 10_sample.csv)'
    )
    parser.add_argument(
        '--output-dir',
        default='redownloaded_audio',
        help='Directory to save downloaded audio files (default: redownloaded_audio)'
    )
    parser.add_argument(
        '--url-column',
        default=None,
        help='Name of the URL column in CSV (auto-detected if not specified)'
    )
    parser.add_argument(
        '--filename-column',
        default=None,
        help='Name of the filename column in CSV (optional)'
    )
    parser.add_argument(
        '--redownload',
        action='store_true',
        help='Re-download files every 30 seconds for 10 minutes to redownloaded_audio folder'
    )
    
    args = parser.parse_args()
    
    # Check if CSV file exists
    if not os.path.exists(args.csv):
        print(f"Error: CSV file '{args.csv}' not found.", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read CSV and download files
    downloaded = 0
    failed = 0
    skipped = 0
    
    try:
        with open(args.csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            if not headers:
                print("Error: CSV file appears to be empty or has no headers.", file=sys.stderr)
                sys.exit(1)
            
            # Find URL and filename columns
            url_column = args.url_column or find_url_column(headers)
            filename_column = args.filename_column or find_filename_column(headers)
            
            if not url_column:
                print("Error: Could not find URL column in CSV.", file=sys.stderr)
                sys.exit(1)
            
            print(f"Using URL column: '{url_column}'")
            if filename_column:
                print(f"Using filename column: '{filename_column}'")
            print(f"Output directory: {output_dir}\n")
            
            rows = list(reader)
            for idx, row in enumerate(tqdm(rows, desc="Downloading audio files"), 1):
                url = row.get(url_column, '').strip()
                
                if not url:
                    print(f"Row {idx}: Empty URL, skipping", file=sys.stderr)
                    skipped += 1
                    continue
                
                # Determine output filename
                if filename_column and row.get(filename_column):
                    filename = row[filename_column].strip()
                    if not filename.endswith('.mp3') and not filename.endswith('.m4a'):
                        # Try to detect extension from URL
                        if url.lower().endswith('.m4a'):
                            filename += '.m4a'
                        else:
                            filename += '.mp3'
                else:
                    filename = get_filename_from_url(url, f"file_{idx}")
                
                output_path = output_dir / filename
                
                # Download the file (will overwrite if it already exists)
                if output_path.exists():
                    print(f"Row {idx}: File '{filename}' already exists, re-downloading...", file=sys.stderr)
                
                # Download the file
                if download_file(url, output_path):
                    downloaded += 1
                else:
                    failed += 1
                    # Remove partial file if download failed
                    if output_path.exists():
                        output_path.unlink()
    
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Download complete!")
    print(f"  Downloaded: {downloaded}")
    print(f"  Failed: {failed}")
    print(f"  Skipped: {skipped}")
    print(f"  Total: {len(rows) if 'rows' in locals() else 0}")
    print(f"{'='*50}")
    
    # Start periodic re-downloads if requested
    if args.redownload:
        redownload_periodically(
            rows,
            url_column,
            filename_column,
            'redownloaded_audio',
            interval=30,
            duration=600  # 10 minutes
        )


if __name__ == "__main__":
    main()


# python download_mp3.py --redownload