#!/usr/bin/env python3
"""
Extract podcast metadata from RSS feed files.
Processes RSS XML files, filters for English language feeds,
and extracts podcast title, episode title, and MP3/M4A URLs.
"""

import os
import sys
import csv
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse
from tqdm import tqdm


def is_english_feed(file_path):
    """Check if RSS feed is in English by looking for language tag."""
    try:
        # First try parsing XML to get language element (handles CDATA automatically)
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            channel = root.find('channel')
            if channel is None:
                channel = root.find('{http://purl.org/rss/1.0/}channel')
            
            if channel is not None:
                language_elem = channel.find('language')
                if language_elem is not None and language_elem.text:
                    lang = language_elem.text.strip().lower()
                    # Check if language starts with 'en' (handles 'en', 'en-us', 'en-gb', etc.)
                    return lang.startswith('en')
        except:
            pass
        
        # Fallback to string matching for malformed XML
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Look for various language patterns including CDATA
            patterns = [
                'language>en</language>',
                '<language>en</language>',
                '<language><![CDATA[en]]></language>',
                'language><![CDATA[en]]></language>',
                '<language>en-',
                'language>en-'
            ]
            return any(pattern in content for pattern in patterns)
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
        return False


def extract_audio_url(item):
    """Extract MP3 or M4A URL from RSS item."""
    # Try different common RSS formats
    audio_url = None
    
    # Check for enclosure (common in podcast RSS)
    enclosure = item.find('enclosure')
    if enclosure is not None:
        url = enclosure.get('url', '')
        if url and (url.endswith('.mp3') or url.endswith('.m4a') or 
                    'audio' in enclosure.get('type', '').lower()):
            audio_url = url
    
    # Check for media:content (iTunes/Media RSS)
    for ns in ['{http://search.yahoo.com/mrss/}', '']:
        media_content = item.find(f'{ns}content')
        if media_content is not None:
            url = media_content.get('url', '')
            if url and (url.endswith('.mp3') or url.endswith('.m4a')):
                audio_url = url
                break
    
    # Check for link that points to audio file
    link = item.find('link')
    if link is not None and link.text:
        url = link.text.strip()
        if url.endswith('.mp3') or url.endswith('.m4a'):
            audio_url = url
    
    # Check for itunes:enclosure
    for ns in ['{http://www.itunes.com/dtds/podcast-1.0.dtd}', '']:
        itunes_enclosure = item.find(f'{ns}enclosure')
        if itunes_enclosure is not None:
            url = itunes_enclosure.get('url', '')
            if url:
                audio_url = url
                break
    
    return audio_url


def parse_rss_file(file_path):
    """Parse RSS file and extract podcast metadata."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Get podcast title from channel
        channel = root.find('channel')
        if channel is None:
            # Try RSS 2.0 namespace
            channel = root.find('{http://purl.org/rss/1.0/}channel')
        
        if channel is None:
            return None
        
        podcast_title_elem = channel.find('title')
        podcast_title = podcast_title_elem.text.strip() if podcast_title_elem is not None and podcast_title_elem.text else "Unknown"
        
        # Extract episodes
        episodes = []
        items = channel.findall('item')
        
        for item in items:
            # Get episode title
            title_elem = item.find('title')
            episode_title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Unknown"
            
            # Get audio URL
            audio_url = extract_audio_url(item)
            
            if audio_url:
                episodes.append({
                    'podcast_title': podcast_title,
                    'episode_title': episode_title,
                    'audio_url': audio_url
                })
        
        return episodes
    
    except ET.ParseError as e:
        print(f"XML parse error in {file_path}: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Extract podcast metadata from RSS feed files"
    )
    parser.add_argument(
        '--rss-dir',
        default='/shared/0/datasets/podcasts/data/rss_feeds/0',
        help='Directory containing RSS XML files (default: /shared/0/datasets/podcasts/data/rss_feeds/0)'
    )
    parser.add_argument(
        '--output',
        default='mp3_list.csv',
        help='Output CSV file (default: mp3_list.csv)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=20,
        help='Limit number of RSS files to process (default: 20)'
    )
    parser.add_argument(
        '--skip-english-check',
        action='store_true',
        help='Skip English language check (process all feeds)'
    )
    
    args = parser.parse_args()
    
    rss_dir = Path(args.rss_dir)
    
    if not rss_dir.exists():
        print(f"Error: RSS directory '{rss_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)
    
    # Find all RSS XML files
    rss_files = sorted(list(rss_dir.glob('*.rss.xml')))[:args.limit]
    
    if not rss_files:
        print(f"No RSS XML files found in '{rss_dir}'", file=sys.stderr)
        sys.exit(1)
    
    print(f"Processing first {len(rss_files)} RSS files from: {rss_dir}")
    print(f"Output CSV: {args.output}\n")
    
    all_episodes = []
    processed = 0
    skipped_non_english = 0
    skipped_no_audio = 0
    
    skipped_files = []


    # Process each RSS file
    for rss_file in tqdm(rss_files, desc="Processing RSS feeds"):
        # Check if English (unless skip flag is set)
        if not args.skip_english_check:
            if not is_english_feed(rss_file):
                skipped_files.append(rss_file)
                skipped_non_english += 1
                continue
        
        # Parse RSS file
        episodes = parse_rss_file(rss_file)
        
        if episodes is None:
            continue
        
        if not episodes:
            skipped_no_audio += 1
            continue
        
        all_episodes.extend(episodes)
        processed += 1
    
    # Write to CSV
    if all_episodes:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['podcast_title', 'episode_title', 'audio_url'])
            writer.writeheader()
            writer.writerows(all_episodes)
        
        print(f"\n{'='*50}")
        print(f"Extraction complete!")
        print(f"  Processed feeds: {processed}")
        print(f"  Total episodes: {len(all_episodes)}")
        print(f"  Skipped (non-English): {skipped_non_english}")
        print(f"  Skipped (no audio): {skipped_no_audio}")
        print(f"  Output saved to: {args.output}")
        print(f"{'='*50}")
    else:
        print("\nNo episodes with audio URLs found.", file=sys.stderr)


if __name__ == "__main__":
    main()


