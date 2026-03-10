#!/usr/bin/env python3
"""
Sample one episode from each podcast in mp3_list.csv
and export to sample_mp3_list.csv
"""

import csv
import random
import argparse
import sys
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser(
        description="Sample one episode from each podcast"
    )
    parser.add_argument(
        '--input',
        default='mp3_list.csv',
        help='Input CSV file (default: mp3_list.csv)'
    )
    parser.add_argument(
        '--output',
        default='sample_mp3_list.csv',
        help='Output CSV file (default: sample_mp3_list.csv)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for reproducible sampling (optional)'
    )
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
    
    # Read input CSV and group episodes by podcast
    podcasts = defaultdict(list)
    
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                podcast_title = row.get('podcast_title', '').strip()
                if podcast_title:
                    podcasts[podcast_title].append({
                        'podcast_title': podcast_title,
                        'episode_title': row.get('episode_title', '').strip(),
                        'audio_url': row.get('audio_url', '').strip()
                    })
    except FileNotFoundError:
        print(f"Error: Input file '{args.input}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading '{args.input}': {e}", file=sys.stderr)
        sys.exit(1)
    
    if not podcasts:
        print(f"No podcasts found in '{args.input}'", file=sys.stderr)
        sys.exit(1)
    
    # Sample one episode from each podcast
    sampled_episodes = []
    for podcast_title, episodes in podcasts.items():
        if episodes:
            sampled_episode = random.choice(episodes)
            sampled_episodes.append(sampled_episode)
    
    # Write to output CSV
    try:
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['podcast_title', 'episode_title', 'audio_url'])
            writer.writeheader()
            writer.writerows(sampled_episodes)
        
        print(f"Sampled {len(sampled_episodes)} episodes from {len(podcasts)} podcasts")
        print(f"Output saved to: {args.output}")
    except Exception as e:
        print(f"Error writing to '{args.output}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
