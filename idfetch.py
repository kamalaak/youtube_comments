from pytube import Channel
import re
import os
from tqdm import tqdm

def fetch_video_ids(channel_url, output_file="ids.txt"):
    try:
        # Initialize channel
        channel = Channel(channel_url)
        print(f"\nFetching videos from: {channel.channel_name}")

        # Get raw HTML content
        html = channel.html

        # Extract all video IDs using regex
        video_ids = list(set(re.findall(r"watch\?v=([a-zA-Z0-9_-]{11})", html)))

        if not video_ids:
            print("Warning: No video IDs found in page source")
            print("Trying alternative extraction method...")
            # Fallback to pytube's video_urls
            video_ids = [url.split('v=')[-1].split('&')[0] for url in channel.video_urls]
            video_ids = list(set(video_ids))

        # Filter valid IDs
        video_ids = [vid for vid in video_ids if len(vid) == 11]

        if not video_ids:
            print("Error: Still no video IDs found")
            return []

        # Handle existing file and duplicates
        existing_ids = set()
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                existing_ids = {line.strip() for line in f if line.strip()}

        # Find new IDs
        new_ids = [vid for vid in video_ids if vid not in existing_ids]

        # Append new IDs
        if new_ids:
            with open(output_file, 'a') as f:
                for vid in tqdm(new_ids, desc="Saving new IDs"):
                    f.write(f"{vid}\n")
            print(f"Success: Added {len(new_ids)} new IDs (Total: {len(video_ids)})")
        else:
            print(f"No new IDs found (Channel has {len(video_ids)} videos)")

        return video_ids

    except Exception as e:
        print(f"\nError: {str(e)}")
        print("Possible solutions:")
        print("1. Verify the channel URL is correct")
        print("2. Try again later (YouTube might be blocking)")
        print("3. Use VPN if you're being rate-limited")
        return []

# Example usage
channel_url = "https://www.youtube.com/channel/UCk3JZr7eS3pg5AGEvBdEvFg"
video_ids = fetch_video_ids(channel_url)

if video_ids:
    print("\nSample video IDs:")
    print("\n".join(video_ids[:5]))
