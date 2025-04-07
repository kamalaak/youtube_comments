from youtube_comment_downloader import YoutubeCommentDownloader
import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm import tqdm

# Configuration
VIDEO_IDS_FILE = "all_video_ids.txt"  # One ID per line
OUTPUT_DIR = "tamil_comments_data"
MAX_COMMENTS_PER_VIDEO = 500  # Reduced to avoid detection
MAX_WORKERS = 3  # Conservative thread count
REQUEST_DELAY = (1, 3)  # Random delay range in seconds

# Tanglish keywords (case-insensitive)
TANGLISH_KEYWORDS = {
    'super', 'thanks', 'anna', 'video', 'bro', 'sir', 'hi', 'hello',
    'nalla', 'thala', 'varuma', 'romba', 'semma', 'epdi', 'keep it up'
}

def load_video_ids():
    """Load video IDs from file"""
    with open(VIDEO_IDS_FILE) as f:
        return [line.strip() for line in f if line.strip() and len(line.strip()) == 11]

def is_pure_tamil(text):
    """Check if text contains only Tamil characters"""
    return all(0x0B80 <= ord(c) <= 0x0BFF or c.isspace() or c in ',.!?;:' for c in text)

def is_tanglish(text):
    """Check for English-written Tamil-like content"""
    text_lower = text.lower()
    return (any(keyword in text_lower for keyword in TANGLISH_KEYWORDS) and \
           not any(0x0B80 <= ord(c) <= 0x0BFF for c in text))

def classify_comment(text):
    """Categorize comment into language types"""
    if is_pure_tamil(text):
        return "pure_tamil"
    elif is_tanglish(text):
        return "tanglish"
    else:
        return "code_mixed"

def scrape_comments(video_id):
    """Safe comment scraping with random delays"""
    downloader = YoutubeCommentDownloader()
    comments = []
    
    try:
        for i, comment in enumerate(downloader.get_comments(video_id)):
            if i >= MAX_COMMENTS_PER_VIDEO:
                break
            comments.append(comment['text'].strip())
            
            # Random delay to avoid detection
            if i % random.randint(10, 20) == 0:
                time.sleep(random.uniform(*REQUEST_DELAY))
                
        return video_id, comments
    except Exception as e:
        print(f"⚠️ Error on {video_id}: {str(e)}")
        return video_id, []

def process_batch(batch_ids, batch_num):
    """Process a batch of videos"""
    batch_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_comments, vid): vid for vid in batch_ids}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {batch_num}"):
            video_id, comments = future.result()
            
            for comment in comments:
                if any(0x0B80 <= ord(c) <= 0x0BFF for c in comment):  # Contains Tamil
                    batch_results.append({
                        "video_id": video_id,
                        "text": comment,
                        "type": classify_comment(comment)
                    })
    
    # Save batch results
    if batch_results:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_num}.parquet")
        pd.DataFrame(batch_results).to_parquet(batch_file)
    
    return len(batch_results)

def combine_results():
    """Merge all batch files into final datasets"""
    all_data = []
    for file in os.listdir(OUTPUT_DIR):
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(OUTPUT_DIR, file))
            all_data.append(df)
    
    if all_data:
        final_df = pd.concat(all_data)
        
        # Save separate files for each type
        final_df[final_df['type'] == 'pure_tamil'].to_csv("pure_tamil_comments.csv", index=False)
        final_df[final_df['type'] == 'code_mixed'].to_csv("code_mixed_comments.csv", index=False)
        final_df[final_df['type'] == 'tanglish'].to_csv("tanglish_comments.csv", index=False)
        
        print(f"\n✅ Saved {len(final_df)} total comments:")
        print(f"- Pure Tamil: {len(final_df[final_df['type'] == 'pure_tamil'])}")
        print(f"- Code-Mixed: {len(final_df[final_df['type'] == 'code_mixed'])}")
        print(f"- Tanglish: {len(final_df[final_df['type'] == 'tanglish'])}")

def main():
    video_ids = load_video_ids()
    print(f"Loaded {len(video_ids)} valid video IDs")
    
    # Process in batches of 100 videos
    BATCH_SIZE = 100
    total_comments = 0
    
    for batch_num, i in enumerate(range(0, len(video_ids), BATCH_SIZE)):
        batch_ids = video_ids[i:i+BATCH_SIZE]
        batch_count = process_batch(batch_ids, batch_num + 1)
        total_comments += batch_count
        
        print(f"\nBatch {batch_num+1} complete: {batch_count} comments")
        print(f"Total collected: {total_comments}")
        
        # Extended cooldown every 5 batches
        if (batch_num + 1) % 5 == 0:
            cooldown = random.randint(30, 60)
            print(f"🛑 Cooling down for {cooldown} seconds...")
            time.sleep(cooldown)
    
    # Combine and categorize all results
    combine_results()

if __name__ == "__main__":
    main()