from youtube_comment_downloader import YoutubeCommentDownloader
import pandas as pd
import time
import random
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import numpy as np

# Configuration
VIDEO_IDS_FILE = "all_video_ids.txt"
OUTPUT_DIR = "comment_data"
MAX_COMMENTS_PER_VIDEO = 500
MAX_WORKERS = 3
REQUEST_DELAY = (1, 3)  # seconds
MAX_RUNTIME = timedelta(hours=20)
SAVE_INTERVAL = timedelta(minutes=30)

# Tanglish keywords
TANGLISH_KEYWORDS = {
    'super', 'thanks', 'anna', 'video', 'bro', 'sir', 'hi', 'hello',
    'nalla', 'thala', 'varuma', 'romba', 'semma', 'epdi', 'keep it up'
}

def load_video_ids():
    """Load and validate video IDs"""
    with open(VIDEO_IDS_FILE) as f:
        return [line.strip() for line in f if len(line.strip()) == 11]

def is_pure_tamil(text):
    """Check for Tamil-only text"""
    return all(0x0B80 <= ord(c) <= 0x0BFF or c.isspace() or c in ',.!?;:' for c in text)

def is_tanglish(text):
    """Detect English-written Tamil-like content"""
    text_lower = text.lower()
    return (any(kw in text_lower for kw in TANGLISH_KEYWORDS) and \
           not any(0x0B80 <= ord(c) <= 0x0BFF for c in text))

def classify_comment(text):
    """Categorize comment type"""
    if is_pure_tamil(text):
        return "pure_tamil"
    elif is_tanglish(text):
        return "tanglish"
    return "code_mixed"

def scrape_comments(video_id):
    """Fetch comments with random delays"""
    downloader = YoutubeCommentDownloader()
    comments = []
    
    try:
        for i, comment in enumerate(downloader.get_comments(video_id)):
            if i >= MAX_COMMENTS_PER_VIDEO:
                break
            comments.append(comment['text'].strip())
            
            if i % random.randint(10, 20) == 0:
                time.sleep(random.uniform(*REQUEST_DELAY))
                
        return video_id, comments
    except Exception as e:
        print(f"⚠️ Failed {video_id}: {str(e)}")
        return video_id, []

def process_batch(batch_ids, batch_num):
    """Process a batch of videos"""
    batch_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_comments, vid): vid for vid in batch_ids}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {batch_num}"):
            video_id, comments = future.result()
            
            for comment in comments:
                if any(0x0B80 <= ord(c) <= 0x0BFF for c in comment):
                    batch_results.append({
                        "video_id": video_id,
                        "text": comment,
                        "type": classify_comment(comment)
                    })
    
    if batch_results:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_num}.parquet")
        pd.DataFrame(batch_results).to_parquet(batch_file, compression='gzip')
    
    return len(batch_results)

def combine_results():
    """Merge all batch files"""
    all_data = []
    for file in os.listdir(OUTPUT_DIR):
        if file.endswith(".parquet"):
            all_data.append(pd.read_parquet(os.path.join(OUTPUT_DIR, file)))
    
    if all_data:
        final_df = pd.concat(all_data)
        
        # Save categorized datasets
        for type_name in ["pure_tamil", "code_mixed", "tanglish"]:
            subset = final_df[final_df['type'] == type_name]
            if not subset.empty:
                subset.to_csv(f"{type_name}_comments.csv", index=False)
        
        print(f"\n✅ Final counts:")
        print(final_df['type'].value_counts())

def main():
    start_time = datetime.now()
    video_ids = load_video_ids()
    print(f"Loaded {len(video_ids)} video IDs | Target runtime: {MAX_RUNTIME}")
    
    processed_ids = set()
    if os.path.exists("processed.log"):
        with open("processed.log") as f:
            processed_ids = {line.strip() for line in f}
    
    remaining_ids = [vid for vid in video_ids if vid not in processed_ids]
    total_batches = len(remaining_ids) // 100 + 1
    
    for batch_num, i in enumerate(range(0, len(remaining_ids), 100)):
        if datetime.now() - start_time > MAX_RUNTIME:
            print("⏰ Max runtime reached")
            break
            
        batch_ids = remaining_ids[i:i+100]
        batch_count = process_batch(batch_ids, batch_num + 1)
        
        # Log processed IDs
        with open("processed.log", "a") as f:
            f.write("\n".join(batch_ids) + "\n")
        
        # Progress report
        elapsed = datetime.now() - start_time
        print(f"\nBatch {batch_num+1}/{total_batches} | {batch_count} comments")
        print(f"Elapsed: {elapsed} | Est. remaining: {elapsed*(total_batches-batch_num-1)/(batch_num+1)}")
        
        # Periodic cooldown
        if (batch_num + 1) % 5 == 0:
            cooldown = random.randint(30, 60)
            print(f"🛑 Cooling down for {cooldown}s...")
            time.sleep(cooldown)
    
    combine_results()
    print(f"\nTotal runtime: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()
