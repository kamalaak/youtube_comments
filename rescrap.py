from youtube_comment_downloader import YoutubeCommentDownloader
import pandas as pd
import time
import random
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json

# Configuration
VIDEO_IDS_FILE = "all_video_ids.txt"
OUTPUT_DIR = "comment_data"
MAX_COMMENTS_PER_VIDEO = 500
MAX_WORKERS = 3
REQUEST_DELAY = (1, 3)  # Random delay between requests (min, max) in seconds
MAX_RUNTIME = timedelta(hours=20)
SAVE_INTERVAL = timedelta(minutes=30)

# Tanglish keywords
TANGLISH_KEYWORDS = {
    'super', 'thanks', 'anna', 'video', 'bro', 'sir', 'hi', 'hello',
    'nalla', 'thala', 'varuma', 'romba', 'semma', 'epdi', 'keep it up'
}

def safe_to_parquet(df, path):
    """Handle Parquet export with fallback to CSV"""
    try:
        df.to_parquet(path, engine='pyarrow', compression='gzip')
    except Exception as e:
        print(f"⚠️ Parquet failed ({str(e)}), falling back to CSV")
        df.to_csv(path.replace('.parquet', '.csv'), index=False)

def load_video_ids():
    """Load video IDs from file"""
    with open(VIDEO_IDS_FILE) as f:
        return [line.strip() for line in f if len(line.strip()) == 11]

def load_progress():
    """Load progress from JSON file if it exists"""
    if os.path.exists("progress.json"):
        try:
            with open("progress.json", "r") as f:
                return json.load(f)
        except:
            return {"processed_ids": [], "current_batch": 0}
    return {"processed_ids": [], "current_batch": 0}

def save_progress(processed_ids, current_batch):
    """Save progress to JSON file"""
    progress = {
        "processed_ids": list(processed_ids),
        "current_batch": current_batch,
        "last_saved": datetime.now().isoformat()
    }
    with open("progress.json", "w") as f:
        json.dump(progress, f)

def is_pure_tamil(text):
    """Check if text contains only Tamil characters"""
    return all(0x0B80 <= ord(c) <= 0x0BFF or c.isspace() or c in ',.!?;:' for c in text)

def is_tanglish(text):
    """Check if text matches Tanglish patterns"""
    text_lower = text.lower()
    return (any(kw in text_lower for kw in TANGLISH_KEYWORDS) and 
           not any(0x0B80 <= ord(c) <= 0x0BFF for c in text))

def classify_comment(text):
    """Classify comment into Pure Tamil/Tanglish/Code-mixed"""
    if not text.strip():
        return "empty"
    elif is_pure_tamil(text):
        return "pure_tamil"
    elif is_tanglish(text):
        return "tanglish"
    return "code_mixed"

def scrape_comments(video_id):
    """Scrape comments for a single video"""
    downloader = YoutubeCommentDownloader()
    comments = []
    
    try:
        for i, comment in enumerate(downloader.get_comments(video_id)):
            if i >= MAX_COMMENTS_PER_VIDEO:
                break
                
            comments.append({
                "video_id": video_id,
                "text": comment['text'].strip(),
                "type": classify_comment(comment['text'].strip()),
                "timestamp": datetime.now().isoformat()
            })
            
            if i % random.randint(5, 15) == 0:
                time.sleep(random.uniform(*REQUEST_DELAY))
                
        return video_id, comments
    except Exception as e:
        print(f"⚠️ Failed {video_id}: {str(e)}")
        return video_id, []

def process_batch(batch_ids, batch_num):
    """Process a batch of video IDs"""
    batch_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_comments, vid): vid for vid in batch_ids}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {batch_num}"):
            video_id, comments = future.result()
            batch_results.extend(comments)
    
    if batch_results:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        batch_file = os.path.join(OUTPUT_DIR, f"batch_{batch_num}_{datetime.now().strftime('%Y%m%d')}.parquet")
        safe_to_parquet(pd.DataFrame(batch_results), batch_file)
    
    return len(batch_results)

def combine_results():
    """Combine all batch files into final datasets"""
    all_data = []
    for file in os.listdir(OUTPUT_DIR):
        if not (file.endswith(".parquet") or file.endswith(".csv")):
            continue
            
        try:
            file_path = os.path.join(OUTPUT_DIR, file)
            df = pd.read_parquet(file_path) if file.endswith(".parquet") else pd.read_csv(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"⚠️ Error reading {file}: {str(e)}")
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save by comment type
        for type_name in ["pure_tamil", "code_mixed", "tanglish"]:
            subset = final_df[final_df['type'] == type_name]
            if not subset.empty:
                safe_to_parquet(subset, f"{type_name}_comments.parquet")
        
        # Save full dataset
        safe_to_parquet(final_df, "all_comments.parquet")
        print("\n✅ Final counts:")
        print(final_df['type'].value_counts())

def main():
    """Main scraping workflow with resume support"""
    start_time = datetime.now()
    video_ids = load_video_ids()
    progress = load_progress()
    
    # Filter already processed IDs
    processed_ids = set(progress.get("processed_ids", []))
    remaining_ids = [vid for vid in video_ids if vid not in processed_ids]
    
    print(f"⏳ Resuming from {len(processed_ids)} processed videos | {len(remaining_ids)} remaining")
    total_batches = (len(remaining_ids) // 100) + 1
    current_batch = progress.get("current_batch", 0)

    for batch_num in range(current_batch, total_batches):
        batch_start = batch_num * 100
        batch_ids = remaining_ids[batch_start : batch_start + 100]

        print(f"\n📦 Processing batch {batch_num + 1}/{total_batches} ({(batch_num + 1)/total_batches:.1%})")
        batch_count = process_batch(batch_ids, batch_num + 1)
        
        # Update progress
        processed_ids.update(batch_ids)
        save_progress(processed_ids, batch_num + 1)
        
        print(f"✔️ Batch {batch_num + 1} complete | {batch_count} comments")
        
        # Runtime check
        elapsed = datetime.now() - start_time
        if elapsed > MAX_RUNTIME:
            print(f"⏰ Max runtime reached ({elapsed})")
            break

    # Final cleanup
    if len(processed_ids) == len(video_ids):
        os.remove("progress.json")  # Only delete if fully completed
        print("♻️ Progress file cleaned up (fully completed)")
    
    combine_results()
    print(f"\n🏁 Total runtime: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()