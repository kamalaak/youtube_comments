from youtube_comment_downloader import YoutubeCommentDownloader
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
import re

# Configuration
VIDEO_IDS = [
    "m67-bOpOoPU",
    "FYErehuSuuw",# Tech review
    "poo0BXryffI",
    "Ll62YIkEvs8",
    "z8Ll24GetSY",
    "nt_DbEv2kso",
    "Q8rEvxBZyxg",
    "OSvk5HLi-S4",
    "Fvy43W36Nx0",
    "fqrgFJcqTp0",
    "082Mqyo24xA",
    "tCqTU6d4qls?si",
    "PVcAhAxQcgM",
    "PVcAhAxQcgM",
    "qnozeCkyzzw",
    "fGVTTTRHWr0",
]
MAX_COMMENTS_PER_VIDEO = 2000
OUTPUT_FILE = "all_mixed_comments.csv"

def scrape_video_comments(video_id):
    """Scrape all comments from a single video"""
    downloader = YoutubeCommentDownloader()
    comments = []

    try:
        for i, comment in enumerate(downloader.get_comments(video_id)):
            if i >= MAX_COMMENTS_PER_VIDEO:
                break
            comments.append(comment['text'].strip())
        return (video_id, comments)
    except Exception as e:
        print(f"Failed on {video_id}: {str(e)}")
        return (video_id, [])

def is_mixed_content(text):
    """Detects both code-switching AND Tanglish"""
    # Tamil Unicode detection
    has_tamil = any(0x0B80 <= ord(c) <= 0x0BFF for c in text)

    # English detection (basic)
    has_english = any(c.isalpha() and ord(c) < 128 for c in text)

    # Tanglish patterns (English script with Tamil-like words)
    tanglish_patterns = [
        r"\bsuper\b.*\bsir\b",  # "super sir"
        r"\bthanks\b.*\banna\b", # "thanks anna"
        r"\bvideo\b.*\brocks\b", # "video rocks"
        r"\blatest\b.*\bvideo\b", # "latest video"
        r"\bnalla\b",            # "nalla" (good in Tamil)
        r"\bthala\b",            # "thala" (leader)
        r"\bvaruma\b",           # "varuma" (will come)
    ]

    is_tanglish = any(re.search(pattern, text, re.IGNORECASE) for pattern in tanglish_patterns)

    return (has_tamil and has_english) or is_tanglish

def main():
    start_time = time.time()

    # Phase 1: Parallel scraping
    print(f"Scraping {len(VIDEO_IDS)} videos...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(scrape_video_comments, VIDEO_IDS))

    # Phase 2: Enhanced filtering
    all_mixed_comments = []
    for video_id, comments in results:
        filtered = [c for c in comments if is_mixed_content(c)]
        print(f"{video_id}: {len(filtered)}/{len(comments)} mixed comments")
        all_mixed_comments.extend(filtered)

    # Save with comment type classification
    df = pd.DataFrame({
        "text": all_mixed_comments,
        "type": ["tamil_english" if any(0x0B80 <= ord(c) <= 0x0BFF for c in text)
                else "tanglish" for text in all_mixed_comments]
    })
    df.to_csv(OUTPUT_FILE, index=False)

    # Performance report
    elapsed = time.time() - start_time
    print(f"\nTotal: {len(all_mixed_comments)} mixed comments")
    print(f"Time: {elapsed:.1f}s ({len(all_mixed_comments)/elapsed:.1f} comments/sec)")

if __name__ == "__main__":
    main()
