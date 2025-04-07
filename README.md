# Tamil YouTube Comment Scraper

A robust pipeline for scraping and analyzing Tamil language comments from YouTube, including code-mixed (Tamil-English) and Tanglish content.

## Features

- **Multi-Language Support**: Handles Pure Tamil, Tamil-English Code-Mixed, and Tanglish comments
- **Large-Scale Processing**: Optimized for scraping 10,000+ videos
- **Intelligent Classification**: Auto-categorizes comments into 3 language types
- **Anti-Ban System**: Randomized delays and request throttling
- **Progress Tracking**: Real-time ETA calculations and resume capabilities
- **Efficient Storage**: Compressed Parquet + CSV outputs

## Installation

### Requirements
- Python 3.8+
- Chrome browser (for Selenium fallback)

```bash
# Clone repository
git clone https://github.com/yourusername/tamil-comment-scraper.git
cd tamil-comment-scraper

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt



```

# 🚀 How to Use

***Step 1: Extract Video IDs from a Channel***
- Open the sel.py file.
- Replace the channel_url with the YouTube channel link you want to scrape.
- Run the script:
``` 
    python sel.py
```
TThis will automatically create or update the all_video_ids.txt file with video IDs. 

✅ You can extract video IDs from multiple channels — just update the link in sel.py and run again.

🧠 Don’t worry about duplicates — the script only adds new IDs to the .txt file.

# Step 2: Scrape Comments from Videos

## Once video IDs are saved in all_video_ids.txt, run:

```
python scra.py

```

This will scrape the comments from all listed videos and categorize them by language type.


# 🧠 Workflow Overview

## 1. Initialization

- Loads video IDs
- Resumes from last checkpoint if available

# 2. Batch Processing

- Processes up to 100 videos in parallel
- Applies anti-ban delays
- Automatically retries failed requests