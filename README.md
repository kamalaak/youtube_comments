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
