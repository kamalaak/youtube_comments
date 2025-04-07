from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import re
import os

def get_all_video_ids(channel_url, output_file="all_video_ids.txt"):
    # Configure Chrome to run headless
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(channel_url)

    # Wait for page to load
    time.sleep(3)

    # Scroll to bottom multiple times to load all videos
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    scroll_attempts = 0

    while True:
        # Scroll to bottom
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(2)  # Wait to load

        # Calculate new scroll height
        new_height = driver.execute_script("return document.documentElement.scrollHeight")

        # Check if we've reached the end
        if new_height == last_height:
            scroll_attempts += 1
            if scroll_attempts > 3:  # Try 3 times before giving up
                break
        else:
            scroll_attempts = 0

        last_height = new_height

    # Extract all video links
    video_links = driver.find_elements(By.CSS_SELECTOR, "a#video-title-link")
    video_ids = []

    for link in video_links:
        href = link.get_attribute("href")
        if "v=" in href:
            video_id = href.split("v=")[1].split("&")[0]
            if len(video_id) == 11:  # Validate ID length
                video_ids.append(video_id)

    driver.quit()

    # Save to file (appending new ones)
    existing_ids = set()
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            existing_ids = set(line.strip() for line in f)

    new_ids = set(video_ids) - existing_ids

    if new_ids:
        with open(output_file, 'a') as f:
            for vid in new_ids:
                f.write(f"{vid}\n")

    print(f"Found {len(video_ids)} videos ({len(new_ids)} new)")
    return video_ids

# Example usage
channel_url = "https://www.youtube.com/@Behindwoodstv/videos"
all_video_ids = get_all_video_ids(channel_url)
print(f"Sample IDs: {all_video_ids[:5]}")
