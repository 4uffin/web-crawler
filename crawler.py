import asyncio
import httpx
import json
import time 
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser 
import sys
import os

# --- Configuration ---
# This is now the SEED URL (Starting Point) for your broad web crawl.
TARGET_ROOT = "https://www.wikipedia.org/" # <--- IMPORTANT: Change this to your actual starting URL

# User Agent for ethical crawling - identify your crawler!
USER_AGENT = 'Mozilla/5.0 (compatible; MyCustomCrawler/1.0; +http://github.com/4uffin/web-crawler)' 

# Crawl Scope Limits
MAX_PAGES = 1000 # UPDATED: Limit the crawl to 1000 pages
MAX_DEPTH = 50   # Stop crawling links found deeper than this level 

# Output and Performance Settings
OUTPUT_FILE = "index.json"
SNIPPET_LENGTH = 200
MIN_DELAY_SECONDS = 2  # Rate limiting: minimum delay between requests to the same host

# Set of file extensions to ignore (images, zips, etc.)
IGNORED_EXTENSIONS = (
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar', '.mp4', '.mp3',
    '.txt', '.xml', '.atom', '.rss', '.css', '.js', '.ico'
)

# Global sets for state management
# Changed to_crawl_queue to store (url, depth) tuples for depth limiting
to_crawl_queue = {(TARGET_ROOT, 0)} 
crawled_urls = set()
index_data = []

# New state for ethical crawling
robots_cache = {} # Key: domain (netloc), Value: RobotFileParser instance
host_last_request_time = {} # Key: domain (netloc), Value: timestamp of last request

def is_valid(url):
    """
    Checks if a URL is valid for broad crawling (must be http/https and not an ignored file type).
    """
    parsed = urlparse(url)
    
    # 1. Must be a standard web scheme
    if parsed.scheme not in ('http', 'https'):
        return False

    # 2. Check if the path ends with an ignored extension
    if parsed.path.lower().endswith(IGNORED_EXTENSIONS):
        return False
        
    return True

def clean_and_summarize(soup):
    """
    Extracts title, prioritized main content, and a priority-based snippet from the page.
    """
    
    # 1. Title Extraction
    title_tag = soup.find('title')
    title = title_tag.text.strip() if title_tag else "Untitled Page"

    # 2. Prioritized Content Extraction (for Lunr search indexing)
    
    # Define tags that typically contain the main, valuable content
    main_content_tags = soup.find(['main', 'article', 'section'], role=['main', 'article'])
    
    if main_content_tags:
        # Use only the text from the primary content block if available
        searchable_block = main_content_tags
    else:
        # Fallback to the entire body if no main semantic tag is found
        searchable_block = soup.body

    if not searchable_block:
        content = ""
    else:
        # Remove script, style, navigation, and other noisy tags from the block
        for tag in searchable_block(['script', 'style', 'nav', 'header', 'footer', 'form']):
            tag.decompose()
            
        # Get all text and clean up whitespace
        content = searchable_block.get_text(separator=' ', strip=True) 

    # 3. SNIPPET PRIORITY LOGIC
    snippet = ""
    description_tag = soup.find('meta', attrs={'name': 'description'})
    
    if description_tag and 'content' in description_tag.attrs:
        # Priority 1: Use the meta description
        snippet = description_tag['content'].strip()
    else:
        # Priority 2: Fallback to the first SNIPPET_LENGTH characters of the body content
        snippet = content[:SNIPPET_LENGTH].strip()
        if len(content) > SNIPPET_LENGTH:
            snippet += "..."
            
    return title, content, snippet

async def fetch_robots_txt(client, host):
    """Asynchronously fetches and caches the robots.txt file for a given host."""
    if host in robots_cache:
        return robots_cache[host]

    robots_url = f"https://{host}/robots.txt"
    parser = RobotFileParser()
    parser.set_url(robots_url)
    
    try:
        # Attempt to fetch the robots.txt content
        print(f"  Fetching robots.txt for: {host}")
        response = await client.get(robots_url, timeout=5)
        
        if response.status_code == 200:
            # Parse the content line by line
            parser.parse(response.text.splitlines())
        else:
            # For 404/failure, the parser defaults to allowing all, which is acceptable.
            pass
            
    except Exception as e:
        print(f"  [ROBOTS ERROR] Failed to fetch robots.txt for {host}. Assuming permission. Error: {e}")
        
    robots_cache[host] = parser
    return parser


async def crawl(url, depth, client):
    """
    Fetches a single URL, processes it, and extracts new links.
    """
    global index_data

    # Check limits
    if len(crawled_urls) >= MAX_PAGES:
        print(f"Crawler hit MAX_PAGES limit ({MAX_PAGES}). Stopping crawl.")
        return

    # Skip if already processed
    if url in crawled_urls:
        return

    # Check depth limit
    if depth > MAX_DEPTH:
        print(f"  [SKIP] Max depth ({MAX_DEPTH}) exceeded for {url}. Current depth: {depth}")
        return
    
    parsed_url = urlparse(url)
    host = parsed_url.netloc

    # --- 1. ROBOTS.TXT CHECK ---
    robots_parser = await fetch_robots_txt(client, host)
    
    # Check if the USER_AGENT is allowed to crawl this URL
    if not robots_parser.can_fetch(USER_AGENT.split('/')[0], url): # Use only the crawler name (e.g., MyCustomCrawler)
        print(f"  [SKIP] Robots.txt disallowed crawl for {url}")
        crawled_urls.add(url) 
        return
    
    # --- 2. RATE LIMITING ---
    now = time.time()
    last_request = host_last_request_time.get(host, 0)
    elapsed = now - last_request
    
    if elapsed < MIN_DELAY_SECONDS:
        delay = MIN_DELAY_SECONDS - elapsed
        print(f"  [DELAY] Rate limit hit for {host}. Waiting {delay:.2f}s.")
        await asyncio.sleep(delay)

    # Update timestamp before making the request
    host_last_request_time[host] = time.time()
    
    # Now that checks are done, proceed with the crawl
    crawled_urls.add(url)
    print(f"Crawling: {url} (Depth: {depth}) ({len(crawled_urls)}/{MAX_PAGES})")

    try:
        # Fetch the page content
        response = await client.get(url, follow_redirects=True, timeout=10)
        response.raise_for_status()
        
        # Check Content-Type header to ensure it's HTML
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type:
             return

        # Parse and Process
        soup = BeautifulSoup(response.text, 'html.parser')
        title, content, snippet = clean_and_summarize(soup)
        
        # Add to the global index list
        index_data.append({
            "id": url,
            "title": title,
            "content": content,
            "url": url,
            "snippet": snippet 
        })

        # Find new links to crawl
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(url, href)
            
            # Normalize and validate the URL
            if is_valid(absolute_url):
                # Only add to queue if it hasn't been crawled yet AND respects MAX_DEPTH
                new_depth = depth + 1
                if absolute_url not in crawled_urls and new_depth <= MAX_DEPTH:
                    # Add new URL with its depth to the queue set
                    to_crawl_queue.add((absolute_url, new_depth))

    except httpx.HTTPError as e:
        print(f"  [ERROR] HTTP failure for {url}: {e}")
    except Exception as e:
        print(f"  [ERROR] General error processing {url}: {e}")


async def run_crawler():
    """Main asynchronous crawler loop."""
    # Pass the externalized USER_AGENT to the client headers
    headers = {'User-Agent': USER_AGENT}
    
    # Initialize the queue by unpacking the set into a list for iteration
    queue_list = list(to_crawl_queue)
    
    async with httpx.AsyncClient(headers=headers) as client:
        
        # Change loop logic to iterate over a list/queue that holds (url, depth) tuples
        current_queue = queue_list
        
        while current_queue and len(crawled_urls) < MAX_PAGES:
            # Simple BFS/FIFO Queue Logic: take the next 5 items from the front
            batch_items = current_queue[:5]
            current_queue = current_queue[5:]
            
            if not batch_items:
                break

            # Create and run a list of crawl tasks concurrently, passing url and depth
            tasks = [crawl(url, depth, client) for url, depth in batch_items]
            await asyncio.gather(*tasks)
            
            # Since `to_crawl_queue` is modified within `crawl`, we need to 
            # rebuild the iterable list from the set for the next loop iteration.
            
            # First, check for new links that were added by the concurrent tasks
            new_links = [item for item in to_crawl_queue if item not in set(current_queue)]
            
            # Update the queue list with new links for the next iteration
            current_queue.extend(new_links)
            
            # Clear the global set to prepare for the next round of additions (prevents duplicates)
            to_crawl_queue.clear() 
            # Repopulate the set with the current contents of the list (for deduplication check in crawl)
            to_crawl_queue.update(current_queue)


    # Finalize and write the index
    print("-" * 50)
    print(f"Crawl finished. Total pages indexed: {len(index_data)}")
    
    try:
        # Write the final JSON index file
        with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)
        print(f"Successfully wrote index to {OUTPUT_FILE}")

    except IOError as e:
        print(f"[CRITICAL ERROR] Could not write output file {OUTPUT_FILE}: {e}")
        sys.exit(1)


# Main entry point for the script
if __name__ == "__main__":
    
    # Validation checks
    if not TARGET_ROOT.startswith("http"):
         print(f"FATAL: TARGET_ROOT must be a full URL (e.g., https://yoursite.com). Current: {TARGET_ROOT}")
         sys.exit(1)
         
    # Run the main asynchronous function
    asyncio.run(run_crawler())
