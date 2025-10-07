import asyncio
import httpx
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sys
import os

# --- Configuration ---
# IMPORTANT: CHANGE THIS TO YOUR ACTUAL SITE'S ROOT URL (e.g., 'https://connor.dev')
TARGET_ROOT = "https://example.com"
MAX_PAGES = 50  # Limit the crawl for safety and speed
OUTPUT_FILE = "index.json"
SNIPPET_LENGTH = 200

# Set of file extensions to ignore (images, zips, etc.)
IGNORED_EXTENSIONS = (
    '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar', '.mp4', '.mp3',
    '.txt', '.xml', '.atom', '.rss', '.css', '.js', '.ico'
)

# Global sets for state management
to_crawl_queue = {TARGET_ROOT}
crawled_urls = set()
index_data = []

def is_valid(url):
    """Checks if a URL belongs to the target domain and is not an ignored file type."""
    parsed = urlparse(url)
    
    # Check if the domain matches (or is a relative path)
    if not parsed.netloc or parsed.netloc in urlparse(TARGET_ROOT).netloc:
        # Check if the path ends with an ignored extension
        if parsed.path.lower().endswith(IGNORED_EXTENSIONS):
            return False
        return True
    return False

def clean_and_summarize(soup):
    """
    Extracts title, full content, and a priority-based snippet from the page.
    Priority: <meta name="description"> > First 200 characters of content.
    """
    
    # 1. Title Extraction
    title_tag = soup.find('title')
    title = title_tag.text.strip() if title_tag else "Untitled Page"

    # 2. Full Content Extraction (for Lunr search indexing)
    
    # Remove script, style, and navigation elements to clean up text
    for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'form']):
        tag.decompose()
        
    # Get all text and clean up whitespace
    content = soup.body.get_text(separator=' ', strip=True) if soup.body else ""

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

async def crawl(url, client):
    """Fetches a single URL, processes it, and extracts new links."""
    global index_data

    # Check limits
    if len(crawled_urls) >= MAX_PAGES:
        print(f"Crawler hit MAX_PAGES limit ({MAX_PAGES}). Stopping crawl.")
        return

    # Skip if already processed
    if url in crawled_urls:
        return

    crawled_urls.add(url)
    print(f"Crawling: {url} ({len(crawled_urls)}/{MAX_PAGES})")

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
                # Only add to queue if it hasn't been crawled yet
                if absolute_url not in crawled_urls and absolute_url not in to_crawl_queue:
                    to_crawl_queue.add(absolute_url)

    except httpx.HTTPError as e:
        # Handle connection errors, timeouts, 404s, etc.
        print(f"  [ERROR] HTTP failure for {url}: {e}")
    except Exception as e:
        print(f"  [ERROR] General error processing {url}: {e}")


async def run_crawler():
    """Main asynchronous crawler loop."""
    # Use httpx.AsyncClient for connection pooling and better performance
    async with httpx.AsyncClient() as client:
        
        while to_crawl_queue and len(crawled_urls) < MAX_PAGES:
            # Get up to 5 URLs to process concurrently in the next batch
            batch_urls = []
            
            # Simple way to pop URLs from the set: convert to list, slice, and remove
            urls_list = list(to_crawl_queue)
            batch_urls = urls_list[:5]
            
            for url in batch_urls:
                to_crawl_queue.discard(url)

            if not batch_urls:
                break

            # Create and run a list of crawl tasks concurrently
            tasks = [crawl(url, client) for url in batch_urls]
            await asyncio.gather(*tasks)

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
    # Ensure all URLs start with the target root for the crawl logic to work
    if not TARGET_ROOT.startswith("http"):
         print(f"FATAL: TARGET_ROOT must be a full URL (e.g., https://yoursite.com). Current: {TARGET_ROOT}")
         sys.exit(1)
         
    # Run the main asynchronous function
    asyncio.run(run_crawler())
