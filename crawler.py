import asyncio
import aiohttp
import os
import json
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from URLManager import URLManager
from SimhashManager import SimhashManager
import hashlib
import random

# =======================
# Helpers
# =======================
def url_to_id(url: str) -> str:
    """Stable SHA-256 ID for URL."""
    return hashlib.sha256(url.encode()).hexdigest()

# =======================
# Config
# =======================
DATA_FOLDER = "urls_data/raw"
URL_MAP_FILE = "urls_data/url_map.json"
os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs("urls_data", exist_ok=True)

NUM_WORKERS = 100
TARGET_PAGES = 1000
SAVE_INTERVAL = 10  # seconds

executor = ThreadPoolExecutor(max_workers=NUM_WORKERS*3)
save_queue = asyncio.Queue()
pages_crawled = 0
soft_limit_reached = False

# URL map to reverse filename → URL
url_map = {}

# =======================
# Parsing / Extraction
# =======================
def extract_canonical(soup):
    tag = soup.find("link", rel="canonical")
    return tag["href"].strip() if tag and tag.get("href") else None


def is_page_english_by_metadata(soup):

    html_tag = soup.find("html")
    if html_tag:
        lang = html_tag.get("lang") or html_tag.get("xml:lang")
        if lang and lang.lower().startswith("en"):
            return True
        else:
            return False

    # Check <meta http-equiv="content-language">
    meta = soup.find("meta", attrs={"http-equiv": "content-language"})
    if meta:
        lang = meta.get("content", "").lower()
        if "en" in lang:
            return True
        else:
            return False

    # No definitive metadata → return None (unknown)
    return None

def extract_text(html):
    soup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.decompose()
    return soup.get_text(separator=" ", strip=True)

def extract_links(base_url, html):
    links = set()
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http"):
            links.add(href)
        elif href.startswith("/"):
            parsed = urlparse(base_url)
            links.add(f"{parsed.scheme}://{parsed.netloc}{href}")
    return links

# =======================
# Disk writer task
# =======================
async def disk_writer():
    while True:
        item = await save_queue.get()
        if item is None:
            break
        filename, html, url = item
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        url_map[url_to_id(url)] = url  # update map
        save_queue.task_done()
        save_url_map()

# =======================
# Crawl worker
# =======================# 
async def crawl_worker(name, url_manager, simhash_manager):
    global pages_crawled, soft_limit_reached
    async with aiohttp.ClientSession(headers=random.choice([
        {"User-Agent": "Mozilla/5.0"}, 
        {"User-Agent": "Chrome/91.0.4472.124"}, 
        {"User-Agent": "Safari/537.36"},
        {"User-Agent": "Edge/18.18363"},
        {"User-Agent": "Opera/9.80"},
        {"User-Agent": "Firefox/89.0"}
        ])) as session:
        while True:
            if soft_limit_reached:
                break
            url = await url_manager.get_url()
            if url is None:
                # No URL available, sleep briefly
                await asyncio.sleep(0.5)
                continue

            try:
                print(f"[{name}] Crawling: {url}")
                async with session.get(url, timeout=10) as resp:
                    # If status not OK, mark failed
                    if int(resp.status ) >= 300:
                        url_manager.mark_failed(url)
                        print(f"[{name}] Failed {url}: Status {resp.status}")
                        continue

                    if "text/html" not in resp.headers.get("Content-Type", ""):
                        url_manager.mark_visited(url)
                        continue


                    html = await resp.text()
                    loop = asyncio.get_running_loop()

                    # Parse HTML
                    soup = await loop.run_in_executor(None, BeautifulSoup, html, "html.parser")
                    # Check canonical URL
                    canonical_url = await loop.run_in_executor(None, extract_canonical, soup)
                    if canonical_url and canonical_url != url:
                        await url_manager.add_url(canonical_url)
                        url_manager.mark_visited(url)
                        continue
                    # Check language metadata
                    is_english = await loop.run_in_executor(None, is_page_english_by_metadata, soup)
                    if is_english is False:
                        url_manager.mark_visited(url)
                        continue

                    # Extract text & simhash
                    page_text = await loop.run_in_executor(None, extract_text, html)
                    is_new = await loop.run_in_executor(None, simhash_manager.add_page, url, page_text)
                    if not is_new:
                        url_manager.mark_visited(url)
                        continue

                    # Save HTML
                    doc_id = url_to_id(url)
                    filename = os.path.join(DATA_FOLDER, f"{doc_id}.html")
                    await save_queue.put((filename, html, url))
                    url_manager.mark_visited(url)

                    # Extract links
                    links = await loop.run_in_executor(None, extract_links, url, html)
                    for link in links:
                        await url_manager.add_url(link)

                    # Increment counter and check soft limit
                    pages_crawled += 1
                    if pages_crawled >= TARGET_PAGES:
                        soft_limit_reached = True
                        print(f"[{name}] Reached soft limit (~{TARGET_PAGES} pages)")
                        url_manager.save_state()
                        simhash_manager.save_state()
                        break

            except Exception as e:
                url_manager.mark_failed(url)
                print(f"[{name}] Failed {url}: {e}")


# =======================
# Periodic Simhash save
# =======================
async def periodic_simhash_save(simhash_manager,url_manager, interval=SAVE_INTERVAL):
    while True:
        await asyncio.sleep(interval)
        simhash_manager.save_state()
        url_manager.save_state()
        print(f"[SimhashManager] State saved.")

# =======================
# Save URL map
# =======================
def save_url_map():
    with open(URL_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(url_map, f, indent=2)

def load_url_map():
    global url_map
    if os.path.exists(URL_MAP_FILE):
        with open(URL_MAP_FILE, "r", encoding="utf-8") as f:
            url_map = json.load(f)

# =======================
# Main
# =======================
async def main():
    url_manager = URLManager(disable_robots=True)
    await url_manager.load_state()

    simhash_manager = SimhashManager()
    simhash_manager.load_state()

    load_url_map()

    # Seed URLs
    if not url_manager.has_pending_urls():
        seeds = [

        ]
        for url in seeds:
            await url_manager.add_url(url)

    crawl_tasks = [crawl_worker(f"Worker-{i+1}", url_manager, simhash_manager) for i in range(NUM_WORKERS)]
    save_task = asyncio.create_task(disk_writer())
    simhash_task = asyncio.create_task(periodic_simhash_save(simhash_manager, url_manager))

    print("Starting crawl...")
    await asyncio.gather(*crawl_tasks)

    # Finish disk writer
    await save_queue.put(None)
    await save_task

    simhash_task.cancel()
    try:
        await simhash_task
    except asyncio.CancelledError:
        pass

    url_manager.save_state()
    simhash_manager.save_state()
    save_url_map()
    print(f"Crawling finished! Total pages crawled: {pages_crawled}")

if __name__ == "__main__":
    asyncio.run(main())
