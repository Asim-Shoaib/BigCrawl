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

NUM_WORKERS = 350
TARGET_PAGES = 20000
SAVE_INTERVAL = 30  # seconds

executor = ThreadPoolExecutor(max_workers=NUM_WORKERS*3)
save_queue = asyncio.Queue()
pages_crawled = 0
soft_limit_reached = False

# URL map to reverse filename → URL
url_map = {}

# =======================
# Parsing / Extraction
# =======================
def extract_canonical(html):
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("link", rel="canonical")
    return tag["href"].strip() if tag and tag.get("href") else None

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
    async with aiohttp.ClientSession() as session:
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
                    if "text/html" not in resp.headers.get("Content-Type", ""):
                        url_manager.mark_visited(url)
                        continue

                    html = await resp.text()
                    loop = asyncio.get_running_loop()

                    # Canonical URL
                    canonical = await loop.run_in_executor(None, extract_canonical, html)
                    if canonical and canonical != url:
                        await url_manager.add_url(canonical)
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
    url_manager = URLManager()
    await url_manager.load_state()

    simhash_manager = SimhashManager()
    simhash_manager.load_state()

    load_url_map()

    # Seed URLs
    if not url_manager.has_pending_urls():
        seeds = [
        # --- Pakistan News / Media ---
        "https://www.dawn.com/",
        "https://www.geo.tv/",
        "https://www.express.pk/",
        "https://tribune.com.pk/",
        "https://www.samaa.tv/",
        "https://arynews.tv/",
        "https://www.bolnews.com/",
        "https://www.thenews.com.pk/",
        "https://www.pakistantoday.com.pk/",
        "https://www.radio.gov.pk/",

        # --- Government of Pakistan ---
        "https://www.pakistan.gov.pk/",
        "https://www.pbs.gov.pk/",                 # Pakistan Bureau of Statistics
        "https://www.senate.gov.pk/",
        "https://na.gov.pk/",                      # National Assembly
        "https://www.fbr.gov.pk/",
        "https://www.hec.gov.pk/",

        # --- Global News (Highly Link-Dense) ---
        "https://www.bbc.com/news",
        "https://edition.cnn.com/",
        "https://www.reuters.com/",
        "https://www.aljazeera.com/",
        "https://www.nytimes.com/",
        "https://www.theguardian.com/international",
        "https://www.wsj.com/",
        "https://time.com/",
        "https://www.economist.com/",

        # --- Technology, Companies, Big Personalities ---
        "https://techcrunch.com/",                 # startups + companies
        "https://www.theverge.com/",
        "https://www.wired.com/",
        "https://www.forbes.com/",
        "https://www.bloomberg.com/",
        "https://www.ft.com/",
        "https://www.crunchbase.com/",             # company data, founders, funding
        "https://www.linkedin.com/feed/",          # lots of company/person links
        "https://www.imdb.com/",                   # celebrities, actors
        "https://en.wikipedia.org/wiki/List_of_Pakistanis",
        "https://en.wikipedia.org/wiki/List_of_Internet_phenomena",
        "https://en.wikipedia.org/wiki/List_of_companies_by_market_capitalization",

        # --- Big Social / Trending / Pop Culture (Link-Rich) ---
        "https://www.youtube.com/trending",
        "https://twitter.com/explore",
        "https://www.reddit.com/r/worldnews/",
        "https://www.reddit.com/r/pakistan/",
        "https://www.reddit.com/r/news/",
        "https://www.reddit.com/r/politics/",
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
