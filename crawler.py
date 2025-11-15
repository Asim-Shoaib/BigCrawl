import asyncio
import aiohttp
import os
from URLManager import URLManager
from urllib.parse import urlparse
from bs4 import BeautifulSoup

DATA_FOLDER = "urls_data/raw"
os.makedirs(DATA_FOLDER, exist_ok=True)

MAX_CONNECTIONS_PER_DOMAIN = 4

# Global counter and lock
pages_crawled = 0
pages_lock = asyncio.Lock()  # protect counter for concurrency

# =======================
# Worker function
# =======================
async def crawl_worker(name, url_manager, pages_to_crawl):
    global pages_crawled

    while url_manager.has_pending_urls():
        async with pages_lock:
            if pages_crawled >= pages_to_crawl:
                return

        domain, first_url = url_manager.get_new_domain()
        if domain is None:
            await asyncio.sleep(1)
            continue

        print(f"[{name}] Assigned domain: {domain}")
        urls_to_crawl = [first_url]
        sem = asyncio.Semaphore(MAX_CONNECTIONS_PER_DOMAIN)

        async with aiohttp.ClientSession() as session:

            async def fetch_url(url):
                global pages_crawled
                async with sem:
                    async with pages_lock:
                        if pages_crawled >= pages_to_crawl:
                            return
                    try:
                        async with session.get(url, timeout=10) as resp:
                            content_type = resp.headers.get("Content-Type", "")
                            if "text/html" not in content_type:
                                url_manager.mark_visited(url)
                                return

                            html = await resp.text()
                            filename = os.path.join(DATA_FOLDER, f"{hash(url)}.html")
                            with open(filename, "w", encoding="utf-8") as f:
                                f.write(html)
                            url_manager.mark_visited(url)

                            async with pages_lock:
                                pages_crawled += 1
                                if pages_crawled >= pages_to_crawl:
                                    return

                            # Extract links
                            links = extract_links(url, html)
                            for link in links:
                                url_manager.add_url(link)
                            for link in links:
                                if urlparse(link).netloc == domain:
                                    urls_to_crawl.append(link)

                    except Exception as e:
                        print(f"[{name}] Failed {url}: {e}")
                        url_manager.mark_failed(url)

            while urls_to_crawl:
                async with pages_lock:
                    if pages_crawled >= pages_to_crawl:
                        return
                tasks = []
                while urls_to_crawl and len(tasks) < MAX_CONNECTIONS_PER_DOMAIN:
                    url = urls_to_crawl.pop(0)
                    tasks.append(fetch_url(url))
                await asyncio.gather(*tasks)

        print(f"[{name}] Finished domain: {domain}")


# =======================
# Link extractor
# =======================
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
# Main
# =======================
async def main(pages_to_crawl=50):
    url_manager = URLManager()
    url_manager.load_state()

    if not url_manager.has_pending_urls():
        seeds = [
            "https://aplusbloggers.blogspot.com/",
        ]
        for url in seeds:
            url_manager.add_url(url)

    NUM_WORKERS = 5
    tasks = [crawl_worker(f"Worker-{i+1}", url_manager, pages_to_crawl) for i in range(NUM_WORKERS)]
    await asyncio.gather(*tasks)

    url_manager.save_state()
    print(f"Crawling finished! Total pages crawled: {pages_crawled}")


if __name__ == "__main__":
    asyncio.run(main(pages_to_crawl=100))  