import os
import json
import asyncio
from urllib.parse import urlparse
from collections import deque
import aiohttp
import random

class URLManager:
    DATA_FOLDER = "urls_data"
    STATE_FILE = os.path.join(DATA_FOLDER, "url_manager_state.json")
    FAILED_FILE = os.path.join(DATA_FOLDER, "failed_urls.json")
    ROBOTS_FOLDER = os.path.join(DATA_FOLDER, "robots_txt")

    def __init__(self, global_queue_maxsize=0, disable_robots=False):
        os.makedirs(self.DATA_FOLDER, exist_ok=True)
        os.makedirs(self.ROBOTS_FOLDER, exist_ok=True)
        # single global queue for all URLs (unbounded by default)
        # Use a set to track URLs that are queued to avoid duplicates
        self.queued = set()
        self.visited = set()
        self.being_crawled = set()
        self.failed_urls = set()

        # robots
        self.robots_txt = {}
        self.robots_lock = asyncio.Lock()
        self.disable_robots = disable_robots

        # global async queue for workers
        # maxsize=0 makes the queue unbounded
        self.global_queue = asyncio.Queue(maxsize=global_queue_maxsize)
        # optional per-domain scrape limiting (kept for compatibility)
        self.domain_scrape_tracker = {}
        self.domain_scrape_limit = 100000

    # -----------------------
    # Internal helpers
    # -----------------------
    def _get_domain(self, url):
        return urlparse(url).netloc

    # -----------------------
    # Async robots.txt
    # -----------------------
    async def _fetch_robots(self, domain):
        print(f"Fetching robots.txt for domain: {domain}")
        async with self.robots_lock:
            if domain in self.robots_txt:
                return self.robots_txt[domain]

            robots_path = os.path.join(self.ROBOTS_FOLDER, f"{domain}.txt")
            if os.path.exists(robots_path):
                try:
                    with open(robots_path, "r", encoding="utf-8") as f:
                        text = f.read()
                    from urllib.robotparser import RobotFileParser
                    rp = RobotFileParser()
                    rp.parse(text.splitlines())
                    self.robots_txt[domain] = rp
                    return rp
                except:
                    pass

            robots_url = f"https://{domain}/robots.txt"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(robots_url, timeout=5) as resp:
                        if resp.status != 200:
                            self.robots_txt[domain] = None
                            return None
                        text = await resp.text()
            except:
                self.robots_txt[domain] = None
                return None

            try:
                with open(robots_path, "w", encoding="utf-8") as f:
                    f.write(text)
            except:
                pass

            from urllib.robotparser import RobotFileParser
            rp = RobotFileParser()
            rp.parse(text.splitlines())
            self.robots_txt[domain] = rp
            return rp

    async def is_allowed(self, url):
        if self.disable_robots:
            return True
        domain = self._get_domain(url)
        if domain not in self.robots_txt:
            await self._fetch_robots(domain)
        rp = self.robots_txt.get(domain)
        if rp is None:
            return True
        return rp.can_fetch("*", url)

    # -----------------------
    # Add URL
    # -----------------------
    async def add_url(self, url):
        domain = self._get_domain(url)
        if domain not in self.domain_scrape_tracker:
            self.domain_scrape_tracker[domain] = 0

        # skip if seen, being crawled, failed, or already queued
        if url in self.visited or url in self.being_crawled or url in self.failed_urls or url in self.queued:
            return

        if self.domain_scrape_tracker[domain] >= self.domain_scrape_limit:
            if random.random() < 0.05:  # 5% chance to give the URL a pass
                pass
            else:
                return
        if not await self.is_allowed(url) and not self.disable_robots:
            return

        # increment tracker and push to global queue
        self.domain_scrape_tracker[domain] += 1
        try:
            self.global_queue.put_nowait(url)
            self.queued.add(url)
        except Exception:
            # put_nowait can only raise QueueFull for bounded queues; ignore otherwise
            await self.global_queue.put(url)
            self.queued.add(url)

    # -----------------------
    # Worker fetch
    # -----------------------
    async def get_url(self):
        """
        Worker calls this to get a single URL.
        Returns next URL from the global queue.
        """
        # A very small chance to shuffle the queue to avoid starvation
        if random.random() < 0.01:
            temp_list = []
            while not self.global_queue.empty():
                temp_list.append(await self.global_queue.get())
            random.shuffle(temp_list)
            for url in temp_list:
                await self.global_queue.put(url)
        try:
            url = await self.global_queue.get()
            # move from queued->being_crawled
            self.queued.discard(url)
            self.being_crawled.add(url)
            return url
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    async def _refill_global_queue(self):
        # Not used in single global queue mode but keep for compatibility
        return

    # -----------------------
    # Mark visited / failed
    # -----------------------
    def mark_visited(self, url):
        self.visited.add(url)
        self.being_crawled.discard(url)
        try:
            self.global_queue.task_done()
        except Exception:
            pass

    def mark_failed(self, url):
        self.failed_urls.add(url)
        self.being_crawled.discard(url)
        try:
            self.global_queue.task_done()
        except Exception:
            pass

    def has_pending_urls(self):
        return not self.global_queue.empty() or bool(self.queued)

    # -----------------------
    # Persistence
    # -----------------------
    def save_state(self):
        state_data = {
            "queued": list(self.queued),
            "visited": list(self.visited),
            "being_crawled": list(self.being_crawled),
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state_data, f, indent=2)

        with open(self.FAILED_FILE, "w") as f:
            json.dump(list(self.failed_urls), f, indent=2)

    async def load_state(self):
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, "r") as f:
                data = json.load(f)

            self.queued = set(data.get("queued", []))
            self.visited = set(data.get("visited", []))
            self.being_crawled = set(data.get("being_crawled", []))

        if os.path.exists(self.FAILED_FILE):
            with open(self.FAILED_FILE, "r") as f:
                failed_urls = set(json.load(f))
                self.visited.update(failed_urls)

        # refill global queue after loading state
        for url in list(self.queued):
            try:
                self.global_queue.put_nowait(url)
            except Exception:
                await self.global_queue.put(url)


