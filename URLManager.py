import os
import json
from urllib.parse import urlparse
from collections import deque

class URLManager:
    """
    URLManager for domain-affinity crawling with state persistence.
    Ensures:
    - Each URL is only assigned to one worker at a time.
    - Worker sticks to a domain while it has URLs.
    - Failed URLs are stored for retry on next load.
    """

    DATA_FOLDER = "urls_data"
    STATE_FILE = os.path.join(DATA_FOLDER, "url_manager_state.json")
    FAILED_FILE = os.path.join(DATA_FOLDER, "failed_urls.json")

    def __init__(self):
        os.makedirs(self.DATA_FOLDER, exist_ok=True)

        # domain -> deque of URLs
        self.domain_map = {}
        # URLs already visited
        self.visited = set()
        # URLs currently being crawled
        self.being_crawled = set()
        # URLs that failed to fetch
        self.failed_urls = set()

        self.load_state()

    def add_url(self, url):
        """Add URL if not visited, being crawled, or failed"""
        if url in self.visited or url in self.being_crawled or url in self.failed_urls:
            return
        domain = urlparse(url).netloc
        if domain not in self.domain_map:
            self.domain_map[domain] = deque()
        if url not in self.domain_map[domain]:
            self.domain_map[domain].append(url)

    def mark_visited(self, url):
        """Mark URL as visited and remove it from being_crawled"""
        self.visited.add(url)
        self.being_crawled.discard(url)
        domain = urlparse(url).netloc
        if domain in self.domain_map and url in self.domain_map[domain]:
            self.domain_map[domain].remove(url)
        if domain in self.domain_map and not self.domain_map[domain]:
            del self.domain_map[domain]

    def mark_failed(self, url):
        """Mark a URL as failed for retry later"""
        self.being_crawled.discard(url)
        self.failed_urls.add(url)

    def get_url_for_domain(self, domain):
        """Get a URL from the given domain, respecting being_crawled"""
        if domain in self.domain_map:
            while self.domain_map[domain]:
                url = self.domain_map[domain].popleft()
                if url not in self.visited and url not in self.being_crawled:
                    self.being_crawled.add(url)
                    return url
            del self.domain_map[domain]
        return None

    def get_new_domain(self):
        """Get a new domain and URL to crawl"""
        for domain, queue in list(self.domain_map.items()):
            while queue:
                url = queue.popleft()
                if url not in self.visited and url not in self.being_crawled:
                    self.being_crawled.add(url)
                    return domain, url
            del self.domain_map[domain]
        return None, None

    def has_pending_urls(self):
        """Check if there are URLs left to crawl"""
        return any(queue for queue in self.domain_map.values()) or bool(self.being_crawled) or bool(self.failed_urls)

    # =========================
    # State persistence methods
    # =========================
    def save_state(self):
        """Save current state and failed URLs to files"""
        state_data = {
            "domain_map": {k: list(v) for k, v in self.domain_map.items()},
            "visited": list(self.visited),
            "being_crawled": list(self.being_crawled)
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state_data, f, indent=2)

        with open(self.FAILED_FILE, "w") as f:
            json.dump(list(self.failed_urls), f, indent=2)

    def load_state(self):
        """Load state and failed URLs from files"""
        if os.path.exists(self.STATE_FILE):
            with open(self.STATE_FILE, "r") as f:
                state_data = json.load(f)
            self.domain_map = {k: deque(v) for k, v in state_data.get("domain_map", {}).items()}
            self.visited = set(state_data.get("visited", []))
            self.being_crawled = set(state_data.get("being_crawled", []))

        if os.path.exists(self.FAILED_FILE):
            with open(self.FAILED_FILE, "r") as f:
                self.failed_urls = set(json.load(f))
            # Optionally, re-add failed URLs to domain map for retry
            for url in list(self.failed_urls):
                self.add_url(url)
            self.failed_urls.clear()
