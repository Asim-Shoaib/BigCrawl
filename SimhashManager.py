import os
import json
from simhash import Simhash, SimhashIndex

STATE_FOLDER = "urls_data"
os.makedirs(STATE_FOLDER, exist_ok=True)

class SimhashManager:
    """
    Manage Simhashes for near-duplicate detection.
    - k: maximum Hamming distance for duplicates
    - f: number of bits in Simhash (default 64)
    """
    def __init__(self, state_file="simhash_state.json", k=3, f=64):
        self.state_file = os.path.join(STATE_FOLDER, state_file)
        self.k = k
        self.f = f
        self.hashes = {}  # {page_id: Simhash object}
        self.index = SimhashIndex([], f=self.f, k=self.k)
        self.load_state()
    
    def compute_hash(self, text):
        """Compute Simhash for a text"""
        return Simhash(text, f=self.f)
    
    def add_page(self, page_id, text):
        """
        Add page and return True if not a duplicate,
        False if near-duplicate already exists.
        """
        page_hash = self.compute_hash(text)
                
        # Check for near-duplicates
        dupes = self.index.get_near_dups(page_hash)
        if dupes:
            # Near-duplicate found, skip
            return False
        
        # No duplicate → add
        self.hashes[page_id] = page_hash
        self.index.add(page_id, page_hash)
        return True
    
    def save_state(self):
        """Save hashes to file"""
        data = {pid: h.value for pid, h in self.hashes.items()}
        with open(self.state_file, "w") as f:
            json.dump(data, f)
        print(f"[SimhashManager] Saved {len(self.hashes)} hashes.")
    
    def load_state(self):
        """Load hashes from file"""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                data = json.load(f)
            self.hashes = {pid: Simhash(val, f=self.f) for pid, val in data.items()}
            self.index = SimhashIndex(
                [(pid, h) for pid, h in self.hashes.items()], f=self.f, k=self.k
            )
            print(f"[SimhashManager] Loaded {len(self.hashes)} hashes.")
        else:
            self.hashes = {}
            self.index = SimhashIndex([], f=self.f, k=self.k)
            print("[SimhashManager] No previous state found, starting fresh.")
