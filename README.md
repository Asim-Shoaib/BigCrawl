# WebCrawler — focused README (crawler.py)

This repository contains a small asynchronous web crawler. The main entrypoint and the focus of configuration is `crawler.py` — everything else (URL manager, simhash manager, state files) supports it.

This README gives a concise guide for running the crawler and the exact file/line locations of variables you can edit to tune the bot.

## Highlights / features
- Asynchronous crawling with configurable worker concurrency.
- Global frontier implemented using `URLManager` (single global queue).
- Robots.txt checks (configurable per instance).
- Duplicate / near-duplicate detection via `SimhashManager`.
- Persistent state: queued/visited/failed/simhash stored under `urls_data/` so crawls can resume.

## Quick start

1. Install requirements (see `requirements.txt` added to the project):

```powershell
python -m pip install -r requirements.txt
```

2. Run the crawler (from repo root):

```powershell
python crawler.py
```

3. To seed new start URLs, edit the `seeds = []` list (file and line shown below) or add them at runtime via `URLManager.add_url()`.

## Files of interest
- `crawler.py` — main crawler. Edit this to change crawler runtime behavior.
- `URLManager.py` — global queue URL manager and persistence.
- `SimhashManager.py` — near-duplicate detection using simhash.
- `urls_data/` — persisted state and raw HTML pages.

## Where to change behavior (file + line numbers)
Below are the most useful variables and places to edit in `crawler.py` (paths and line numbers are based on the current repository code):

- `DATA_FOLDER` (line 23)
  - Location where raw HTML files are saved. Default: `"urls_data/raw"`.

- `URL_MAP_FILE` (line 24)
  - JSON that maps saved filenames back to original URLs. Default: `"urls_data/url_map.json"`.

- `NUM_WORKERS` (line 28)
  - Number of concurrent crawl workers. Increase for more parallelism, but ensure your machine/network can handle it. Default: `100`.

- `TARGET_PAGES` (line 29)
  - Soft limit of pages to crawl during a run. When reached, workers stop. Default: `1000`.

- `SAVE_INTERVAL` (line 30)
  - Interval (seconds) at which simhash and URL manager state are saved. Default: `10` seconds.

- `executor = ThreadPoolExecutor(max_workers=NUM_WORKERS*3)` (line 32)
  - Thread pool used to run blocking parsing tasks. Increase if parsing becomes a bottleneck.

- `save_queue = asyncio.Queue()` (line 33)
  - Queue used by workers to send pages to be written to disk by `disk_writer()`.

- `User-Agent` list (lines 108–114)
  - The crawler picks a random User-Agent header from this list for each `ClientSession`. Edit or expand this list to vary headers.

- `url_manager = URLManager(disable_robots=True)` (line 215)
  - Instantiation of the URL manager. You can change `disable_robots` to `False` to enable robots.txt checks. You can also pass `global_queue_maxsize` to the constructor.

- `seeds = []` (line 225)
  - Seed URLs to start crawling if no pending URLs exist in state. Add your start URLs here.

- `crawl_tasks` creation uses `NUM_WORKERS` (line 231)
  - Worker tasks are created with the number of workers above.

If you need different line numbers (file changed later), search for the variable names above in `crawler.py` to find the current location.

## Runtime behavior summary
- Workers fetch URLs from `URLManager.get_url()` (the global queue).
- Each worker performs basic filtering:
  - Skip non-HTML responses.
  - Respect canonical URLs (if canonical differs, add canonical and skip original).
  - Skip pages that metadata says are not English (heuristic) or that are near-duplicates per Simhash.
  - Save HTML to `DATA_FOLDER` and update `url_map`.
  - Extract links and add them back to the `URLManager` frontier.
- `SimhashManager` prevents near-duplicate pages from being saved/processed.

## Persistence & resuming
- `URLManager.save_state()` and `SimhashManager.save_state()` are called periodically and when soft limits are reached.
- State files live in `urls_data/` and are reloaded on start by `load_state()` routines.

## Requirements
See `requirements.txt` for the exact packages used. Basic list includes:
- aiohttp
- beautifulsoup4
- simhash

## Example quick edits
- Reduce worker count to 10 (edit line 28):

```python
NUM_WORKERS = 10
```

- Add seed URLs (edit line 225):

```python
seeds = [
    'https://example.com',
    'https://example.org',
]
```

## Safety and etiquette
- When enabling robots (recommended), respect `robots.txt` and crawl politeness. If `disable_robots=True` is used, only do so for controlled environments or with permission.
