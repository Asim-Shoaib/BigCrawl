import os, json
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup
from tqdm import tqdm  # <-- added
from pathlib import Path

def is_page_english_by_metadata(html):
    soup = BeautifulSoup(html, "html.parser")

    html_tag = soup.find("html")
    if html_tag:
        lang = html_tag.get("lang") or html_tag.get("xml:lang")
        if lang and lang.lower().startswith("en"):
            return True
        else:
            return False

    meta = soup.find("meta", attrs={"http-equiv": "content-language"})
    if meta:
        lang = meta.get("content", "").lower()
        if "en" in lang:
            return True
        else:
            return False

    return None

def should_delete_page(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html = f.read()
        if not is_page_english_by_metadata(html):
            return True
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['style', 'script']):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        words = text.split()
        return len(words) < 200
    except:
        return True

def process_file(file_key):
    file_path = os.path.join(Path(os.getcwd()).parent, 'urls_data', 'raw', f'{file_key}.html')
    try:
        delete_flag = should_delete_page(file_path)
        return file_path, delete_flag
    except Exception as e:
        print(f"Error {file_path}: {e}")
        return file_path, True

def run_multiprocessing(file_keys, max_workers=None):
    to_delete = []
    max_workers = max_workers or os.cpu_count()

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, key): key for key in file_keys}

        # Wrap as_completed with tqdm for progress
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            file_path, delete_flag = future.result()
            if delete_flag:
                to_delete.append(file_path)

    return to_delete

if __name__ == "__main__":
    url_map_path = os.path.join(Path(os.getcwd()).parent, 'urls_data', 'url_map.json')
    with open(url_map_path, 'r') as f:
        url_map = json.load(f)

    all_files = list(url_map.keys())[47000:]
    bad_files = run_multiprocessing(all_files, max_workers=os.cpu_count())
    print("Bad files:", len(bad_files))
    with open('to_delete.txt', 'a') as f:
        for file in bad_files:
            f.write(file + '\n')
    

