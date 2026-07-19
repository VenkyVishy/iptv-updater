import os
import re
import time
import sqlite3
import subprocess
import urllib.parse
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ==========================================
# CONFIGURATION SYSTEM
# ==========================================
FORCE_FRESH_START = False      # True = clear all history and restart fresh
CACHE_VALIDITY_HOURS = 168     # 7 Days revalidation window
PLAYLIST_NAME = "playlist.m3u"
GITHUB_REPO_PATH = "VenkyVishy/iptv-updater"

class AdvancedStreamEngine:
    def __init__(self):
        # Using a highly standard modern browser fingerprint
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        # Comprehensive tracking signatures
        self.stream_regex = re.compile(
            r'(https?://[^\s\'"<>]+(?:\.m3u8|\.m3u|\.mp4|\.ts|\.git)(?:[^\s\'"<>]*))', 
            re.IGNORECASE
        )
        
        self.target_keywords = ["m3u", "m3u8", ".m3u", ".m3u8", ".git", "IPTV"]
        self.shortener_domains = ["tinyurl.com", "bit.ly", "cutt.ly", "t.co", "is.gd", "gg.gg", "shorturl.at"]
        
        self.init_database()
        self.init_m3u_file()

    def init_database(self):
        self.conn = sqlite3.connect("iptv_advanced_cache.db", check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        if FORCE_FRESH_START:
            print("🔥 [FORCE FRESH START] Resetting internal tables...")
            self.cursor.execute("DROP TABLE IF EXISTS stream_cache")
            self.cursor.execute("DROP TABLE IF EXISTS processed_channels")
            self.conn.commit()

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_cache (
                channel_key TEXT PRIMARY KEY,
                url TEXT,
                timestamp INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_channels (
                channel_name TEXT PRIMARY KEY,
                country TEXT
            )
        ''')
        self.conn.commit()

    def init_m3u_file(self):
        if FORCE_FRESH_START or not os.path.exists(PLAYLIST_NAME):
            with open(PLAYLIST_NAME, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")

    def get_cached_stream(self, channel_key):
        if FORCE_FRESH_START:
            return None
        self.cursor.execute("SELECT url, timestamp FROM stream_cache WHERE channel_key = ?", (channel_key,))
        row = self.cursor.fetchone()
        if row:
            url, timestamp = row
            if int(time.time()) - timestamp < (CACHE_VALIDITY_HOURS * 3600):
                return url
        return None

    def save_to_cache(self, channel_key, url):
        self.cursor.execute('''
            INSERT OR REPLACE INTO stream_cache (channel_key, url, timestamp)
            VALUES (?, ?, ?)
        ''', (channel_key, url, int(time.time())))
        self.conn.commit()

    def add_processed_channel(self, name, country):
        self.cursor.execute('INSERT OR IGNORE INTO processed_channels (channel_name, country) VALUES (?, ?)', (name, country))
        self.conn.commit()

    def get_all_processed_channels(self):
        self.cursor.execute("SELECT channel_name, country FROM processed_channels")
        return self.cursor.fetchall()

    def rebuild_m3u_from_cache(self):
        print(f"🔄 Rebuilding {PLAYLIST_NAME} using valid cached feeds...")
        with open(PLAYLIST_NAME, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            
        channels = self.get_all_processed_channels()
        for name, country in channels:
            norm_query = re.sub(r'[^\w\s]', '', name.lower()).strip()
            valid_url = self.get_cached_stream(norm_query)
            if valid_url:
                self._append_raw_to_m3u(name, valid_url, country)

    def _append_raw_to_m3u(self, name, url, country):
        with open(PLAYLIST_NAME, "r", encoding="utf-8") as f:
            content = f.read()
        if url in content:
            return
        with open(PLAYLIST_NAME, "a", encoding="utf-8") as f:
            f.write(f'#EXTINF:-1 tvg-name="{name}" tvg-country="{country}",{name}\n')
            f.write(f"{url}\n")
        print(f"💾 Appended link to {PLAYLIST_NAME} -> {name}")

    def resolve_short_url(self, url):
        if not any(domain in url for domain in self.shortener_domains):
            return url
        try:
            res = requests.head(url, headers=self.headers, timeout=4, allow_redirects=True)
            return res.url
        except Exception:
            return url

    def check_stream_health(self, url):
        url = self.resolve_short_url(url)
        try:
            res = requests.head(url, headers=self.headers, timeout=3, allow_redirects=True)
            if res.status_code == 200:
                return True, url
            with requests.get(url, headers=self.headers, timeout=3, stream=True) as r:
                if r.status_code == 200:
                    return True, url
        except Exception:
            pass
        return False, url

    # ==========================================
    # AGGRESSIVE GLOBAL WEB SCRAPER MATRIX
    # ==========================================
    def scrape_platform_raw(self, url):
        found = []
        try:
            res = requests.get(url, headers=self.headers, timeout=7)
            found.extend(self.stream_regex.findall(res.text))
            
            soup = BeautifulSoup(res.text, "html.parser")
            for link in soup.find_all(['a', 'iframe'], href=True):
                href = link['href']
                if any(ext in href.lower() for ext in ['.m3u8', '.m3u', '.git', '/raw/', 't.me/s/']):
                    found.append(href)
        except Exception:
            pass
        return found

    def harvest_fallback_hubs(self, query):
        """High-yield public API fallbacks when search engine scraper blocks occur."""
        hubs = [
            f"https://raw.githubusercontent.com/iptv-org/iptv/master/channels.json",
            f"https://t.me/s/IPTV_M3U8_Links",
            f"https://pastebin.com/raw/d2B8N15K" 
        ]
        links = []
        for hub in hubs:
            try:
                res = requests.get(hub, headers=self.headers, timeout=5)
                # Find matching target items containing our target text safely
                if query.lower() in res.text.lower():
                    links.extend(self.stream_regex.findall(res.text))
            except Exception:
                pass
        return links

    def gather_global_search_dorks(self, query):
        dork = f"{query} iptv m3u8 playlist filetype:m3u"
        encoded_dork = urllib.parse.quote_plus(dork)
        
        engines = {
            "duckduckgo": f"https://html.duckduckgo.com/html/?q={encoded_dork}",
            "bing": f"https://www.bing.com/search?q={encoded_dork}"
        }
        
        candidates = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(self.scrape_platform_raw, url): name for name, url in engines.items()}
            for f in as_completed(futures):
                candidates.extend(f.result())
                
        # Inject our internal structural fallback targets instantly
        candidates.extend(self.harvest_fallback_hubs(query))
        return list(set(candidates))

    def process_raw_hub_links(self, links):
        raw_urls = []
        for link in links:
            cleaned = link
            if "pastebin.com" in link and "/raw/" not in link:
                cleaned = link.replace("pastebin.com/", "pastebin.com/raw/")
            elif "github.com" in link and "/blob/" in link:
                cleaned = link.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
                
            if cleaned != link or any(cleaned.endswith(x) for x in ['.m3u', '.m3u8', '.ts', '.mp4']):
                raw_urls.append(cleaned)
        
        final_streams = []
        def fetch_raw(u):
            try:
                r = requests.get(u, headers=self.headers, timeout=4)
                return self.stream_regex.findall(r.text)
            except Exception:
                return []

        with ThreadPoolExecutor(max_workers=5) as exec_raw:
            results = exec_raw.map(fetch_raw, raw_urls[:20])
            for res_list in results:
                final_streams.extend(res_list)
        return final_streams

    def discover_alternative_streams(self, query):
        print(f"   🕵️  Running web matrices tracking matches for: {query}...")
        discovered_hubs = self.gather_global_search_dorks(query)
        verified_streams = self.process_raw_hub_links(discovered_hubs)
        
        # If still empty, scan raw hits directly inside our gathered hub sources
        if not verified_streams and discovered_hubs:
            verified_streams = [s for s in discovered_hubs if any(x in s for x in ['.m3u8', '.m3u'])]
            
        return list(set(verified_streams))

    def process_query(self, query, country="GLOBAL"):
        norm_query = re.sub(r'[^\w\s]', '', query.lower()).strip()
        self.add_processed_channel(query, country)

        cached_url = self.get_cached_stream(norm_query)
        if cached_url:
            print(f"⚡ [CACHE VALID] Found unexpired stream -> {query}")
            self._append_raw_to_m3u(query, cached_url, country)
            return True

        print(f"⚠️  Cache missing or stale for [{query}]. Scraping external web arrays...")
        alternatives = self.discover_alternative_streams(query)
        print(f"   📊 Discovered {len(alternatives)} stream signatures. Verifying live health status...")

        if alternatives:
            with ThreadPoolExecutor(max_workers=6) as validator:
                futures = [validator.submit(self.check_stream_health, url) for url in alternatives[:30]]
                for f in as_completed(futures):
                    is_alive, working_url = f.result()
                    if is_alive:
                        print(f"   🟢 [SUCCESS] Valid dynamic link captured -> {working_url}")
                        self.save_to_cache(norm_query, working_url)
                        self._append_raw_to_m3u(query, working_url, country)
                        return True
                        
        print(f"   ❌ No open channels streaming active feeds online for: {query}")
        return False

    def sync_to_github(self):
        print(f"\n🚀 Initiating Git Sync toward {GITHUB_REPO_PATH}...")
        try:
            status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
            if not status.stdout.strip():
                print("ℹ️  No changes made to target playlist output. Skipping deployment.")
                return

            subprocess.run(["git", "add", PLAYLIST_NAME], check=True)
            subprocess.run(["git", "add", "iptv_advanced_cache.db"], check=True)
            
            commit_msg = f"🤖 Auto-Update: Playlist Synchronized | Epoch {int(time.time())}"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            
            print("📥 Pulling remote alterations upstream to reconcile branches...")
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=True)
            
            print("📤 Pushing verified playlist payloads upstream to repository remote branches...")
            subprocess.run(["git", "push", "origin", "main"], check=True)
            print("🚀 [SUCCESSFUL DEPLOYMENT] Playlist changes successfully committed and deployed.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Git pipeline execution error: {e}")

# ==========================================
# RUNTIME ENTRY POINT
# ==========================================
def main():
    engine = AdvancedStreamEngine()
    
    # Target search queries to scan and process
    default_ingest_targets = [
        {"name": "HBO Max", "country": "US"},
        {"name": "Sky Sports Main Event", "country": "UK"},
        {"name": "ESPN US Live", "country": "US"}
    ]

    print("🛠️  Processing update cycles...")
    for target in default_ingest_targets:
        engine.process_query(target["name"], target["country"])
        
    engine.rebuild_m3u_from_cache()
    engine.sync_to_github()

if __name__ == "__main__":
    main()
