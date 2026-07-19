import os
import re
import time
import sqlite3
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ==========================================
# CONFIGURATION SYSTEM
# ==========================================
FORCE_FRESH_START = True       # Force clean state
CACHE_VALIDITY_HOURS = 168     # 7 Days revalidation window
PLAYLIST_NAME = "playlist.m3u"
GITHUB_REPO_PATH = "VenkyVishy/iptv-updater"

class BulletproofStreamEngine:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.init_database()
        self.init_m3u_file()
        
        # Public, high-yield structured master databases
        self.master_sources = [
            "https://iptv-org.github.io/iptv/index.m3u",
            "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
            "https://gist.githubusercontent.com/abidr/b1f537c511d1f1df965d7f3ff0f2ec36/raw/iptv.m3u"
        ]
        self.global_registry = []

    def init_database(self):
        self.conn = sqlite3.connect("iptv_advanced_cache.db", check_same_thread=False)
        self.cursor = self.conn.cursor()
        if FORCE_FRESH_START:
            self.cursor.execute("DROP TABLE IF EXISTS stream_cache")
            self.cursor.execute("DROP TABLE IF EXISTS processed_channels")
            self.conn.commit()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stream_cache (
                channel_key TEXT PRIMARY KEY, url TEXT, timestamp INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_channels (
                channel_name TEXT PRIMARY KEY, country TEXT
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
        if row and (int(time.time()) - row[1] < (CACHE_VALIDITY_HOURS * 3600)):
            return row[0]
        return None

    def save_to_cache(self, channel_key, url):
        self.cursor.execute("INSERT OR REPLACE INTO stream_cache VALUES (?, ?, ?)", (channel_key, url, int(time.time())))
        self.conn.commit()

    def add_processed_channel(self, name, country):
        self.cursor.execute('INSERT OR IGNORE INTO processed_channels VALUES (?, ?)', (name, country))
        self.conn.commit()

    def load_master_indices(self):
        """Pre-downloads and compiles massive working streaming arrays into memory."""
        print("📥 Downloading master global streaming databases...")
        compiled_streams = []
        
        for url in self.master_sources:
            try:
                res = requests.get(url, headers=self.headers, timeout=15)
                if res.status_code == 200:
                    lines = res.text.splitlines()
                    current_name = None
                    for line in lines:
                        line = line.strip()
                        if line.startswith("#EXTINF:"):
                            # Parse out channel name
                            name_match = re.search(r',([^,]+)$', line)
                            if name_match:
                                current_name = name_match.group(1).strip()
                        elif line.startswith("http") and current_name:
                            compiled_streams.append({"name": current_name, "url": line})
            except Exception as e:
                print(f" ⚠️ High-yield fetch skip for {url}: {e}")
                
        print(f"✅ Ingested {len(compiled_streams)} potential stream targets.")
        self.global_registry = compiled_streams

    def check_stream_health(self, url):
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

    def process_query(self, query, country="GLOBAL"):
        norm_query = re.sub(r'[^\w\s]', '', query.lower()).strip()
        self.add_processed_channel(query, country)

        cached_url = self.get_cached_stream(norm_query)
        if cached_url:
            print(f"⚡ [CACHE VALID] Found unexpired stream -> {query}")
            return True

        print(f"⚠️  Cache missing/stale for [{query}]. Searching active master data metrics...")
        
        # Filter stream records containing the channel query name
        candidates = []
        for item in self.global_registry:
            if norm_query in re.sub(r'[^\w\s]', '', item["name"].lower()):
                candidates.append(item["url"])
        
        candidates = list(set(candidates))
        print(f"   📊 Discovered {len(candidates)} candidate matches. Verifying stream health...")

        if candidates:
            with ThreadPoolExecutor(max_workers=8) as validator:
                futures = [validator.submit(self.check_stream_health, url) for url in candidates[:40]]
                for f in as_completed(futures):
                    is_alive, working_url = f.result()
                    if is_alive:
                        print(f"   🟢 [SUCCESS] Active live stream verified -> {working_url}")
                        self.save_to_cache(norm_query, working_url)
                        return True
                        
        print(f"   ❌ No functional alternative streams found for: {query}")
        return False

    def write_final_playlist(self):
        print(f"📝 Compiling validated streams into {PLAYLIST_NAME}...")
        self.cursor.execute("SELECT channel_name, country FROM processed_channels")
        all_channels = self.cursor.fetchall()
        
        with open(PLAYLIST_NAME, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")
            for name, country in all_channels:
                norm_query = re.sub(r'[^\w\s]', '', name.lower()).strip()
                self.cursor.execute("SELECT url FROM stream_cache WHERE channel_key = ?", (norm_query,))
                row = self.cursor.fetchone()
                if row:
                    f.write(f'#EXTINF:-1 tvg-name="{name}" tvg-country="{country}",{name}\n')
                    f.write(f"{row[0]}\n")

    def sync_to_github(self):
        print(f"\n🚀 Initiating Git Sync toward {GITHUB_REPO_PATH}...")
        try:
            # Track script, database, and playlist files securely
            subprocess.run(["git", "add", PLAYLIST_NAME, "iptv_advanced_cache.db", "merge_playlists.py"], check=True)
            
            # Check if there are staging updates to prevent empty commit errors
            status = subprocess.run(["git", "diff", "--cached", "--quiet"])
            if status.returncode == 0:
                print("ℹ️ No streaming matrix modifications found. Skipping Git push.")
                return

            commit_msg = f"🤖 Auto-Update: Playlist Synchronized | Epoch {int(time.time())}"
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            
            print("📥 Pulling remote modifications...")
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=True)
            
            print("📤 Pushing payload to GitHub...")
            subprocess.run(["git", "push", "origin", "main"], check=True)
            print("🚀 [SUCCESSFUL DEPLOYMENT] Playlist changes live on GitHub.")
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Git Sync failed: {e}")

# ==========================================
# RUNTIME ENTRY POINT
# ==========================================
def main():
    engine = BulletproofStreamEngine()
    engine.load_master_indices()
    
    default_ingest_targets = [
        {"name": "HBO", "country": "US"},
        {"name": "Sky Sports", "country": "UK"},
        {"name": "ESPN", "country": "US"}
    ]

    print("🛠️  Processing update cycles...")
    for target in default_ingest_targets:
        engine.process_query(target["name"], target["country"])
        
    engine.write_final_playlist()
    engine.sync_to_github()

if __name__ == "__main__":
    main()
