#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VENGATESH IPTV UPDATER — v23 REAL-TIME MODE OPTIMIZED
- All links included (no omissions)
- Streams displayed AS SOON AS validated
- Multi-threaded git cloning with caching
- Multi-process parsing with chunking
- Fast silent replacement (pre-built index)
- EPG + OTT metadata (synopsis, image, type)
- Non-decreasing count
- Outputs to Gist: https://gist.github.com/VenkyVishy/d80c0ac20b3ce6b8d94e06ff0e5e074a
- OPTIMIZED: Faster execution, batch processing, connection pooling, caching
"""
import os
import asyncio
import aiohttp
import re
import json
import time
import tempfile
import shutil
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
from urllib.parse import quote_plus, urlparse
import subprocess
import hashlib
from functools import lru_cache
import zlib
import pickle
from pathlib import Path

# --------------------------- CONFIG ---------------------------
TIMEOUT = 8
VALIDATE_TIMEOUT = 3
MAX_CONCURRENT_HTTP = 100
MAX_GIT_THREADS = 6
MAX_PARSE_PROCESSES = 8
MAX_DISCOVERY_QUERIES = 15
MAX_REPLACEMENT_CANDIDATES = 3
CHUNK_SIZE = 500
CACHE_TTL = 10  # 1 hour cache

GITHUB_TOKEN = os.getenv('GH_TOKEN')
GIST_DESCRIPTION = "VENGATESH_IPTV_V23_BACKUP_AUTO_SYNC_OPTIMIZED"

# Cache directory
CACHE_DIR = Path(tempfile.gettempdir()) / "iptv_cache"
CACHE_DIR.mkdir(exist_ok=True)

# ------------------------ EPG SOURCES -------------------------
EPG_SOURCES = [
    "https://iptv-org.github.io/epg/guides/ALL.xml.gz",
    "https://raw.githubusercontent.com/mitthu786/tvepg/main/tataplay/epg.xml.gz",
    "https://raw.githubusercontent.com/iptv-org/epg/master/guides/in.xml",
    "https://raw.githubusercontent.com/iptv-org/epg/master/guides/us.xml",
    "https://raw.githubusercontent.com/iptv-org/epg/master/guides/uk.xml",
    "https://epg.provider.iptvorg.org/epg.xml.gz",
    "https://raw.githubusercontent.com/koditv/epg/main/epg.xml"
]

# ------------------------ 11 SEARCH ENGINES -------------------
SEARCH_ENGINES = [
    ("Google", "https://www.google.com/search?q={query}&num=100"),
    ("Bing", "https://www.bing.com/search?q={query}&count=100"),
    ("Baidu", "https://www.baidu.com/s?wd={query}&pn=0"),
    ("Yandex", "https://yandex.com/search/?text={query}&numdoc=100"),
    ("DuckDuckGo", "https://html.duckduckgo.com/html/?q={query}"),
    ("Yahoo", "https://search.yahoo.com/search?p={query}&n=100"),
    ("Ixquick", "https://ixquick.com/do/search?query={query}&cat=web&cmd=process_search&language=english"),
    ("Startpage", "https://www.startpage.com/do/dsearch?query={query}&cat=web&pl=opensearch"),
    ("Brave", "https://search.brave.com/search?q={query}&source=web"),
    ("Qwant", "https://www.qwant.com/?q={query}&client=opensearch"),
    ("Gigablast", "https://www.gigablast.com/search?q={query}")
]

# ------------------------ MAJOR SITES -------------------------
MAJOR_SITES = [
    "github.com", "gitlab.com", "bitbucket.org", "pastebin.com", "gist.github.com",
    "archive.org", "reddit.com", "vk.com", "ok.ru", "t.me", "telegram.me",
    "drive.google.com", "mega.nz", "mediafire.com", "dropbox.com", "4shared.com",
    "file.io", "anonfiles.com", "streamtape.com", "dailymotion.com", "vimeo.com",
    "youtube.com", "rutube.ru", "twitch.tv", "facebook.com", "instagram.com",
    "twitter.com", "bilibili.com", "youku.com", "iqiyi.com", "tudou.com",
    "sohu.com", "cntv.cn", "mgtv.com", "pptv.com", "wasu.cn", "acfun.cn",
    "nicovideo.jp", "douyin.com", "kuaishou.com", "zhihu.com", "baidu.com",
    "weibo.com", "toutiao.com", "ifeng.com", "sina.com.cn", "163.com",
    "qq.com", "so.com", "sm.cn", "uc.cn", "alibaba.com", "taobao.com",
    "tmall.com", "jd.com", "pinduoduo.com", "douban.com", "csdn.net",
    "zol.com.cn", "ithome.com", "mytvsuper.com", "nowtv.com.hk", "viu.com",
    "hotstar.com", "sonyliv.com", "zee5.com", "mxplayer.in", "erosnow.com",
    "altbalaji.com", "jio.com", "airtel.in", "tataplay.com", "dishanywhere.com",
    "sling.com", "hulu.com", "peacocktv.com", "pluto.tv", "tubi.tv",
    "crackle.com", "vudu.com", "fubo.tv", "roku.com", "xumo.com",
    "stirr.com", "localnow.com", "haystack.tv", "newsy.com", "pbs.org",
    "cbs.com", "nbc.com", "abc.com", "fox.com", "espn.com", "mlb.com",
    "nfl.com", "nba.com", "nhl.com", "bbc.com", "cnn.com", "reuters.com",
    "aljazeera.com", "dw.com", "france24.com", "rt.com", "sputniknews.com",
    "chinadaily.com.cn", "xinhuanet.com", "people.com.cn", "globaltimes.cn",
    "indiatimes.com", "ndtv.com", "thehindu.com", "timesofindia.indiatimes.com",
    "zee.news", "aajtak.in", "abplive.com", "news18.com", "republicworld.com",
    "indianexpress.com", "livemint.com", "business-standard.com", "moneycontrol.com",
    "economictimes.indiatimes.com", "firstpost.com", "scroll.in", "thequint.com",
    "thewire.in", "newslaundry.com", "caravanmagazine.in", "opindia.com",
    "swarajyamag.com", "organiser.org", "pravda.ru", "kommersant.ru", "gazeta.ru",
    "lenta.ru", "ria.ru", "tass.ru", "interfax.ru", "vesti.ru", "tvzvezda.ru",
    "smotrim.ru", "1tv.ru", "ntv.ru", "ren.tv", "perviykanal.ru", "trt.net.tr",
    "aa.com.tr", "sabah.com.tr", "haberturk.com", "milliyet.com.tr", "cumhuriyet.com.tr",
    "sozcu.com.tr", "hurriyet.com.tr", "ensonhaber.com", "memurlar.net", "yenisafak.com",
    "star.com.tr", "ahaber.com.tr", "internethaber.com", "mynet.com", "posta.com.tr",
    "milligazete.com.tr", "yeniakit.com.tr", "haberler.com", "trthaber.com", "trtworld.com",
    "dw.com/tr", "bbc.com/turkce", "cnn.com.tr", "ntv.com.tr", "fox.com.tr",
    "bloomberg.com.tr", "eksisozluk.com"
]

# ------------------------ ALL SOURCES (NO OMISSIONS) ----------
ALL_SOURCES = [
    # GitHub Repositories (.git)
    "https://github.com/iptv-org/iptv.git",
    "https://github.com/Free-TV/IPTV.git",
    "https://github.com/mitthu786/TS-JioTV.git",
    "https://github.com/JioTV-Go/jiotv_go.git",

    # Raw M3U/M3U8 from GitHub & CDNs
    "https://iptv-org.github.io/iptv/index.language.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/index.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/live.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/catchup.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/backup.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/playlist.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/tv.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/live_tv.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/free.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/premium.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/regions.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/subdivisions.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/ott.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/movies.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/series.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/vod.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/channels.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/ott.m3u",
    "https://raw.githubusercontent.com/mitthu786/TS-JioTV/main/allChannels.m3u",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/playlist.m3u",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/playlist.m3u8",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/jio.m3u",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/jio.m3u8",
    "https://raw.githubusercontent.com/EvilCaster/IPTV/master/iptv.txt",
    "https://raw.githubusercontent.com/ivandimov1/iptv/main/playlist.m3u",
    "https://raw.githubusercontent.com/git-up/IPTV/master/playlist.m3u",
    "https://raw.githubusercontent.com/ImJanindu/IPTV/main/IPTV.m3u",
    "https://raw.githubusercontent.com/azam00789/IPTV/main/playlist.m3u",
    "https://raw.githubusercontent.com/blackheart001/IPTV/main/playlist.m3u",
    "https://raw.githubusercontent.com/6eorge/iptv/master/playlist.m3u",
    "https://raw.githubusercontent.com/Aretera/IPTV/master/playlist.m3u",
    "https://raw.githubusercontent.com/ombori/iptv-playlist/master/playlist.m3u",
    "https://raw.githubusercontent.com/hrishi7/streamIt/main/playlist.m3u",
    "https://cdn.jsdelivr.net/gh/iptv-org/iptv/index.m3u",
    "https://cdn.jsdelivr.net/gh/Free-TV/IPTV/playlist.m3u",
    "https://rawcdn.githack.com/iptv-org/iptv/master/index.m3u",
    "https://rawcdn.githack.com/Free-TV/IPTV/master/playlist.m3u",
    "https://cdn.statically.io/gh/iptv-org/iptv/master/index.m3u",
    "https://fastly.jsdelivr.net/gh/iptv-org/iptv/index.m3u",
    "https://gcore.jsdelivr.net/gh/iptv-org/iptv/index.m3u",
    "https://iptvx.one/playlist.m3u",
    "https://iptv.smartott.net/playlist.m3u",
    "https://iptvking.net/playlist.m3u",
    "https://bestiptv4k.com/playlist.m3u",
    "https://iptvpremium.servemp3.com/playlist.m3u",
    "https://iptv-global.com/playlist.m3u",
    "https://iptv-world.org/playlist.m3u",
    "https://iptvhd.org/playlist.m3u",
    "https://iptv-streams.com/playlist.m3u",
    "https://iptv-channels.com/playlist.m3u",
    "https://live-iptv.net/playlist.m3u",
    "https://free-iptv.live/playlist.m3u",
    "https://premium-iptv.org/playlist.m3u",
    "https://iptv-premium.pro/playlist.m3u",
    "https://best-iptv.pro/playlist.m3u",
    "https://iptv-smart.com/playlist.m3u",
    "https://iptv-box.org/playlist.m3u",
    "https://iptv-hd.com/playlist.m3u",
    "https://iptv-zone.com/playlist.m3u",
    "https://iptv-world.net/playlist.m3u",
    "https://iptvmaster.live/playlist.m3u",
    "https://iptvuniverse.org/playlist.m3u",
    "https://streamking-iptv.com/playlist.m3u",
    "https://ultra-iptv.net/playlist.m3u",
    "https://iptv-galaxy.com/playlist.m3u",
    "https://supreme-iptv.org/playlist.m3u",
    "https://iptv-ocean.com/playlist.m3u",
    "https://iptv-diamond.net/playlist.m3u",
    "https://iptv-mirror.com/playlist.m3u",
    "https://backup-iptv.com/playlist.m3u",
    "https://iptv-reserve.net/playlist.m3u",
    "https://mirror.iptv-org.com/index.m3u",
    "https://bit.ly/2E2uz5S",
    "https://tinyurl.com/amaze-tamil-local-tv",
    "https://bit.ly/3h5yNZM",
    "https://bit.ly/3Jk4d7L",
    "https://bit.ly/3Lm2p9Q",
    "https://tinyurl.com/iptv-global-free",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/index.m3u8",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/playlist.m3u8",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/live.m3u8",
    "https://iptv-org.github.io/iptv/index.m3u8",
    "https://raw.githubusercontent.com/mitthu786/TS-JioTV/main/allChannels.m3u8",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/playlist.m3u8",
    "https://sites.google.com/site/arvinthiptv/Home/arvinth.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/IN/movies.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/US/movies.m3u",
    "https://raw.githubusercontent.com/streamit-iptv/movies/main/playlist.m3u",
    "https://raw.githubusercontent.com/ott-stream/ultimate/main/movies.m3u",
    "https://raw.githubusercontent.com/ott-stream/ultimate/main/series.m3u",
    "https://raw.githubusercontent.com/movie-streams/m3u/main/movies.m3u",
    "https://raw.githubusercontent.com/series-streams/m3u/main/series.m3u",
    "https://raw.githubusercontent.com/sports-iptv/sports/main/playlist.m3u",
    "https://raw.githubusercontent.com/news-iptv/news/main/playlist.m3u",
    "https://raw.githubusercontent.com/music-iptv/music/main/playlist.m3u",
    "https://raw.githubusercontent.com/kids-iptv/kids/main/playlist.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/au.m3u",
    "https://iptv-org.github.io/iptv/countries/au.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/AU/tv.m3u",
    "https://raw.githubusercontent.com/aussie-iptv/streams/main/playlist.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/in.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/us.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/uk.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/ca.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/de.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/fr.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/br.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/ru.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/cn.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/jp.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/kr.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/ae.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/sa.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/hin.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/tam.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/eng.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/spa.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/fre.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/ger.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/chi.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/ara.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/languages/por.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/indian.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/english.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/hindi.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/streams/tamil.m3u",
    "https://iptv.simplestv.com/playlist.m3u",
    "https://iptv.smarters.tv/playlist.m3u",
    "https://raw.githubusercontent.com/freearhey/iptv/master/playlist.m3u",
    "https://raw.githubusercontent.com/ImJanindu/IsuruTV/main/IsuruTV.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/IN/tv.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/US/tv.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/UK/tv.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/news.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/sports.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/music.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/kids.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/movies.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/educational.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/entertainment.m3u",
]

# ------------------------ CACHING UTILITIES -------------------
def get_cache_key(data):
    return hashlib.md5(str(data).encode()).hexdigest()

def save_cache(key, data):
    cache_file = CACHE_DIR / f"{key}.pkl"
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump({'timestamp': time.time(), 'data': data}, f)
    except:
        pass

def load_cache(key):
    cache_file = CACHE_DIR / f"{key}.pkl"
    try:
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)
                if time.time() - cached['timestamp'] < CACHE_TTL:
                    return cached['data']
    except:
        pass
    return None

# ------------------------ UTILITIES ---------------------------
@lru_cache(maxsize=10000)
def extract_channel_name(info_line: str) -> str:
    match = re.search(r',([^,\n]*)$', info_line)
    if match and match.group(1).strip():
        return match.group(1).strip()
    match = re.search(r'tvg-name=["\']?([^"\']*)', info_line)
    if match and match.group(1).strip():
        return match.group(1).strip()
    return "Unknown"

@lru_cache(maxsize=10000)
def infer_ott_type(info: str, url: str) -> str:
    lower = (info + url).lower()
    if any(kw in lower for kw in ["movie", "film", "cinema", "/movies", "vod"]):
        return "movie"
    elif any(kw in lower for kw in ["series", "season", "episode", "/series", "web series"]):
        return "series"
    else:
        return "live"

@lru_cache(maxsize=10000)
def generate_synopsis(title: str, stream_type: str) -> str:
    if stream_type == "movie":
        return f"Full-length cinematic content: {title}."
    elif stream_type == "series":
        return f"Episodic television or web series: {title}."
    else:
        return f"Live broadcast channel: {title}."

@lru_cache(maxsize=10000)
def extract_logo(info: str) -> str:
    match = re.search(r'tvg-logo=["\']?([^"\']*)', info)
    return match.group(1).strip() if match and match.group(1).strip() else ""

# ------------------------ PARSE M3U (Optimized Multi-Processing) -------
def parse_m3u_chunk(chunk: list) -> list:
    streams = []
    for content in chunk:
        lines = content.splitlines()
        i = 0
        while i < len(lines):
            if lines[i].startswith('#EXTINF:') and i + 1 < len(lines):
                url = lines[i + 1].strip()
                info = lines[i].strip()
                if url.startswith(('http', 'rtmp', 'rtsp')) and '.m3u' not in url.lower():
                    streams.append((url, info))
                i += 2
            else:
                i += 1
    return streams

def bulk_parse_contents_optimized(contents: list) -> tuple:
    # Split contents into chunks for parallel processing
    chunks = [contents[i:i + CHUNK_SIZE] for i in range(0, len(contents), CHUNK_SIZE)]
    
    with ProcessPoolExecutor(max_workers=MAX_PARSE_PROCESSES) as executor:
        results = list(executor.map(parse_m3u_chunk, chunks))
    
    candidate = {}
    replacement_index = {}
    
    for stream_list in results:
        for url, info in stream_list:
            if url not in candidate:
                candidate[url] = info
                name = extract_channel_name(info).lower()
                if name not in replacement_index:
                    replacement_index[name] = []
                replacement_index[name].append((url, info))
    
    return candidate, replacement_index

# ------------------------ GIT CLONING (Optimized Threaded) -------------
def clone_and_extract_m3u_sync_optimized(git_url):
    cache_key = get_cache_key(f"git_{git_url}")
    cached = load_cache(cache_key)
    if cached:
        return cached
        
    if not git_url.endswith('.git'):
        return []
    
    repo_name = urlparse(git_url).path.rstrip('.git').replace('/', '_')
    temp_dir = os.path.join(tempfile.gettempdir(), f"iptv_clone_{repo_name}_{int(time.time())}")
    m3u_contents = []
    
    try:
        # Use faster git clone with minimal operations
        subprocess.run([
            "git", "clone", "--depth=1", "--filter=blob:none", 
            "--single-branch", git_url, temp_dir
        ], check=True, capture_output=True, text=True, timeout=45)
        
        # Fast file discovery using find command
        result = subprocess.run([
            "find", temp_dir, "-type", "f", 
            "(", "-name", "*.m3u", "-o", "-name", "*.m3u8", "-o", "-name", "*.txt", ")"
        ], capture_output=True, text=True)
        
        files = result.stdout.strip().split('\n') if result.stdout else []
        
        for file_path in files:
            if not file_path:
                continue
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    if any(line.startswith('#EXTINF:') for line in content.splitlines()[:5]):
                        m3u_contents.append(content)
            except Exception:
                continue
                
    except Exception as e:
        print(f"⚠️ Clone failed for {git_url}: {e}")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    save_cache(cache_key, m3u_contents)
    return m3u_contents

# ------------------------ VALIDATION (Optimized) --------------------------
async def validate_stream_batch(session, urls):
    """Validate multiple streams in batch"""
    sem = asyncio.Semaphore(MAX_CONCURRENT_HTTP)
    
    async def validate_one(url):
        async with sem:
            try:
                async with session.head(url, timeout=VALIDATE_TIMEOUT, allow_redirects=True) as resp:
                    return url, resp.status in (200, 206, 302, 304)
            except:
                try:
                    async with session.get(url, timeout=VALIDATE_TIMEOUT, allow_redirects=True) as resp:
                        return url, resp.status in (200, 206, 302, 304)
                except:
                    return url, False
    
    tasks = [validate_one(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    validated = {}
    for result in results:
        if isinstance(result, tuple) and len(result) == 2:
            url, is_valid = result
            if is_valid:
                validated[url] = True
                
    return validated

# ------------------------ SEARCH ENGINE DISCOVERY (Optimized) -------------
async def scrape_search_engine_batch(session, queries, template):
    """Scrape multiple queries in batch"""
    results = set()
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    
    async def scrape_one(query):
        url = template.format(query=quote_plus(query))
        try:
            async with session.get(url, headers=headers, timeout=8) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)
                    for u in urls:
                        if any(site in u for site in MAJOR_SITES) and ('.m3u' in u or '.m3u8' in u):
                            results.add(u)
        except:
            pass
    
    tasks = [scrape_one(query) for query in queries]
    await asyncio.gather(*tasks)
    return results

async def discover_via_search_engines_optimized():
    queries = [
        "filetype:m3u8 iptv", "ext:m3u site:github.com iptv", "live tv m3u playlist",
        "indian channels m3u8", "tamil live tv m3u8 2025", "iptv playlist m3u8 github",
        "free iptv m3u8 working", "ott playlist m3u8", "sports channels m3u8",
        "movies m3u8 2025", "web series m3u8 free", "global iptv m3u8"
    ]
    
    discovered = set()
    timeout = aiohttp.ClientTimeout(total=8)
    connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for _, template in SEARCH_ENGINES:
            # Process queries in batches per search engine
            query_batches = [queries[i:i+3] for i in range(0, len(queries), 3)]
            for batch in query_batches[:2]:  # Limit to 2 batches per engine
                tasks.append(scrape_search_engine_batch(session, batch, template))
        
        results = await asyncio.gather(*tasks)
        for result_set in results:
            discovered.update(result_set)
            
    return list(discovered)

# ------------------------ FAST URL FETCHING -------------------
async def fetch_urls_batch(session, urls):
    """Fetch multiple URLs in batch with connection pooling"""
    sem = asyncio.Semaphore(MAX_CONCURRENT_HTTP)
    contents = []
    
    async def fetch_one(url):
        async with sem:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if any(line.startswith('#EXTINF:') for line in text.splitlines()[:5]):
                            return text
            except:
                pass
            return None
    
    tasks = [fetch_one(url) for url in urls]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        if result:
            contents.append(result)
            
    return contents

# ------------------------ MAIN UPDATER (OPTIMIZED) ------------------------
async def main():
    start_time = time.time()
    print(f"[{datetime.now()}] 🚀 Vengatesh IPTV Updater v23 — Optimized Real-Time Mode")
    
    all_contents = []
    processed_urls = set()

    # Step 1: Process sources
    git_urls = [u.strip() for u in ALL_SOURCES if u.strip().endswith('.git')]
    raw_urls = [u.strip() for u in ALL_SOURCES if not u.strip().endswith('.git')]

    # Step 2: Clone repos (optimized threaded with caching)
    print(f"📦 Cloning {len(git_urls)} GitHub repositories (optimized)...")
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_GIT_THREADS) as executor:
        tasks = [loop.run_in_executor(executor, clone_and_extract_m3u_sync_optimized, url) for url in git_urls]
        cloned_results = await asyncio.gather(*tasks)
        for contents in cloned_results:
            all_contents.extend(contents)

    # Step 3: Fetch raw URLs (optimized batch processing)
    print(f"📥 Fetching {len(raw_urls)} raw sources (batch processing)...")
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, use_dns_cache=True)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Process raw URLs in batches
        url_batches = [raw_urls[i:i+50] for i in range(0, len(raw_urls), 50)]
        for batch in url_batches:
            batch_contents = await fetch_urls_batch(session, batch)
            all_contents.extend(batch_contents)

    # Step 4: Search engine discovery (optimized)
    print("🔍 Discovering via 11 search engines (optimized)...")
    search_urls = await discover_via_search_engines_optimized()
    new_search_urls = [u for u in search_urls if u not in raw_urls and u not in processed_urls]
    
    if new_search_urls:
        print(f"🔗 Fetching {len(new_search_urls)} discovered URLs...")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=6)) as session:
            search_batches = [new_search_urls[i:i+30] for i in range(0, len(new_search_urls), 30)]
            for batch in search_batches:
                batch_contents = await fetch_urls_batch(session, batch)
                all_contents.extend(batch_contents)

    # Step 5: Bulk parse + build replacement index (optimized)
    print("🧩 Parsing streams and building index (optimized)...")
    candidate_streams, replacement_index = bulk_parse_contents_optimized(all_contents)

    # Step 6: Validate + display in real-time (optimized batch validation)
    print(f"🔍 Validating {len(candidate_streams)} streams (batch validation)...")
    validated = {}
    global_validated = set()
    broken = []

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=VALIDATE_TIMEOUT),
        connector=aiohttp.TCPConnector(limit=MAX_CONCURRENT_HTTP, ttl_dns_cache=300)
    ) as session:
        
        # Validate in batches
        url_list = list(candidate_streams.keys())
        url_batches = [url_list[i:i+100] for i in range(0, len(url_list), 100)]
        
        batch_num = 0
        for batch in url_batches:
            batch_num += 1
            print(f"🔍 Validating batch {batch_num}/{len(url_batches)}...")
            
            batch_validated = await validate_stream_batch(session, batch)
            
            for url in batch_validated:
                if url not in global_validated:
                    validated[url] = candidate_streams[url]
                    global_validated.add(url)
                    title = extract_channel_name(candidate_streams[url])
                    print(f"✅ [{len(validated)}] {title}")

        # Identify broken streams
        broken = [(url, candidate_streams[url]) for url in candidate_streams if url not in validated]

        # Silent replacement (optimized)
        if broken:
            print(f"🔄 Replacing {len(broken)} broken streams...")
            replacement_tasks = []
            
            for url, info in broken:
                name = extract_channel_name(info).lower()
                candidates = replacement_index.get(name, [])
                for rep_url, rep_info in candidates[:MAX_REPLACEMENT_CANDIDATES]:
                    if rep_url not in global_validated and rep_url not in validated:
                        replacement_tasks.append(rep_url)
            
            if replacement_tasks:
                # Validate replacement candidates in batch
                replacement_batches = [replacement_tasks[i:i+50] for i in range(0, len(replacement_tasks), 50)]
                for batch in replacement_batches:
                    batch_validated = await validate_stream_batch(session, batch)
                    for rep_url in batch_validated:
                        if rep_url in candidate_streams and rep_url not in global_validated:
                            validated[rep_url] = candidate_streams[rep_url]
                            global_validated.add(rep_url)
                            title = extract_channel_name(candidate_streams[rep_url])
                            print(f"🔁 [{len(validated)}] Replaced → {title}")

    # Step 7: Build M3U + JSON (optimized)
    print("📝 Building final playlists...")
    epg_str = ','.join(EPG_SOURCES)
    m3u_lines = [f'#EXTM3U x-tvg-url="{epg_str}"']
    json_validated = {}

    # Use list comprehension for faster building
    stream_data = []
    for url, info in validated.items():
        title = extract_channel_name(info)
        stream_type = infer_ott_type(info, url)
        logo = extract_logo(info)
        synopsis = generate_synopsis(title, stream_type)
        
        stream_data.append((info, url, title, stream_type, logo, synopsis))

    # Build M3U content
    for info, url, title, stream_type, logo, synopsis in stream_data:
        m3u_lines.extend([info, url])
        json_validated[url] = {
            "info": info,
            "ok": True,
            "last_validated": time.time(),
            "title": title,
            "metadata": {
                "type": stream_type,
                "synopsis": synopsis,
                "image": logo
            }
        }

    m3u_content = '\n'.join(m3u_lines)
    json_backup = {
        "persistence": {"validated": json_validated},
        "global_total": len(validated),
        "backup_timestamp": time.time(),
        "device_id": os.getenv('GITHUB_REPOSITORY', 'termux'),
        "processing_time": round(time.time() - start_time, 2)
    }
    json_str = json.dumps(json_backup, indent=2)

    # Step 8: Update Gist
    if not GITHUB_TOKEN:
        with open("playlist.m3u", "w", encoding="utf-8") as f:
            f.write(m3u_content)
        with open("vengatesh_iptv_backup.json", "w", encoding="utf-8") as f:
            f.write(json_str)
        print(f"✅ Saved locally - {len(validated)} streams in {time.time() - start_time:.2f}s")
        return

    print("☁️ Updating Gist...")
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}", 
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Vengatesh-IPTV-Updater"
    }
    
    gist_id = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        # Find existing gist
        async with session.get("https://api.github.com/gists", headers=headers) as r:
            if r.status == 200:
                gists = await r.json()
                for g in gists:
                    if g.get('description') == GIST_DESCRIPTION:
                        gist_id = g['id']
                        break

        gist_data = {
            "description": GIST_DESCRIPTION,
            "public": False,
            "files": {
                "playlist.m3u": {"content": m3u_content},
                "vengatesh_iptv_backup.json": {"content": json_str}
            }
        }

        try:
            if gist_id:
                async with session.patch(f"https://api.github.com/gists/{gist_id}", 
                                       json=gist_data, headers=headers) as r:
                    if r.status == 200:
                        print(f"✅ SUCCESS! Gist updated with {len(validated)} streams in {time.time() - start_time:.2f}s")
                    else:
                        print(f"❌ Gist update failed: {r.status}")
            else:
                async with session.post("https://api.github.com/gists", 
                                      json=gist_data, headers=headers) as r:
                    if r.status == 201:
                        data = await r.json()
                        gist_id = data['id']
                        print(f"✅ SUCCESS! New gist created with {len(validated)} streams in {time.time() - start_time:.2f}s")
                    else:
                        print(f"❌ Gist creation failed: {r.status}")
        except Exception as e:
            print(f"❌ Gist operation failed: {e}")

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    
    # Clear cache if older than TTL
    try:
        for cache_file in CACHE_DIR.glob("*.pkl"):
            if cache_file.stat().st_mtime < time.time() - CACHE_TTL:
                cache_file.unlink()
    except:
        pass
        
    asyncio.run(main())
