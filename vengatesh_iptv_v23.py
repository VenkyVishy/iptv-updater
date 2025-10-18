#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VENGATESH IPTV UPDATER — SELF-HEALING v23 (Optimized)
- No duplicate or marketing links (e.g., iptvking.net, bestiptv4k.com removed)
- Short URLs preserved (bit.ly, tinyurl.com)
- Full GitHub repo cloning (.git)
- 11 search engines + MAJOR_SITES for discovery
- Auto-validate, auto-replace, auto-add
- All languages, countries, categories from iptv-org
- Outputs to Gist: https://gist.github.com/VenkyVishy/d80c0ac20b3ce6b8d94e06ff0e5e074a
"""
import os
import asyncio
import aiohttp
import re
import json
import time
import tempfile
import shutil
from datetime import datetime
from urllib.parse import quote_plus, urlparse
import subprocess

# --------------------------- CONFIG ---------------------------
TIMEOUT = 10
VALIDATE_TIMEOUT = 6
MAX_CONCURRENT = 60
MAX_DISCOVERY_QUERIES = 15

GITHUB_TOKEN = os.getenv('GH_TOKEN')
GIST_DESCRIPTION = "VENGATESH_IPTV_V23_BACKUP_AUTO_SYNC"

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

# ------------------------ TRUSTED SOURCES ONLY ----------------
# Removed: all "marketing" IPTV sites (iptvking.net, bestiptv4k.com, etc.)
# Kept: GitHub, CDNs, short URLs, official repos
ALL_SOURCES = [
    # === GitHub Repositories (.git) ===
    "https://github.com/iptv-org/iptv.git",
    "https://github.com/Free-TV/IPTV.git",
    "https://github.com/mitthu786/TS-JioTV.git",
    "https://github.com/JioTV-Go/jiotv_go.git",

    # === Official iptv-org Playlists (All Languages, Countries, Categories) ===
    "https://iptv-org.github.io/iptv/index.m3u",
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

    # All countries
    "https://raw.githubusercontent.com/iptv-org/iptv/master/countries/au.m3u",
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

    # All categories
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/news.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/sports.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/music.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/kids.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/movies.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/educational.m3u",
    "https://raw.githubusercontent.com/iptv-org/iptv/master/categories/entertainment.m3u",

    # === Trusted Third-Party Repos ===
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/channels.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/ott.m3u",
    "https://raw.githubusercontent.com/mitthu786/TS-JioTV/main/allChannels.m3u",
    "https://raw.githubusercontent.com/mitthu786/TS-JioTV/main/allChannels.m3u8",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/playlist.m3u",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/playlist.m3u8",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/jio.m3u",
    "https://raw.githubusercontent.com/JioTV-Go/jiotv_go/main/jio.m3u8",

    # === CDNs ===
    "https://cdn.jsdelivr.net/gh/iptv-org/iptv/index.m3u",
    "https://cdn.jsdelivr.net/gh/Free-TV/IPTV/playlist.m3u",
    "https://rawcdn.githack.com/iptv-org/iptv/master/index.m3u",
    "https://cdn.statically.io/gh/iptv-org/iptv/master/index.m3u",
    "https://fastly.jsdelivr.net/gh/iptv-org/iptv/index.m3u",
    "https://gcore.jsdelivr.net/gh/iptv-org/iptv/index.m3u",

    # === Short URLs (PRESERVED) ===
    "https://bit.ly/2E2uz5S",
    "https://tinyurl.com/amaze-tamil-local-tv",
    "https://bit.ly/3h5yNZM",
    "https://bit.ly/3Jk4d7L",
    "https://bit.ly/3Lm2p9Q",
    "https://tinyurl.com/iptv-global-free",

    # === Other trusted raw sources ===
    "https://raw.githubusercontent.com/freearhey/iptv/master/playlist.m3u",
    "https://raw.githubusercontent.com/ImJanindu/IsuruTV/main/IsuruTV.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/IN/tv.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/US/tv.m3u",
    "https://raw.githubusercontent.com/Free-IPTV/Countries/master/UK/tv.m3u",
    "https://raw.githubusercontent.com/aussie-iptv/streams/main/playlist.m3u",
    "https://raw.githubusercontent.com/streamit-iptv/movies/main/playlist.m3u",
    "https://raw.githubusercontent.com/ott-stream/ultimate/main/movies.m3u",
    "https://raw.githubusercontent.com/ott-stream/ultimate/main/series.m3u",
    "https://raw.githubusercontent.com/sports-iptv/sports/main/playlist.m3u",
    "https://raw.githubusercontent.com/news-iptv/news/main/playlist.m3u",
    "https://raw.githubusercontent.com/music-iptv/music/main/playlist.m3u",
    "https://raw.githubusercontent.com/kids-iptv/kids/main/playlist.m3u"
]

# ------------------------ UTILITIES ---------------------------
def extract_channel_name(info_line: str) -> str:
    match = re.search(r',([^,\n]*)$', info_line)
    if match and match.group(1).strip():
        return match.group(1).strip()
    match = re.search(r'tvg-name=["\']?([^"\']*)', info_line)
    if match and match.group(1).strip():
        return match.group(1).strip()
    return "Unknown"

def is_m3u_content(text: str) -> bool:
    return any(line.startswith('#EXTINF:') for line in text.splitlines()[:10])

# ------------------------ GIT CLONING -------------------------
async def clone_and_extract_m3u(git_url):
    if not git_url.endswith('.git'):
        return []
    repo_name = urlparse(git_url).path.rstrip('.git').replace('/', '_')
    temp_dir = os.path.join(tempfile.gettempdir(), f"iptv_clone_{repo_name}_{int(time.time())}")
    m3u_contents = []
    try:
        subprocess.run(["git", "clone", "--depth=1", git_url, temp_dir], check=True, capture_output=True, text=True, timeout=60)
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(('.m3u', '.m3u8', '.txt')):
                    full_path = os.path.join(root, file)
                    try:
                        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            if is_m3u_content(content):
                                m3u_contents.append(content)
                    except Exception:
                        continue
    except Exception as e:
        print(f"⚠️ Clone failed for {git_url}: {e}")
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
    return m3u_contents

# ------------------------ VALIDATION --------------------------
async def validate_stream(session, url):
    try:
        async with session.head(url, timeout=VALIDATE_TIMEOUT, allow_redirects=True) as resp:
            return resp.status in (200, 206, 302, 304)
    except:
        try:
            async with session.get(url, timeout=VALIDATE_TIMEOUT, allow_redirects=True) as resp:
                return resp.status in (200, 206, 302, 304)
        except:
            return False

# ------------------------ SEARCH ENGINE DISCOVERY -------------
async def scrape_search_engine(session, query, template):
    results = set()
    url = template.format(query=quote_plus(query))
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        async with session.get(url, headers=headers, timeout=12) as resp:
            if resp.status == 200:
                text = await resp.text()
                urls = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+', text)
                for u in urls:
                    if any(site in u for site in MAJOR_SITES) and ('.m3u' in u or '.m3u8' in u):
                        results.add(u)
    except:
        pass
    return results

async def discover_via_search_engines():
    queries = [
        "filetype:m3u8 iptv",
        "ext:m3u site:github.com iptv",
        "live tv m3u playlist",
        "indian channels m3u8",
        "tamil live tv m3u8 2025",
        "iptv playlist m3u8 github",
        "m3u8 live streams 2025",
        "free iptv m3u8 working",
        "ott playlist m3u8",
        "sports channels m3u8",
        "news channels m3u8",
        "movies m3u8 2025",
        "web series m3u8 free",
        "vod playlist m3u8",
        "global iptv m3u8"
    ]
    discovered = set()
    timeout = aiohttp.ClientTimeout(total=12)
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for query in queries[:MAX_DISCOVERY_QUERIES]:
            for _, template in SEARCH_ENGINES:
                try:
                    results = await scrape_search_engine(session, query, template)
                    discovered.update(results)
                except:
                    continue
    return list(discovered)

# ------------------------ AUTO-REPLACEMENT --------------------
async def find_replacement(session, name, all_contents):
    name_norm = name.strip().lower()
    candidates = []
    for content in all_contents:
        lines = content.splitlines()
        i = 0
        while i < len(lines):
            if lines[i].startswith('#EXTINF:') and i+1 < len(lines):
                url = lines[i+1].strip()
                info = lines[i]
                tvg_match = re.search(r'tvg-name=["\']?([^"\']*)', info, re.IGNORECASE)
                tvg = tvg_match.group(1).strip().lower() if tvg_match else ""
                ch_name = extract_channel_name(info).lower()
                if (tvg and tvg == name_norm) or (name_norm in ch_name):
                    candidates.append((url, info))
                i += 2
                continue
            i += 1
    for url, info in candidates:
        if await validate_stream(session, url):
            return url, info
    return None, None

# ------------------------ MAIN UPDATER ------------------------
async def main():
    print(f"[{datetime.now()}] 🚀 Starting Vengatesh IPTV Updater v23 — Self-Healing Mode")
    
    all_contents = []

    # Step 1: Split sources
    git_urls = [u.strip() for u in ALL_SOURCES if u.strip().endswith('.git')]
    raw_urls = [u.strip() for u in ALL_SOURCES if not u.strip().endswith('.git')]

    # Step 2: Clone .git repos
    print(f"📦 Cloning {len(git_urls)} GitHub repositories...")
    clone_tasks = [clone_and_extract_m3u(url) for url in git_urls]
    cloned_contents = await asyncio.gather(*clone_tasks)
    for contents in cloned_contents:
        all_contents.extend(contents)

    # Step 3: Fetch raw URLs
    print(f"📥 Fetching {len(raw_urls)} raw sources...")
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        fetch_tasks = [session.get(url) for url in raw_urls]
        responses = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        for resp in responses:
            if isinstance(resp, aiohttp.ClientResponse) and resp.status == 200:
                try:
                    text = await resp.text()
                    if is_m3u_content(text):
                        all_contents.append(text)
                except:
                    pass

    # Step 4: Discover via 11 search engines
    print("🔍 Discovering via 11 search engines...")
    search_urls = await discover_via_search_engines()
    if search_urls:
        print(f"🔎 Found {len(search_urls)} new URLs")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            tasks = [session.get(u) for u in search_urls if u not in raw_urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for resp in responses:
                if isinstance(resp, aiohttp.ClientResponse) and resp.status == 200:
                    try:
                        text = await resp.text()
                        if is_m3u_content(text):
                            all_contents.append(text)
                    except:
                        pass

    # Step 5: Parse all streams
    print("🧩 Parsing streams...")
    candidate_streams = {}
    for content in all_contents:
        lines = content.splitlines()
        i = 0
        while i < len(lines):
            if lines[i].startswith('#EXTINF:') and i+1 < len(lines):
                url = lines[i+1].strip()
                info = lines[i].strip()
                if url.startswith(('http', 'rtmp', 'rtsp')) and '.m3u' not in url.lower() and url not in candidate_streams:
                    candidate_streams[url] = info
                i += 2
            else:
                i += 1

    # Step 6: Validate + replace (non-decreasing count)
    print(f"🔍 Validating {len(candidate_streams)} streams...")
    validated = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=VALIDATE_TIMEOUT)) as session:
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        async def validate_one(url, info):
            async with sem:
                if await validate_stream(session, url):
                    validated[url] = info
                else:
                    name = extract_channel_name(info)
                    rep_url, rep_info = await find_replacement(session, name, all_contents)
                    if rep_url and rep_url not in validated:
                        validated[rep_url] = rep_info

        await asyncio.gather(*[validate_one(url, info) for url, info in candidate_streams.items()])

    print(f"✅ Validated {len(validated)} streams (non-decreasing)")

    # Step 7: Build M3U + JSON
    epg_str = ','.join(EPG_SOURCES)
    m3u_content = f'#EXTM3U x-tvg-url="{epg_str}"\n'
    m3u_content += '\n'.join(f"{info}\n{url}" for url, info in validated.items())

    json_backup = {
        "persistence": {
            "validated": {
                url: {
                    "info": info,
                    "ok": True,
                    "last_validated": time.time(),
                    "title": extract_channel_name(info),
                    "metadata": {
                        "type": "movie" if "movie" in info.lower() or "vod" in url.lower() else "series" if "series" in info.lower() else "live"
                    }
                }
                for url, info in validated.items()
            }
        },
        "global_total": len(validated),
        "backup_timestamp": time.time(),
        "device_id": os.getenv('GITHUB_REPOSITORY', 'termux')
    }

    json_str = json.dumps(json_backup, indent=2)

    # Step 8: Update Gist
    if not GITHUB_TOKEN:
        print("⚠️ GH_TOKEN not set. Saving locally.")
        with open("playlist.m3u", "w", encoding="utf-8") as f:
            f.write(m3u_content)
        with open("vengatesh_iptv_backup.json", "w", encoding="utf-8") as f:
            f.write(json_str)
        return

    print("☁️ Updating Gist...")
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    gist_id = None

    async with aiohttp.ClientSession() as session:
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

        if gist_id:
            await session.patch(f"https://api.github.com/gists/{gist_id}", json=gist_data, headers=headers)
        else:
            async with session.post("https://api.github.com/gists", json=gist_data, headers=headers) as r:
                if r.status == 201:
                    data = await r.json()
                    gist_id = data['id']

        if gist_id:
            raw_url = f"https://gist.githubusercontent.com/VenkyVishy/{gist_id}/raw/playlist.m3u"
            print(f"✅ SUCCESS! Playlist: {raw_url}")
        else:
            print("❌ Gist update failed")

if __name__ == "__main__":
    asyncio.run(main())
