# goxplorer.py — gofilelab/newest を巡回し、gofile.io/d/... を収集
# 優先順:
# 1) サイトマップ (sitemap_index.xml / sitemap.xml) → 記事URL → 本文から抽出（XMLは正規表現で<loc>抽出）
# 2) WP REST API (wp-json/wp/v2/posts) → 本文HTMLから抽出
# 3) Playwright で /newest?page=N → 記事詳細へ遷移して抽出
#
# ポイント（この版）:
# - 死活判定は「死亡確定ワードが出たものだけ False」。
#   403/429/500/503、Cloudflare、タイムアウト等「不明」は True（=投稿可）。
# - クイック判定: 先頭 60 件だけ高速チェック／want 到達で即返す。
# - 期限（deadline_sec）を尊重しつつ、抽出ができている状況で“候補0”にならないよう調整。

import os
import re
import time
from html import unescape
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from typing import List, Set, Optional

import cloudscraper
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE_ORIGIN = "https://gofilelab.com"
BASE_LIST_URL = BASE_ORIGIN + "/newest?page={page}"
WP_POSTS_API  = BASE_ORIGIN + "/wp-json/wp/v2/posts?page={page}&per_page=20&_fields=link,content.rendered"
SITEMAP_INDEX = BASE_ORIGIN + "/sitemap_index.xml"

GOFILE_RE = re.compile(r"https?://gofile\.io/d/[A-Za-z0-9]+", re.I)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_ORIGIN + "/newest",
    "Connection": "keep-alive",
}

def _build_scraper():
    proxies = {}
    http_p = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_p = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if http_p:
        proxies["http"] = http_p
    if https_p:
        proxies["https"] = https_p

    s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    if proxies:
        s.proxies.update(proxies)
    s.headers.update(HEADERS)
    try:
        s.cookies.set("ageVerified", "1", domain="gofilelab.com", path="/")
        s.cookies.set("adult", "true", domain="gofilelab.com", path="/")
    except Exception:
        pass
    return s

def fix_scheme(url: str) -> str:
    if url.startswith("htps://"):
        return "https://" + url[len("htps://"):]
    return url

def _now() -> float:
    return time.monotonic()

def _deadline_passed(deadline_ts: Optional[float]) -> bool:
    return deadline_ts is not None and _now() >= deadline_ts

# -------- 中間リンク → gofile 解決 --------
def _resolve_to_gofile(url: str, scraper, timeout: int = 6) -> Optional[str]:
    if not url:
        return None
    url = fix_scheme(url)
    try:
        pr = urlparse(url)
        if pr.netloc.endswith("gofilelab.com"):
            qs = parse_qs(pr.query or "")
            for k in ("url", "u", "target"):
                if k in qs and qs[k]:
                    cand = unquote(qs[k][0])
                    m = GOFILE_RE.search(cand)
                    if m:
                        return fix_scheme(m.group(0))
    except Exception:
        pass
    try:
        r = scraper.get(url, timeout=timeout, allow_redirects=False)
        loc = r.headers.get("Location") or r.headers.get("location")
        if isinstance(loc, str):
            m = GOFILE_RE.search(loc)
            if m:
                return fix_scheme(m.group(0))
    except Exception:
        pass
    m = GOFILE_RE.search(url)
    if m:
        return fix_scheme(m.group(0))
    return None

# -------- HTML/本文から gofile 抽出 --------
def _extract_gofile_from_html(html: str, scraper) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    urls: List[str] = []
    seen = set()

    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if href:
            m = GOFILE_RE.search(href)
            go = fix_scheme(m.group(0)) if m else _resolve_to_gofile(href, scraper)
            if go and go not in seen:
                seen.add(go); urls.append(go)

        for attr in ("data-url", "data-clipboard-text", "data-href"):
            v = (a.get(attr) or "").strip()
            if not v:
                continue
            m2 = GOFILE_RE.search(v)
            if m2:
                go2 = fix_scheme(m2.group(0))
                if go2 and go2 not in seen:
                    seen.add(go2); urls.append(go2)

    for m in GOFILE_RE.findall(html or ""):
        u = fix_scheme(m.strip())
        if u and u not in seen:
            seen.add(u); urls.append(u)
    return urls

# -------- サイトマップ（XML→<loc>は正規表現で抽出） --------
_LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)

def _extract_locs_from_xml(xml_text: str) -> List[str]:
    if not xml_text:
        return []
    raw = _LOC_RE.findall(xml_text)
    locs = []
    for x in raw:
        u = unescape(x).replace("\n", "").replace("\r", "").replace("\t", "").strip()
        if u:
            locs.append(u)
    return locs

def _fetch_sitemap_post_urls(scraper, max_pages: int, deadline_ts: Optional[float]) -> List[str]:
    urls: List[str] = []
    def _get(url: str, timeout: int = 8) -> Optional[str]:
        try:
            r = scraper.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception:
            return None

    xml = _get(SITEMAP_INDEX) or _get(BASE_ORIGIN + "/sitemap.xml")
    if not xml:
        print("[warn] sitemap not available"); return urls

    locs = _extract_locs_from_xml(xml)
    if not locs:
        print("[warn] sitemap had no <loc>"); return urls

    post_sitemaps = [u for u in locs if "post" in u or "news" in u or "posts" in u] or locs

    collected = 0
    for sm in post_sitemaps:
        if _deadline_passed(deadline_ts):
            print("[info] sitemap deadline reached; stop."); break
        xml2 = _get(sm)
        if not xml2: continue
        entry_locs = _extract_locs_from_xml(xml2)
        for u in entry_locs:
            if not u.startswith(BASE_ORIGIN): continue
            urls.append(u); collected += 1
            if collected >= max_pages * 20: break
        if collected >= max_pages * 20: break

    print(f"[info] sitemap collected {len(urls)} post urls")
    return urls

def _collect_via_sitemap(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    post_urls = _fetch_sitemap_post_urls(s, max_pages=num_pages, deadline_ts=deadline_ts)
    if not post_urls: return []

    all_urls: List[str] = []
    seen: Set[str] = set()
    added_total = 0

    for i, post_url in enumerate(post_urls, 1):
        if _deadline_passed(deadline_ts):
            print(f("[info] sitemap deadline at post {i}; stop.")); break
        try:
            r = s.get(post_url, timeout=8)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            print(f"[warn] sitemap detail fetch failed: {post_url} ({e})"); continue
        urls = _extract_gofile_from_html(html, s)
        added = 0
        for u in urls:
            if u not in seen:
                seen.add(u); all_urls.append(u); added += 1
        added_total += added
        if i % 20 == 0:
            print(f"[info] sitemap detail {i} posts processed, got {added_total} gofiles (total {len(all_urls)})")
        time.sleep(0.1)
    return all_urls

# -------- WP API --------
def _collect_via_wp_api(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    all_urls: List[str] = []
    seen: Set[str] = set()

    for p in range(1, num_pages + 1):
        if _deadline_passed(deadline_ts):
            print(f"[info] wp-api deadline at page {p}; stop."); break
        api = WP_POSTS_API.format(page=p)
        try:
            r = s.get(api, timeout=8)
            ctype = r.headers.get("Content-Type", "")
            if "json" not in ctype: raise ValueError("non-json returned")
            arr = r.json()
        except Exception as e:
            print(f"[warn] wp-api page {p} failed: {e}"); break
        if not isinstance(arr, list) or not arr: break

        added = 0
        for item in arr:
            html = (item.get("content", {}) or {}).get("rendered", "") if isinstance(item, dict) else ""
            urls = _extract_gofile_from_html(html, s)
            for u in urls:
                if u not in seen:
                    seen.add(u); all_urls.append(u); added += 1
        print(f"[info] wp-api page {p}: gofiles {added} (total {len(all_urls)})")
        time.sleep(0.15)
    return all_urls

# -------- Playwright（AgeGate・一覧→詳細） --------
def _playwright_ctx(pw):
    browser = pw.chromium.launch(headless=True, args=[
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
    ])
    context = browser.new_context(user_agent=HEADERS["User-Agent"], locale="ja-JP")
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['ja-JP','ja'] });
    """)
    try:
        context.add_cookies([
            {"name": "ageVerified", "value": "1", "domain": "gofilelab.com", "path": "/"},
            {"name": "adult", "value": "true", "domain": "gofilelab.com", "path": "/"},
        ])
    except Exception:
        pass
    context.set_default_timeout(8000)
    return context

def _bypass_age_gate(page) -> None:
    js = """
    try {
      localStorage.setItem('ageVerified', '1');
      localStorage.setItem('adult', 'true');
      localStorage.setItem('age_verified', 'true');
      localStorage.setItem('age_verified_at', Date.now().toString());
    } catch (e) {}
    """
    page.evaluate(js); page.wait_for_timeout(120)

    checkbox_selectors = [
        "input[type='checkbox']",
        "label:has-text('18') >> input[type='checkbox']",
        "label:has-text('成人') >> input[type='checkbox']",
        "label:has-text('同意') >> input[type='checkbox']",
    ]
    button_selectors = [
        "text=同意して閲覧する",
        "text=同意して入場",
        "text=同意して閲覧",
        "text=同意する",
        "button:has-text('同意')",
        "text=I Agree",
        "button:has-text('I Agree')",
        "text=Enter",
        "button:has-text('Enter')",
    ]
    try:
        for sel in checkbox_selectors:
            cb = page.query_selector(sel)
            if cb and (bb := cb.bounding_box()) and bb.get("width",0)>0 and bb.get("height",0)>0:
                cb.click(force=True); page.wait_for_timeout(120); break
    except Exception:
        pass
    try:
        for sel in button_selectors:
            btn = page.query_selector(sel)
            if btn and (bb := btn.bounding_box()) and bb.get("width",0)>0 and bb.get("height",0)>0:
                btn.click(force=True); page.wait_for_timeout(200); break
    except Exception:
        pass
    try:
        page.reload(wait_until="domcontentloaded", timeout=18000); page.wait_for_timeout(180)
    except Exception:
        pass

def _get_html_pw(url: str, scroll_steps: int = 6, wait_ms: int = 600) -> str:
    with sync_playwright() as pw:
        context = _playwright_ctx(pw)
        page = context.new_page()
        page.set_extra_http_headers({
            "Accept": HEADERS["Accept"],
            "Accept-Language": HEADERS["Accept-Language"],
            "Referer": HEADERS["Referer"],
            "Connection": HEADERS["Connection"],
        })
        page.goto(BASE_ORIGIN, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(200); _bypass_age_gate(page)

        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(250); _bypass_age_gate(page)

        for _ in range(scroll_steps):
            page.mouse.wheel(0, 1500); page.wait_for_timeout(wait_ms)

        html = page.content()
        context.close()
        return html

def _extract_article_links_from_list(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    links: List[str] = []; seen = set()
    for sel in ["article a", ".entry-title a", "a[rel='bookmark']"]:
        for a in soup.select(sel):
            href = a.get("href"); 
            if not href: continue
            url = urljoin(BASE_ORIGIN, href.strip())
            if url not in seen:
                seen.add(url); links.append(url)
    if len(links) < 8:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#"): continue
            url = urljoin(BASE_ORIGIN, href); pr = urlparse(url)
            if pr.netloc and not pr.netloc.endswith("gofilelab.com"): continue
            bad = ("/newest", "/category/", "/tag/", "/page/", "/search", "/author", "/feed")
            if any(x in pr.path for x in bad): continue
            ext_bad = (".jpg", ".png", ".gif", ".webp", ".svg", ".css", ".js", ".zip", ".rar")
            if pr.path.endswith(ext_bad): continue
            if url not in seen:
                seen.add(url); links.append(url)
    return links[:50]

def _collect_via_playwright(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    all_urls: List[str] = []; seen_urls: Set[str] = set(); seen_posts: Set[str] = set()
    for p in range(1, num_pages + 1):
        if _deadline_passed(deadline_ts):
            print(f"[info] pw deadline at list page {p}; stop."); break
        list_url = BASE_LIST_URL.format(page=p)
        try:
            lhtml = _get_html_pw(list_url, scroll_steps=6, wait_ms=600)
        except Exception as e:
            print(f"[warn] playwright list {p} failed: {e}"); lhtml = ""
        article_urls = _extract_article_links_from_list(lhtml) if lhtml else []
        print(f"[info] page {p}: found {len(article_urls)} article links")

        added = 0
        for post_url in article_urls:
            if _deadline_passed(deadline_ts): break
            if post_url in seen_posts: continue
            seen_posts.add(post_url)
            try:
                dhtml = _get_html_pw(post_url, scroll_steps=3, wait_ms=500)
            except Exception as e:
                print(f"[warn] playwright detail failed: {post_url} ({e})"); dhtml = ""
            urls = _extract_gofile_from_html(dhtml, s) if dhtml else []
            for u in urls:
                if u not in seen_urls:
                    seen_urls.add(u); all_urls.append(u); added += 1
            time.sleep(0.2)
        print(f"[info] page {p}: extracted {added} new urls (total {len(all_urls)})")
        time.sleep(0.3)
    return all_urls

# -------- 死活判定（“不明は True”、死亡確定語のみ False） --------
_DEATH_MARKERS = (
    "This content does not exist",
    "The content you are looking for could not be found",
    "has been automatically removed",
    "has been deleted by the owner",
)

def is_gofile_alive(url: str) -> bool:
    """
    - まず HEAD (timeout=0.8s)。404/410/451 は即 False。
    - それ以外は GET (timeout=1.5s) で先頭 4KB を見て、死亡確定語があれば False。
    - 403/429/500/503、Cloudflare、タイムアウト等の“不明”は True（死と断定できないため）。
    """
    url = fix_scheme(url)
    s = _build_scraper()
    try:
        r = s.head(url, timeout=0.8, allow_redirects=True)
        if r.status_code in (404, 410, 451):
            return False
        # その他のコードは「不明」扱い（次のGETへ）
    except Exception:
        # HEAD 失敗は不明 → 次の GET で判断
        pass

    try:
        r = s.get(url, timeout=1.5, allow_redirects=True, stream=True)
        chunk = r.raw.read(4096, decode_content=True) if hasattr(r, "raw") else (r.text or "")[:4096]
        text = chunk.decode(errors="ignore") if isinstance(chunk, (bytes, bytearray)) else str(chunk)
        tl = text.lower()
        for dm in _DEATH_MARKERS:
            if dm.lower() in tl:
                return False
        return True
    except Exception:
        # タイムアウト・ネットワークエラー等は “不明”→ True
        return True

# -------- メイン収集（先頭60件だけクイック判定／want到達で即返す） --------
def fetch_listing_pages(num_pages: int = 100, deadline_ts: Optional[float] = None) -> List[str]:
    urls = _collect_via_sitemap(num_pages=num_pages, deadline_ts=deadline_ts)
    if urls: return urls
    urls = _collect_via_wp_api(num_pages=num_pages, deadline_ts=deadline_ts)
    if urls: return urls
    return _collect_via_playwright(num_pages=num_pages, deadline_ts=deadline_ts)

def collect_fresh_gofile_urls(
    already_seen: Set[str], want: int = 3, num_pages: int = 100, deadline_sec: Optional[int] = None
) -> List[str]:
    deadline_ts = (_now() + deadline_sec) if deadline_sec else None
    raw = fetch_listing_pages(num_pages=num_pages, deadline_ts=deadline_ts)

    uniq: List[str] = []
    seen_now: Set[str] = set()

    candidates = [u for u in raw if u not in already_seen][:60]

    for url in candidates:
        if _deadline_passed(deadline_ts):
            print("[info] deadline reached during filtering; stop."); break
        if url in seen_now:
            continue
        if is_gofile_alive(url):
            uniq.append(url); seen_now.add(url)
            if len(uniq) >= want:
                break
    return uniq
