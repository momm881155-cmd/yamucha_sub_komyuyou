# goxplorer.py — gofilelab/newest スクレイパ（早期打ち切り＋超軽量死活判定）
# 目的:
# - まず 40 件だけ素早く収集（RAW_LIMIT）
# - 先頭 18 件だけ超軽量フィルタ（FILTER_LIMIT）→ 3 本集まったら即返す
# - 死活判定は 1 回だけ ≤0.5s GET、先頭 1.5KB に死亡確定文言があれば False。
#   タイムアウト/403/503 などは “死と断定不可” → True（=投稿候補OK）
#
# 環境変数（任意）:
#   RAW_LIMIT=40        収集時の上限（多すぎると締め切りに負ける）
#   FILTER_LIMIT=18     フィルタに回す最大件数
#
# 依存は requirements.txt のまま。

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
_LOC_RE    = re.compile(r"<loc>(.*?)</loc>", re.IGNORECASE | re.DOTALL)

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

RAW_LIMIT      = int(os.getenv("RAW_LIMIT", "40"))
FILTER_LIMIT   = int(os.getenv("FILTER_LIMIT", "18"))

def _build_scraper():
    proxies = {}
    http_p = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_p = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    if http_p:  proxies["http"]  = http_p
    if https_p: proxies["https"] = https_p

    s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})
    if proxies: s.proxies.update(proxies)
    s.headers.update(HEADERS)
    try:
        s.cookies.set("ageVerified", "1", domain="gofilelab.com", path="/")
        s.cookies.set("adult", "true",   domain="gofilelab.com", path="/")
    except Exception:
        pass
    return s

def fix_scheme(url: str) -> str:
    return ("https://" + url[len("htps://"):]) if url.startswith("htps://") else url

def _now() -> float: return time.monotonic()
def _deadline_passed(deadline_ts: Optional[float]) -> bool:
    return deadline_ts is not None and _now() >= deadline_ts

# ====== 死活判定（超軽量） ======
_DEATH_MARKERS = (
    "This content does not exist",
    "The content you are looking for could not be found",
    "has been automatically removed",
    "has been deleted by the owner",
)

def is_gofile_alive(url: str) -> bool:
    """
    1回だけ超短時間 GET (timeout=0.5s)。先頭 1.5KB で死亡確定文言があれば False。
    それ以外（タイムアウトやエラー）は True（=死と断定不可）。
    """
    url = fix_scheme(url)
    s = _build_scraper()
    try:
        r = s.get(url, timeout=0.5, allow_redirects=True, stream=True)
        if hasattr(r, "raw"):
            chunk = r.raw.read(1536, decode_content=True)
            data = chunk.decode(errors="ignore") if isinstance(chunk, (bytes, bytearray)) else str(chunk)
        else:
            data = (r.text or "")[:1536]
        tl = (data or "").lower()
        for dm in _DEATH_MARKERS:
            if dm.lower() in tl:
                return False
        return True
    except Exception:
        return True

# ====== 抽出ユーティリティ ======
def _resolve_to_gofile(url: str, scraper, timeout: int = 4) -> Optional[str]:
    if not url: return None
    url = fix_scheme(url)
    try:
        pr = urlparse(url)
        if pr.netloc.endswith("gofilelab.com"):
            qs = parse_qs(pr.query or "")
            for k in ("url", "u", "target"):
                if k in qs and qs[k]:
                    cand = unquote(qs[k][0])
                    m = GOFILE_RE.search(cand)
                    if m: return fix_scheme(m.group(0))
    except Exception:
        pass
    try:
        r = scraper.get(url, timeout=timeout, allow_redirects=False)
        loc = r.headers.get("Location") or r.headers.get("location")
        if isinstance(loc, str):
            m = GOFILE_RE.search(loc)
            if m: return fix_scheme(m.group(0))
    except Exception:
        pass
    m = GOFILE_RE.search(url)
    return fix_scheme(m.group(0)) if m else None

def _extract_gofile_from_html(html: str, scraper) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    urls, seen = [], set()

    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        if href:
            m = GOFILE_RE.search(href)
            go = fix_scheme(m.group(0)) if m else _resolve_to_gofile(href, scraper)
            if go and go not in seen:
                seen.add(go); urls.append(go)

        for attr in ("data-url", "data-clipboard-text", "data-href"):
            v = (a.get(attr) or "").strip()
            if not v: continue
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

# ====== sitemap/wp-api（速攻で空ならスキップ） ======
def _extract_locs_from_xml(xml_text: str) -> List[str]:
    if not xml_text: return []
    raw = _LOC_RE.findall(xml_text)
    locs = []
    for x in raw:
        u = unescape(x).replace("\n","").replace("\r","").replace("\t","").strip()
        if u: locs.append(u)
    return locs

def _fetch_sitemap_post_urls(scraper, max_pages: int, deadline_ts: Optional[float]) -> List[str]:
    urls = []
    def _get(url: str, timeout: int = 8):
        try:
            r = scraper.get(url, timeout=timeout); r.raise_for_status(); return r.text
        except Exception:
            return None
    xml = _get(SITEMAP_INDEX) or _get(BASE_ORIGIN + "/sitemap.xml")
    if not xml:
        print("[warn] sitemap not available"); return urls
    locs = _extract_locs_from_xml(xml)
    if not locs:
        print("[warn] sitemap had no <loc>"); return urls

    post_sitemaps = [u for u in locs if "post" in u or "news" in u or "posts" in u] or locs
    cap = max_pages * 20
    for sm in post_sitemaps:
        if _deadline_passed(deadline_ts): print("[info] sitemap deadline reached; stop."); break
        xml2 = _get(sm)
        if not xml2: continue
        for u in _extract_locs_from_xml(xml2):
            if u.startswith(BASE_ORIGIN):
                urls.append(u)
                if len(urls) >= cap: break
        if len(urls) >= cap: break
    print(f"[info] sitemap collected {len(urls)} post urls")
    return urls

def _collect_via_sitemap(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    posts = _fetch_sitemap_post_urls(s, max_pages=num_pages, deadline_ts=deadline_ts)
    if not posts: return []
    all_urls, seen = [], set()
    for i, post_url in enumerate(posts, 1):
        if _deadline_passed(deadline_ts): print(f"[info] sitemap deadline at post {i}; stop."); break
        try:
            r = s.get(post_url, timeout=8); r.raise_for_status(); html = r.text
        except Exception as e:
            print(f"[warn] sitemap detail fetch failed: {post_url} ({e})"); continue
        for u in _extract_gofile_from_html(html, s):
            if u not in seen:
                seen.add(u); all_urls.append(u)
        if len(all_urls) >= RAW_LIMIT:  # ★ 早期打ち切り
            return all_urls[:RAW_LIMIT]
        time.sleep(0.08)
    return all_urls[:RAW_LIMIT]

def _collect_via_wp_api(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    all_urls, seen = [], set()
    for p in range(1, num_pages + 1):
        if _deadline_passed(deadline_ts): print(f"[info] wp-api deadline at page {p}; stop."); break
        api = WP_POSTS_API.format(page=p)
        try:
            r = s.get(api, timeout=8)
            if "json" not in (r.headers.get("Content-Type","")): raise ValueError("non-json returned")
            arr = r.json()
        except Exception as e:
            print(f"[warn] wp-api page {p} failed: {e}"); break
        if not isinstance(arr, list) or not arr: break
        for item in arr:
            html = (item.get("content", {}) or {}).get("rendered", "") if isinstance(item, dict) else ""
            for u in _extract_gofile_from_html(html, s):
                if u not in seen:
                    seen.add(u); all_urls.append(u)
        if len(all_urls) >= RAW_LIMIT:  # ★ 早期打ち切り
            return all_urls[:RAW_LIMIT]
        time.sleep(0.12)
    return all_urls[:RAW_LIMIT]

# ====== Playwright（待機強化＋直抽出フォールバック＋早期打ち切り） ======
def _playwright_ctx(pw):
    browser = pw.chromium.launch(headless=True, args=[
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
    ])
    ctx = browser.new_context(user_agent=HEADERS["User-Agent"], locale="ja-JP")
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['ja-JP','ja'] });
    """)
    try:
        ctx.add_cookies([
            {"name": "ageVerified", "value": "1", "domain": "gofilelab.com", "path": "/"},
            {"name": "adult", "value": "true", "domain": "gofilelab.com", "path": "/"},
        ])
    except Exception:
        pass
    ctx.set_default_timeout(10000)
    return ctx

def _bypass_age_gate(page):
    js = """
    try {
      localStorage.setItem('ageVerified','1');
      localStorage.setItem('adult','true');
      localStorage.setItem('age_verified','true');
      localStorage.setItem('age_verified_at', Date.now().toString());
    } catch(e){}
    """
    page.evaluate(js); page.wait_for_timeout(120)
    for sel in [
        "input[type='checkbox']",
        "label:has-text('18') >> input[type='checkbox']",
        "label:has-text('成人') >> input[type='checkbox']",
        "label:has-text('同意') >> input[type='checkbox']",
    ]:
        try:
            cb = page.query_selector(sel)
            if cb and (bb := cb.bounding_box()) and bb.get("width",0)>0 and bb.get("height",0)>0:
                cb.click(force=True); page.wait_for_timeout(90); break
        except Exception:
            pass
    for sel in [
        "text=同意して閲覧する","text=同意して入場","text=同意して閲覧",
        "text=同意する","button:has-text('同意')","text=I Agree","button:has-text('I Agree')",
        "text=Enter","button:has-text('Enter')",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and (bb := btn.bounding_box()) and bb.get("width",0)>0 and bb.get("height",0)>0:
                btn.click(force=True); page.wait_for_timeout(140); break
        except Exception:
            pass
    try:
        page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass

def _get_html_pw(url: str, scroll_steps: int = 6, wait_ms: int = 600) -> str:
    with sync_playwright() as pw:
        ctx = _playwright_ctx(pw)
        page = ctx.new_page()
        page.set_extra_http_headers({
            "Accept": HEADERS["Accept"],
            "Accept-Language": HEADERS["Accept-Language"],
            "Referer": HEADERS["Referer"],
            "Connection": HEADERS["Connection"],
        })

        page.goto(BASE_ORIGIN, wait_until="domcontentloaded", timeout=20000)
        _bypass_age_gate(page)

        page.goto(url, wait_until="domcontentloaded", timeout=22000)
        _bypass_age_gate(page)

        for _ in range(scroll_steps):
            page.mouse.wheel(0, 1500); page.wait_for_timeout(wait_ms)

        html = page.content()
        ctx.close()
        return html

def _extract_article_links_from_list(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    links, seen = [], set()

    # 代表セレクタ
    for sel in ["article a", ".entry-title a", "a[rel='bookmark']"]:
        for a in soup.select(sel):
            href = a.get("href")
            if not href: continue
            url = urljoin(BASE_ORIGIN, href.strip())
            if url not in seen:
                seen.add(url); links.append(url)

    # セーフティ: 内部リンクでノイズ除外
    if len(links) < 12:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#"): continue
            url = urljoin(BASE_ORIGIN, href)
            pr = urlparse(url)
            if pr.netloc and not pr.netloc.endswith("gofilelab.com"): continue
            bad = ("/newest","/category/","/tag/","/page/","/search","/author","/feed","/privacy","/contact")
            if any(x in pr.path for x in bad): continue
            if pr.path.endswith((".jpg",".png",".gif",".webp",".svg",".css",".js",".zip",".rar",".pdf",".xml")): continue
            if url not in seen:
                seen.add(url); links.append(url)

    return links[:50]

def _collect_via_playwright(num_pages: int, deadline_ts: Optional[float]) -> List[str]:
    s = _build_scraper()
    all_urls, seen_urls, seen_posts = [], set(), set()

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

        # 詳細に入る（少なくても 1 ページでそこそこ拾えるので、早期に RAW_LIMIT 到達させる）
        added = 0
        for post_url in article_urls:
            if _deadline_passed(deadline_ts): break
            if post_url in seen_posts: continue
            seen_posts.add(post_url)
            try:
                dhtml = _get_html_pw(post_url, scroll_steps=3, wait_ms=520)
            except Exception as e:
                print(f"[warn] playwright detail failed: {post_url} ({e})"); dhtml = ""
            urls = _extract_gofile_from_html(dhtml, s) if dhtml else []
            for u in urls:
                if u not in seen_urls:
                    seen_urls.add(u); all_urls.append(u); added += 1
                    if len(all_urls) >= RAW_LIMIT:  # ★ 早期打ち切り
                        print(f"[info] early stop: reached RAW_LIMIT={RAW_LIMIT} (total {len(all_urls)})")
                        return all_urls[:RAW_LIMIT]
            time.sleep(0.12)

        print(f"[info] page {p}: extracted {added} new urls (total {len(all_urls)})")
        time.sleep(0.2)

    return all_urls[:RAW_LIMIT]

# ====== エントリーポイント ======
def fetch_listing_pages(num_pages: int = 100, deadline_ts: Optional[float] = None) -> List[str]:
    # まずは sitemap / wp-api（取れれば速い）
    urls = _collect_via_sitemap(num_pages=num_pages, deadline_ts=deadline_ts)
    if urls: return urls[:RAW_LIMIT]
    urls = _collect_via_wp_api(num_pages=num_pages, deadline_ts=deadline_ts)
    if urls: return urls[:RAW_LIMIT]
    # 最後に Playwright
    return _collect_via_playwright(num_pages=num_pages, deadline_ts=deadline_ts)

def collect_fresh_gofile_urls(
    already_seen: Set[str], want: int = 3, num_pages: int = 100, deadline_sec: Optional[int] = None
) -> List[str]:
    deadline_ts = (_now() + deadline_sec) if deadline_sec else None
    raw = fetch_listing_pages(num_pages=num_pages, deadline_ts=deadline_ts)

    # 先頭 FILTER_LIMIT 件だけ超軽量判定。want 到達で即返す。
    candidates = [u for u in raw if u not in already_seen][:max(1, FILTER_LIMIT)]
    uniq, seen_now = [], set()

    for url in candidates:
        if _deadline_passed(deadline_ts):
            print("[info] deadline reached during filtering; stop."); break
        if url in seen_now: continue
        if is_gofile_alive(url):
            uniq.append(url); seen_now.add(url)
            if len(uniq) >= want:
                break
    return uniq
