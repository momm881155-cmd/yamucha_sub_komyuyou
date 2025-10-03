# bot.py — 1日3回、Gofileリンク3つをコミュニティに投稿（通し番号は800から蓄積）
# ・コミュニティ投稿: /2/tweets に community_id を含めて OAuth1 直POST（非公開挙動）
# ・通常投稿        : community_id 未指定時は Tweepy v2 の通常ポスト
# ・収集: goxplorer.collect_fresh_gofile_urls() を deadline付きで呼ぶ
# ・環境変数で締め切り/ページ数を調整: SCRAPE_TIMEOUT_SEC（例:110）, NUM_PAGES（例:100）

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from dateutil import tz
import tweepy
import requests

try:
    from requests_oauthlib import OAuth1
except ImportError:
    OAuth1 = None

from goxplorer import collect_fresh_gofile_urls, is_gofile_alive

STATE_FILE = "state.json"
DAILY_LIMIT = 3
JST = tz.gettz("Asia/Tokyo")
TWEET_LIMIT = 280
TCO_URL_LEN = 23
GOFILE_RE = re.compile(r"https?://gofile\.io/d/[A-Za-z0-9]+", re.I)
ZWSP = "\u200B"
ZWNJ = "\u200C"
INVISIBLES = [ZWSP, ZWNJ]
HARD_LIMIT_SEC = 180  # 3分（保険）

def _default_state():
    return {
        "posted_urls": [],
        "last_post_date": None,
        "posts_today": 0,
        "recent_urls_24h": [],
        "line_seq": 800,
    }

def load_state():
    if not os.path.exists(STATE_FILE):
        return _default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = _default_state()
    for k, v in _default_state().items():
        if k not in data:
            data[k] = v
    return data

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def reset_if_new_day(state, now_jst):
    today_str = now_jst.date().isoformat()
    if state.get("last_post_date") != today_str:
        state["last_post_date"] = today_str
        state["posts_today"] = 0

def can_post_more_today(state):
    return state.get("posts_today", 0) < DAILY_LIMIT

def purge_recent_24h(state, now_utc: datetime):
    cutoff = now_utc - timedelta(hours=24)
    buf = []
    for item in state.get("recent_urls_24h", []):
        try:
            ts = datetime.fromisoformat(item.get("ts"))
        except Exception:
            continue
        if ts >= cutoff:
            buf.append(item)
    state["recent_urls_24h"] = buf

def normalize_url(u: str) -> str:
    if not u:
        return u
    u = u.strip()
    u = re.sub(r"^http://", "https://", u, flags=re.I)
    u = u.rstrip("/")
    return u

def build_seen_set_from_state(state) -> set:
    seen = set()
    for u in state.get("posted_urls", []):
        seen.add(normalize_url(u))
    for item in state.get("recent_urls_24h", []):
        seen.add(normalize_url(item.get("url")))
    return seen

def estimate_tweet_len_tco(text: str) -> int:
    def repl(m): return "U" * TCO_URL_LEN
    replaced = re.sub(r"https?://\S+", repl, text)
    return len(replaced)

def is_alive_retry(url: str, retries: int = 1, delay_sec: float = 0.5) -> bool:
    for _i in range(retries + 1):
        if is_gofile_alive(url):
            return True
        time.sleep(delay_sec)
    return False

def compose_fixed3_text(gofile_urls, start_seq: int, salt_idx: int = 0, add_sig: bool = True):
    invis = INVISIBLES[salt_idx % len(INVISIBLES)]
    lines = []
    seq = start_seq
    take = min(3, len(gofile_urls))
    sel = gofile_urls[:take]
    for u in sel:
        lines.append(f"{seq}{invis}. {u}")
        seq += 1
    text = "\n".join(lines)
    if add_sig:
        seed = (start_seq * 1315423911) ^ int(time.time() // 60)
        sig = "".join(INVISIBLES[(seed >> i) & 1] for i in range(16))
        text = text + sig
    return text, take

def get_client():
    return tweepy.Client(
        bearer_token=None,
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
        wait_on_rate_limit=True,
    )

def post_to_x_api(client, status_text: str):
    return client.create_tweet(text=status_text)

def _oauth1_session():
    if OAuth1 is None:
        raise RuntimeError("requests-oauthlib が必要です。requirements.txt に 'requests-oauthlib==1.3.1' を追加してください。")
    return OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
        signature_type='auth_header'
    )

def post_to_community_via_undocumented_api(status_text: str, community_id: str):
    url = "https://api.twitter.com/2/tweets"  # 環境によっては https://api.x.com/2/tweets でも可
    payload = {"text": status_text, "community_id": str(community_id)}
    sess = _oauth1_session()
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, headers=headers, data=json.dumps(payload), auth=sess, timeout=30)
    try:
        body = r.json()
    except Exception:
        body = r.text
    if not r.ok:
        raise RuntimeError(f"community post failed {r.status_code}: {body}")
    return body

def main():
    start_ts = time.monotonic()

    now_utc = datetime.now(timezone.utc)
    now_jst = now_utc.astimezone(JST)

    state = load_state()
    purge_recent_24h(state, now_utc)
    reset_if_new_day(state, now_jst)

    if not can_post_more_today(state):
        print("Daily limit reached; skip.")
        return

    already_seen = build_seen_set_from_state(state)

    # 収集の締め切り・ページ数（環境変数で調整可能）
    scrape_deadline_sec = int(os.getenv("SCRAPE_TIMEOUT_SEC", "110"))  # 例: 110秒
    num_pages = int(os.getenv("NUM_PAGES", "100"))                      # 例: 100ページ

    if time.monotonic() - start_ts > HARD_LIMIT_SEC:
        print("[warn] time budget exceeded before collection; abort.")
        return

    candidates = collect_fresh_gofile_urls(
        already_seen=already_seen,
        want=12,
        num_pages=num_pages,
        deadline_sec=scrape_deadline_sec,
    )
    print(f"[info] collected candidates: {len(candidates)}")
    if len(candidates) < 3:
        print("Not enough fresh URLs found; skip.")
        save_state(state)
        return

    # 直前チェックで3件
    target = 3
    tested = set()
    preflight = []

    def add_if_alive(u: str):
        if time.monotonic() - start_ts > HARD_LIMIT_SEC:
            return False
        n = normalize_url(u)
        if n in tested or n in already_seen or n in preflight:
            return False
        tested.add(n)
        if is_alive_retry(n, retries=1, delay_sec=0.5):
            preflight.append(n)
            return True
        return False

    for u in candidates:
        if len(preflight) >= target:
            break
        add_if_alive(u)

    if len(preflight) < target:
        print("Final preflight could not assemble 3 URLs; skip.")
        save_state(state)
        return

    # 本文生成（800→803…）
    start_seq = int(state.get("line_seq", 800))
    salt = (now_jst.hour + now_jst.minute) % len(INVISIBLES)
    status_text, _ = compose_fixed3_text(preflight, start_seq=start_seq, salt_idx=salt, add_sig=True)

    if estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.replace(". https://", ".https://")
    while estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.rstrip(ZWSP + ZWNJ)

    community_id = os.getenv("X_COMMUNITY_ID", "").strip()
    try:
        if community_id:
            resp = post_to_community_via_undocumented_api(status_text, community_id)
            tweet_id = resp.get("data", {}).get("id") if isinstance(resp, dict) else None
            print(f"[info] community posted id={tweet_id}")
        else:
            client = get_client()
            resp = post_to_x_api(client, status_text)
            tweet_id = resp.data.get("id") if resp and resp.data else None
            print(f"[info] tweeted id={tweet_id}")

        # ★ 投稿成功分だけ保存（ご指定どおり）
        for u in preflight[:3]:
            if u not in state["posted_urls"]:
                state["posted_urls"].append(u)
            state["recent_urls_24h"].append({"url": u, "ts": now_utc.isoformat()})
        state["posts_today"] = state.get("posts_today", 0) + 1
        state["line_seq"] = start_seq + 3
        save_state(state)
        print(f"Posted (3 gofiles):", status_text)
        return

    except Exception as e:
        print(f"[error] post failed: {e}")
        raise

if __name__ == "__main__":
    main()
