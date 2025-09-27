# bot.py — 1日3回、Gofileリンク3つのみをコミュニティに投稿
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from dateutil import tz
import tweepy
from playwright.sync_api import sync_playwright

from goxplorer import collect_fresh_gofile_urls, is_gofile_alive

# ===== 設定 =====
STATE_FILE = "state.json"
DAILY_LIMIT = 3                 # 1日3投稿
JST = tz.gettz("Asia/Tokyo")
TWEET_LIMIT = 280
TCO_URL_LEN = 23
GOFILE_RE = re.compile(r"https?://gofile\.io/d/[A-Za-z0-9]+", re.I)

# 不可視（重複回避の最小署名）
ZWSP = "\u200B"
ZWNJ = "\u200C"
INVISIBLES = [ZWSP, ZWNJ]

# 実行時間の上限（ウォッチドッグ）
HARD_LIMIT_SEC = 180  # 3分

# ===== state =====
def _default_state():
    return {
        "posted_urls": [],
        "last_post_date": None,
        "posts_today": 0,
        "recent_urls_24h": [],
        "line_seq": 1,
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

# ===== 正規化＆除外集合 =====
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

# ===== ユーティリティ =====
def estimate_tweet_len_tco(text: str) -> int:
    def repl(m): return "U" * TCO_URL_LEN
    replaced = re.sub(r"https?://\S+", repl, text)
    return len(replaced)

def is_alive_retry(url: str, retries: int = 1, delay_sec: float = 0.5) -> bool:
    for i in range(retries + 1):
        if is_gofile_alive(url):
            return True
        if i < retries:
            time.sleep(delay_sec)
    return False

# ===== ツイート本文（3件固定＋通し番号、Amazonリンク無し） =====
def compose_fixed3_text(gofile_urls, start_seq: int, salt_idx: int = 0, add_sig: bool = True):
    invis = INVISIBLES[salt_idx % len(INVISIBLES)]
    lines = []
    seq = start_seq
    take = min(3, len(gofile_urls))
    sel = gofile_urls[:take]
    for i, u in enumerate(sel):
        lines.append(f"{seq}{invis}. {u}")
        seq += 1
    text = "\n".join(lines)
    if add_sig:
        seed = (start_seq * 1315423911) ^ int(time.time() // 60)
        sig = "".join(INVISIBLES[(seed >> i) & 1] for i in range(16))
        text = text + sig
    return text, take

# ===== X API =====
def get_client():
    client = tweepy.Client(
        bearer_token=None,
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
        wait_on_rate_limit=True,
    )
    return client

def post_to_x_v2(client, status_text: str):
    community_id = os.getenv("X_COMMUNITY_ID", "").strip()
    share_flag = os.getenv("X_SHARE_WITH_FOLLOWERS", "false").lower() in ("1","true","yes")

    kwargs = {}
    if community_id:
        kwargs["community_id"] = community_id
        kwargs["share_with_followers"] = share_flag

    return client.create_tweet(text=status_text, **kwargs)

# ===== main =====
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

    # 既知重複
    already_seen = build_seen_set_from_state(state)

    client = get_client()

    # 収集（gofilehub 1〜100ページ）
    if time.monotonic() - start_ts > HARD_LIMIT_SEC:
        print("[warn] time budget exceeded before collection; abort.")
        return
    candidates = collect_fresh_gofile_urls(
        already_seen=already_seen,
        want=12,
        num_pages=100
    )
    print(f"[info] collected candidates: {len(candidates)}")
    if len(candidates) < 3:
        print("Not enough fresh URLs found; skip.")
        return

    # 死活チェックして3件確保
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

    # 本文生成
    start_seq = int(state.get("line_seq", 1))
    salt = (now_jst.hour + now_jst.minute) % len(INVISIBLES)
    status_text, _ = compose_fixed3_text(preflight, start_seq=start_seq, salt_idx=salt, add_sig=True)

    if estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.replace(". https://", ".https://")
    while estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.rstrip(ZWSP + ZWNJ)

    # 投稿
    for attempt in range(3):
        try:
            resp = post_to_x_v2(client, status_text)
            tweet_id = resp.data.get("id") if resp and resp.data else None
            print(f"[info] tweeted id={tweet_id}")

            # 状態更新
            for u in preflight[:3]:
                if u not in state["posted_urls"]:
                    state["posted_urls"].append(u)
                state["recent_urls_24h"].append({"url": u, "ts": now_utc.isoformat()})
            state["posts_today"] = state.get("posts_today", 0) + 1
            state["line_seq"] = start_seq + 3
            save_state(state)
            print(f"Posted (3 gofiles):", status_text)
            return

        except tweepy.Forbidden as e:
            print(f"[error] Forbidden: {e}")
            raise
        except Exception as e:
            print(f"[error] create_tweet failed: {e}")
            raise

if __name__ == "__main__":
    main()
