"""
MeraDownload4K — Telegram Downloader Bot
Platforms: YouTube · Instagram · Facebook · Snapchat
"""

import glob
import http.cookiejar
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import requests
import yt_dlp
from flask import Flask

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bot")

# ---------------------------------------------------------------------------
# Config  —  all values come from environment variables, zero hardcoded secrets
# ---------------------------------------------------------------------------

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is not set.\n"
        "Replit: add it under Secrets.\n"
        "Railway: add it under Variables in your project settings."
    )

# ADMIN_ID is optional — numeric Telegram user ID.
# Set it to receive a startup ping and enable future admin commands.
# Find yours by messaging the bot with /getid after first run.
_raw_admin = os.environ.get("ADMIN_ID", "").strip()
try:
    ADMIN_ID: Optional[int] = int(_raw_admin) if _raw_admin else None
except ValueError:
    ADMIN_ID = None

API_URL  = f"https://api.telegram.org/bot{BOT_TOKEN}"
PORT     = int(os.environ.get("PORT", 8099))
BOT_NAME = ""

DOWNLOAD_DOMAINS = [
    "youtube.com", "youtu.be",
    "instagram.com",
    "facebook.com", "fb.watch", "fb.com",
    "snapchat.com", "snap.com",
]
YOUTUBE_DOMAINS   = {"youtube.com", "youtu.be"}
INSTAGRAM_DOMAINS = {"instagram.com"}
FACEBOOK_DOMAINS  = {"facebook.com", "fb.watch", "fb.com"}
SNAPCHAT_DOMAINS  = {"snapchat.com", "snap.com"}

FFMPEG      = shutil.which("ffmpeg") or ""
FFPROBE     = shutil.which("ffprobe") or ""
COOKIES     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
HAS_COOKIES = os.path.isfile(COOKIES) and os.path.getsize(COOKIES) > 100

MAX_WORKERS      = 4
CLEANUP_INTERVAL = 1800
MAX_FILE_AGE     = 3600

_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
]
_ua_idx  = 0
_ua_lock = threading.Lock()


def _ua() -> str:
    global _ua_idx
    with _ua_lock:
        ua = _UAS[_ua_idx % len(_UAS)]
        _ua_idx += 1
        return ua


# ---------------------------------------------------------------------------
# URL classifiers
# ---------------------------------------------------------------------------

def is_yt(u):        return any(d in u for d in YOUTUBE_DOMAINS)
def is_ig(u):        return any(d in u for d in INSTAGRAM_DOMAINS)
def is_fb(u):        return any(d in u for d in FACEBOOK_DOMAINS)
def is_sc(u):        return any(d in u for d in SNAPCHAT_DOMAINS)
def is_supported(t): return any(d in t for d in DOWNLOAD_DOMAINS)

# ---------------------------------------------------------------------------
# Active download tracking
# ---------------------------------------------------------------------------

executor     = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="dl")
_active      = {}
_active_lock = threading.Lock()
_picks       = {}
_picks_lock  = threading.Lock()


def _lock_user(uid: int) -> bool:
    with _active_lock:
        if _active.get(uid):
            return False
        _active[uid] = True
        return True


def _unlock_user(uid: int):
    with _active_lock:
        _active.pop(uid, None)


# ---------------------------------------------------------------------------
# Telegram API helpers
# ---------------------------------------------------------------------------

def _post(method: str, **kwargs) -> dict:
    try:
        r = requests.post(f"{API_URL}/{method}", timeout=120, **kwargs)
        return r.json()
    except Exception as e:
        log.error("POST %s: %s", method, e)
        return {}


def _get(method: str, **kwargs) -> dict:
    try:
        r = requests.get(f"{API_URL}/{method}", timeout=35, **kwargs)
        return r.json()
    except Exception as e:
        log.error("GET %s: %s", method, e)
        return {}


def get_me() -> dict:
    try:
        return requests.get(f"{API_URL}/getMe", timeout=10).json().get("result", {})
    except Exception:
        return {}


def get_updates(offset=None) -> dict:
    params = {"timeout": 30, "allowed_updates": json.dumps(["message", "callback_query"])}
    if offset is not None:
        params["offset"] = offset
    return _get("getUpdates", params=params)


def send_msg(chat_id, text: str, markup=None) -> dict:
    p: dict = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "HTML"}
    if markup:
        p["reply_markup"] = json.dumps(markup)
    return _post("sendMessage", json=p)


def send_photo(chat_id, url: str, caption: str, markup=None) -> dict:
    p: dict = {"chat_id": chat_id, "photo": url,
                "caption": caption[:1024], "parse_mode": "HTML"}
    if markup:
        p["reply_markup"] = json.dumps(markup)
    return _post("sendPhoto", json=p)


def edit_text(chat_id, mid, text: str) -> dict:
    return _post("editMessageText", json={
        "chat_id": chat_id, "message_id": mid,
        "text": text[:4096], "parse_mode": "HTML",
    })


def edit_markup(chat_id, mid, markup=None) -> dict:
    return _post("editMessageReplyMarkup", json={
        "chat_id": chat_id, "message_id": mid,
        "reply_markup": json.dumps(markup or {}),
    })


def delete_msg(chat_id, mid) -> dict:
    return _post("deleteMessage", json={"chat_id": chat_id, "message_id": mid})


def answer_cb(cb_id: str, text: str = "") -> dict:
    return _post("answerCallbackQuery",
                 json={"callback_query_id": cb_id, "text": text})


def send_video(chat_id, path: str, caption: str = "") -> dict:
    with open(path, "rb") as f:
        return _post("sendVideo",
                     data={"chat_id": chat_id, "caption": caption[:1024],
                           "parse_mode": "HTML", "supports_streaming": "true"},
                     files={"video": f})


def send_audio(chat_id, path: str, caption: str = "") -> dict:
    with open(path, "rb") as f:
        return _post("sendAudio",
                     data={"chat_id": chat_id, "caption": caption[:1024],
                           "parse_mode": "HTML"},
                     files={"audio": f})


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _rm(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


def _find(prefix: str) -> Optional[str]:
    hits = [f for f in glob.glob(f"{prefix}*")
            if not f.endswith((".part", ".ytdl", ".temp"))]
    if not hits:
        return None
    for ext in (".mp4", ".mp3", ".m4a", ".webm", ".mkv", ".avi", ".mov"):
        m = [f for f in hits if f.endswith(ext)]
        if m:
            return m[0]
    return hits[0]


def _fmt_size(path: str) -> str:
    return f"{os.path.getsize(path)/1_048_576:.1f} MB"


def _fmt_dur(s) -> str:
    if not s:
        return ""
    s = int(s)
    h, m, sec = s // 3600, (s % 3600) // 60, s % 60
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


# ---------------------------------------------------------------------------
# Video fix — H264 + AAC + faststart (Telegram compatibility)
# ---------------------------------------------------------------------------

def _probe_codecs(path: str) -> tuple[str, str]:
    """
    Return (video_codec, audio_codec) using ffprobe.
    Falls back to ffmpeg stderr parse if ffprobe unavailable.
    """
    probe = FFPROBE or FFMPEG
    if not probe:
        return ("", "")
    try:
        if FFPROBE:
            r = subprocess.run(
                [FFPROBE, "-v", "quiet", "-show_streams",
                 "-print_format", "json", path],
                capture_output=True, text=True, timeout=15,
            )
            data    = json.loads(r.stdout or "{}")
            vcodec  = ""
            acodec  = ""
            for s in data.get("streams", []):
                if s.get("codec_type") == "video" and not vcodec:
                    vcodec = s.get("codec_name", "")
                if s.get("codec_type") == "audio" and not acodec:
                    acodec = s.get("codec_name", "")
            return vcodec, acodec
        else:
            r = subprocess.run([FFMPEG, "-i", path],
                               capture_output=True, text=True, timeout=15)
            stderr = r.stderr
            vc = re.search(r"Video:\s*(\w+)", stderr)
            ac = re.search(r"Audio:\s*(\w+)", stderr)
            return (vc.group(1) if vc else ""), (ac.group(1) if ac else "")
    except Exception:
        return "", ""


def _fix_for_telegram(src: str) -> str:
    """
    Guarantee the file is H264 + AAC inside an MP4 container with
    +faststart so Telegram can stream it without buffering.

    Strategy:
      1. Probe actual codecs.
      2. If already H264 + (AAC/mp3) + .mp4  →  stream-copy + faststart  (fast).
      3. Otherwise  →  full re-encode to H264/AAC  (slower, but guaranteed).
    Returns path to the fixed file (original deleted on success).
    """
    if not FFMPEG:
        return src

    vcodec, acodec = _probe_codecs(src)
    is_h264 = vcodec.lower() in ("h264", "avc", "avc1")
    is_aac  = acodec.lower() in ("aac", "mp3", "mp4a")
    is_mp4  = src.lower().endswith(".mp4")

    out = src.rsplit(".", 1)[0] + "_tg.mp4"

    if is_h264 and is_aac and is_mp4:
        # Fast path — stream copy, only add faststart
        cmd = [FFMPEG, "-i", src,
               "-c", "copy",
               "-movflags", "+faststart",
               "-y", out]
    else:
        # Full re-encode path — fixes green screens, codec mismatches, VP9/AV1
        log.info("Re-encoding %s (video=%s audio=%s) → H264/AAC", src, vcodec, acodec)
        cmd = [FFMPEG, "-i", src,
               "-c:v", "libx264", "-preset", "fast", "-crf", "23",
               "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart",
               "-y", out]

    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=600)
        if os.path.exists(out) and os.path.getsize(out) > 0:
            _rm(src)
            return out
    except subprocess.CalledProcessError as e:
        log.warning("ffmpeg fix failed: %s", e.stderr[-300:] if e.stderr else e)
    except Exception as e:
        log.warning("ffmpeg fix error: %s", e)

    return src   # Return original on failure


# ---------------------------------------------------------------------------
# yt-dlp format strings
# ---------------------------------------------------------------------------

def _fmt_str(quality: str) -> str:
    """
    Build a format string that strongly prefers H264 (AVC) + M4A streams.
    This prevents VP9/AV1 being selected, which causes green screens.
    Falls back through progressively looser requirements.
    """
    if quality == "mp3":
        return "bestaudio/best"

    h = {"360": 360, "480": 480, "720": 720}.get(quality, 720)
    return (
        # ── Tier 1: Native H264 + M4A  (zero re-encode, fastest) ──────────
        f"bestvideo[vcodec^=avc1][height<={h}][ext=mp4]+bestaudio[acodec^=mp4a][ext=m4a]"
        f"/bestvideo[vcodec^=avc][height<={h}][ext=mp4]+bestaudio[acodec^=mp4a][ext=m4a]"
        # ── Tier 2: H264 video + any audio ────────────────────────────────
        f"/bestvideo[vcodec^=avc1][height<={h}]+bestaudio"
        f"/bestvideo[vcodec^=avc][height<={h}]+bestaudio"
        # ── Tier 3: Any video + audio at height (will re-encode if VP9/AV1)
        f"/bestvideo[height<={h}]+bestaudio"
        # ── Tier 4: Pre-merged single-file fallbacks ───────────────────────
        f"/best[height<={h}][ext=mp4]"
        f"/best[height<={h}]"
        f"/best"
    )


def _base_opts(quality: str, use_cookies: bool = True) -> dict:
    audio_only = quality == "mp3"
    o: dict = {
        "format": _fmt_str(quality),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "http_headers": {"User-Agent": _ua()},
        "retries": 6,
        "fragment_retries": 10,
        "socket_timeout": 30,
        "prefer_ffmpeg": True,
        "merge_output_format": "mp4",
        "outtmpl": "",
    }
    if FFMPEG:
        o["ffmpeg_location"] = FFMPEG
    if use_cookies and HAS_COOKIES:
        o["cookiefile"] = COOKIES

    if audio_only:
        o["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]
    else:
        # Force H264+AAC when ffmpeg merges separate streams
        o["postprocessor_args"] = {
            "merger": [
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
            ],
        }

    return o


def _ytdlp_run(url: str, quality: str, extra: dict | None = None) -> tuple[str, str]:
    prefix = f"/tmp/dl_{int(time.time()*1000)}_"
    o = _base_opts(quality)
    o["outtmpl"] = f"{prefix}%(title).60s.%(ext)s"
    if extra:
        o.update(extra)
    with yt_dlp.YoutubeDL(o) as ydl:
        info  = ydl.extract_info(url, download=True)
        title = (info or {}).get("title", "")
    path = _find(prefix)
    if not path:
        raise ValueError("yt-dlp finished but no output file was found.")
    return path, title


def _ytdlp_meta(url: str) -> dict:
    o: dict = {
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "skip_download": True, "socket_timeout": 20,
        "http_headers": {"User-Agent": _ua()},
    }
    if FFMPEG:
        o["ffmpeg_location"] = FFMPEG
    if HAS_COOKIES:
        o["cookiefile"] = COOKIES
    with yt_dlp.YoutubeDL(o) as ydl:
        info = ydl.extract_info(url, download=False) or {}
    return {
        "title":    info.get("title", "Video"),
        "duration": info.get("duration"),
        "thumb":    info.get("thumbnail", ""),
        "channel":  info.get("uploader") or info.get("channel", ""),
    }


# ---------------------------------------------------------------------------
# Instagram-specific user-agents (rotated per attempt)
# ---------------------------------------------------------------------------

_IG_UAS = [
    # Android Chrome — best success rate for public Reels
    "Mozilla/5.0 (Linux; Android 13; SM-S908U) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    # iPhone Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1",
    # Desktop Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Instagram web-app ID — makes requests look like the official IG web client
_IG_APP_ID = "936619743392459"


def _ig_headers(ua: str) -> dict:
    """Return HTTP headers that bypass Instagram's bot detection."""
    return {
        "User-Agent":       ua,
        "Accept":           "*/*",
        "Accept-Language":  "en-US,en;q=0.9",
        "Accept-Encoding":  "gzip, deflate, br",
        "Origin":           "https://www.instagram.com",
        "Referer":          "https://www.instagram.com/",
        "X-IG-App-ID":      _IG_APP_ID,
        "X-ASBD-ID":        "129477",
        "Sec-Fetch-Dest":   "empty",
        "Sec-Fetch-Mode":   "cors",
        "Sec-Fetch-Site":   "same-site",
    }


# ---------------------------------------------------------------------------
# Platform downloaders
# ---------------------------------------------------------------------------

def _dl_youtube(url: str, quality: str) -> tuple[str, str]:
    """YouTube + Shorts — H264 preferred, re-encoded if needed."""
    path, title = _ytdlp_run(url, quality)
    if quality != "mp3":
        path = _fix_for_telegram(path)
    return path, title


def _dl_instagram(url: str, quality: str = "720") -> tuple[str, str]:
    """
    Instagram Reels / Posts — works without login for public content.

    Extraction chain (most-reliable-first):
      1. yt-dlp  ×3  with rotating mobile UAs + X-IG-App-ID header
      2. instaloader  (anonymous, mobile UA)
      3. og:video / JSON scrape  ×3 UAs  (last resort)

    cookies.txt is injected automatically when present (unlocks private content).
    """

    # ── Methods 1-3: yt-dlp with rotating user-agents ─────────────────────
    for attempt, ua in enumerate(_IG_UAS):
        try:
            prefix = f"/tmp/dl_ig_{int(time.time()*1000)}_"
            o = _base_opts(quality, use_cookies=False)
            o["outtmpl"]      = f"{prefix}%(title).60s.%(ext)s"
            o["http_headers"] = _ig_headers(ua)
            o["retries"]      = 4
            o["socket_timeout"] = 25
            # Tell yt-dlp to prefer the GraphQL API (works without login)
            o["extractor_args"] = {"instagram": {"api": ["graphql"]}}
            if HAS_COOKIES:
                o["cookiefile"] = COOKIES

            with yt_dlp.YoutubeDL(o) as ydl:
                info  = ydl.extract_info(url, download=True)
                title = (info or {}).get("title", "Instagram video")

            path = _find(prefix)
            if path:
                path = _fix_for_telegram(path)
                log.info("Instagram yt-dlp OK (UA #%d)", attempt + 1)
                return path, title

        except Exception as e:
            err = str(e)
            log.warning("Instagram yt-dlp attempt %d: %s", attempt + 1, err[:200])
            if attempt < len(_IG_UAS) - 1:
                time.sleep(2 + attempt * 2)   # 2s, 4s between retries

    # ── Method 4: instaloader (anonymous, no login required for public) ────
    m = re.search(r'/(?:p|reel|tv|reels)/([A-Za-z0-9_-]+)', url)
    if m:
        shortcode = m.group(1)
        stamp  = int(time.time())
        tmpdir = f"/tmp/ig_{stamp}"
        out    = f"/tmp/ig_{stamp}.mp4"
        os.makedirs(tmpdir, exist_ok=True)
        try:
            import instaloader as il
            L = il.Instaloader(
                download_videos=True,
                download_video_thumbnails=False,
                download_geotags=False,
                download_comments=False,
                save_metadata=False,
                compress_json=False,
                quiet=True,
                user_agent=_IG_UAS[0],
            )
            L.dirname_pattern  = tmpdir
            L.filename_pattern = "video"
            if HAS_COOKIES:
                jar = http.cookiejar.MozillaCookieJar(COOKIES)
                jar.load(ignore_discard=True, ignore_expires=True)
                L.context._session.cookies.update(jar)
            post  = il.Post.from_shortcode(L.context, shortcode)
            title = (post.caption or shortcode)[:80].split("\n")[0]
            L.download_post(post, target=tmpdir)
            found = next(
                (os.path.join(tmpdir, f)
                 for f in os.listdir(tmpdir) if f.endswith(".mp4")),
                None,
            )
            if found:
                os.rename(found, out)
                shutil.rmtree(tmpdir, ignore_errors=True)
                out = _fix_for_telegram(out)
                log.info("Instagram instaloader OK")
                return out, title
        except Exception as e:
            log.warning("Instagram instaloader: %s", e)
            shutil.rmtree(tmpdir, ignore_errors=True)

    # ── Method 5: og:video / JSON-LD scrape ──────────────────────────────
    for ua in _IG_UAS:
        hdrs = {
            "User-Agent":      ua,
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "X-IG-App-ID":     _IG_APP_ID,
        }
        try:
            html = requests.get(url, headers=hdrs, timeout=20).text
            for pat in [
                r'"video_url"\s*:\s*"([^"]+)"',
                r'<meta property="og:video" content="([^"]+)"',
                r'<meta property="og:video:url" content="([^"]+)"',
                r'"contentUrl"\s*:\s*"([^"]+\.mp4[^"]*)"',
            ]:
                hit = re.search(pat, html)
                if hit:
                    vurl = hit.group(1).replace("&amp;", "&").replace("\\/", "/")
                    out  = f"/tmp/ig_scrape_{int(time.time())}.mp4"
                    with requests.get(vurl, headers=hdrs, stream=True, timeout=90) as r:
                        r.raise_for_status()
                        with open(out, "wb") as fh:
                            for chunk in r.iter_content(65536):
                                fh.write(chunk)
                    if os.path.exists(out) and os.path.getsize(out) > 0:
                        out = _fix_for_telegram(out)
                        log.info("Instagram scrape OK")
                        return out, "Instagram video"
        except Exception as e:
            log.warning("Instagram scrape (UA #%d): %s", _IG_UAS.index(ua) + 1, e)

    raise ValueError(
        "Instagram download failed on all methods.\n"
        "• Make sure the post is <b>public</b>\n"
        "• For private posts: add a <code>cookies.txt</code> to the bot folder"
    )


def _dl_facebook(url: str, quality: str = "720") -> tuple[str, str]:
    """Facebook videos via yt-dlp with browser-like headers."""
    path, title = _ytdlp_run(url, quality, extra={
        "http_headers": {
            "User-Agent": _ua(),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.facebook.com/",
        }
    })
    path = _fix_for_telegram(path)
    return path, title


def _dl_snapchat(url: str) -> tuple[str, str]:
    """Snapchat Spotlight / Stories via yt-dlp (no cookies — Snapchat rejects them)."""
    path, title = _ytdlp_run(url, "720", extra={
        "extractor_args": {"snapchat": {"formats": ["video"]}},
        "cookiefile": None,
    })
    path = _fix_for_telegram(path)
    return path, title


# ---------------------------------------------------------------------------
# YouTube quality picker
# ---------------------------------------------------------------------------

QUALITY_KB = {
    "inline_keyboard": [
        [
            {"text": "📱 360p",  "callback_data": "dl_360"},
            {"text": "📺 480p",  "callback_data": "dl_480"},
            {"text": "🎬 720p",  "callback_data": "dl_720"},
        ],
        [
            {"text": "🎵 MP3 Audio", "callback_data": "dl_mp3"},
        ],
    ]
}


def _show_picker(chat_id: int, user_id: int, url: str):
    s   = send_msg(chat_id, "⏳ Fetching info…")
    sid = (s.get("result") or {}).get("message_id")
    try:
        m = _ytdlp_meta(url)
    except Exception as e:
        if sid:
            edit_text(chat_id, sid, f"❌ Could not fetch video info.\n<code>{e}</code>")
        return

    if sid:
        delete_msg(chat_id, sid)

    dur     = _fmt_dur(m["duration"])
    channel = m["channel"]
    caption = (
        f"🎬 <b>{m['title']}</b>\n"
        + (f"⏱ {dur}" if dur else "")
        + (f"  •  {channel}" if channel else "")
        + "\n\nChoose quality:"
    )

    sent = None
    if m["thumb"]:
        try:
            sent = send_photo(chat_id, m["thumb"], caption, markup=QUALITY_KB)
        except Exception:
            pass
    if not sent or not sent.get("ok"):
        sent = send_msg(chat_id, caption, markup=QUALITY_KB)

    mid = (sent.get("result") or {}).get("message_id")
    with _picks_lock:
        _picks[user_id] = {"url": url, "chat_id": chat_id, "mid": mid}


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def _worker(chat_id: int, user_id: int, url: str, quality: str,
            status_id: Optional[int]):
    path: Optional[str] = None
    try:
        if is_yt(url):
            path, title = _dl_youtube(url, quality)
        elif is_ig(url):
            path, title = _dl_instagram(url, quality)
        elif is_fb(url):
            path, title = _dl_facebook(url, quality)
        elif is_sc(url):
            path, title = _dl_snapchat(url)
        else:
            raise ValueError("Unsupported link.")

        size = _fmt_size(path)

        if status_id:
            edit_text(chat_id, status_id, "📤 Uploading…")

        is_audio = quality == "mp3"
        cap = f"<b>{title[:200]}</b>" + ("" if is_audio else f"  •  {size}")
        if is_audio:
            send_audio(chat_id, path, caption=cap)
        else:
            send_video(chat_id, path, caption=cap)

        if status_id:
            delete_msg(chat_id, status_id)

    except Exception as e:
        log.error("Download failed user=%d: %s", user_id, e)
        err = f"❌ <b>Failed</b>\n<code>{str(e)[:300]}</code>"
        if status_id:
            try:
                edit_text(chat_id, status_id, err)
            except Exception:
                send_msg(chat_id, err)
        else:
            send_msg(chat_id, err)
    finally:
        _rm(path)
        _unlock_user(user_id)


def _submit(chat_id: int, user_id: int, url: str, quality: str = "720",
            picker_mid: Optional[int] = None):
    if not _lock_user(user_id):
        send_msg(chat_id, "⏳ Your previous download is still in progress.")
        return

    with _picks_lock:
        _picks.pop(user_id, None)

    if picker_mid:
        edit_markup(chat_id, picker_mid)
        edit_text(chat_id, picker_mid, f"⬇️ Downloading {quality.upper()}…")
        status_id = picker_mid
    else:
        r = send_msg(chat_id, "⬇️ Downloading…")
        status_id = (r.get("result") or {}).get("message_id")

    executor.submit(_worker, chat_id, user_id, url, quality, status_id)


# ---------------------------------------------------------------------------
# Message & callback handlers
# ---------------------------------------------------------------------------

def _on_message(msg: dict):
    chat_id    = msg.get("chat", {}).get("id")
    user       = msg.get("from", {})
    user_id    = user.get("id")
    first_name = user.get("first_name", "")
    text       = (msg.get("text") or "").strip()

    if not chat_id or not user_id or not text:
        return

    cmd = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""

    if cmd in ("/start", "/help"):
        send_msg(chat_id,
            f"👋 Hi <b>{first_name or 'there'}</b>!\n\n"
            "Send me a link and I'll download it.\n\n"
            "📥 <b>Supported platforms</b>\n"
            "  • YouTube & Shorts\n"
            "  • Instagram Reels & Posts\n"
            "  • Facebook Videos\n"
            "  • Snapchat Spotlight\n\n"
            "⚙️ <b>Commands</b>\n"
            "  /audio &lt;link&gt; — extract MP3\n"
            "  /getid — show your Telegram numeric ID"
        )
        return

    if cmd == "/getid":
        send_msg(chat_id,
            f"🪪 <b>Your Telegram ID</b>\n\n"
            f"<code>{user_id}</code>\n\n"
            "Copy this number and set it as <code>ADMIN_ID</code> in your "
            "Railway / Replit environment variables."
        )
        return

    if cmd == "/audio":
        url = text[7:].strip()
        if not is_supported(url):
            send_msg(chat_id, "Usage: /audio &lt;link&gt;")
            return
        _submit(chat_id, user_id, url, quality="mp3")
        return

    if is_supported(text):
        if is_yt(text):
            threading.Thread(
                target=_show_picker, args=(chat_id, user_id, text), daemon=True
            ).start()
        else:
            _submit(chat_id, user_id, text)
        return

    send_msg(chat_id, "Send me a YouTube, Instagram, Facebook, or Snapchat link.")


def _on_callback(update: dict):
    cb      = update.get("callback_query", {})
    cb_id   = cb.get("id", "")
    data    = cb.get("data", "")
    user    = cb.get("from", {})
    user_id = user.get("id")
    chat_id = (cb.get("message") or {}).get("chat", {}).get("id")
    mid     = (cb.get("message") or {}).get("message_id")

    if not data.startswith("dl_"):
        answer_cb(cb_id)
        return

    quality = data[3:]
    answer_cb(cb_id, f"Starting {quality.upper()}…")

    with _picks_lock:
        pick = _picks.get(user_id)

    if not pick:
        edit_text(chat_id, mid, "⚠️ Expired. Send the link again.")
        return

    _submit(chat_id, user_id, pick["url"], quality=quality, picker_mid=mid)


# ---------------------------------------------------------------------------
# Flask keep-alive (Railway / UptimeRobot)
# ---------------------------------------------------------------------------

_flask = Flask(__name__)


@_flask.route("/")
def _root():
    return "OK", 200


@_flask.route("/health")
def _health():
    return {"status": "ok", "bot": BOT_NAME}, 200


def _run_flask():
    _flask.run(host="0.0.0.0", port=PORT, use_reloader=False)


# ---------------------------------------------------------------------------
# Background maintenance
# ---------------------------------------------------------------------------

def _cleanup():
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        removed = 0
        for pattern in ("/tmp/dl_*", "/tmp/ig_*"):
            for f in glob.glob(pattern):
                try:
                    if now - os.path.getmtime(f) > MAX_FILE_AGE:
                        if os.path.isdir(f):
                            shutil.rmtree(f, ignore_errors=True)
                        else:
                            os.remove(f)
                        removed += 1
                except OSError:
                    pass
        if removed:
            log.info("Cleaned %d temp files.", removed)


# ---------------------------------------------------------------------------
# Polling loop — auto-reconnect with exponential backoff
# ---------------------------------------------------------------------------

def _poll():
    offset  = None
    backoff = 2
    while True:
        try:
            updates = get_updates(offset=offset)
            backoff = 2   # reset on success
            for u in updates.get("result", []):
                offset = u["update_id"] + 1
                try:
                    if "message" in u:
                        _on_message(u["message"])
                    elif "callback_query" in u:
                        _on_callback(u)
                except Exception as e:
                    log.error("Handler: %s", e)
        except Exception as e:
            log.warning("Polling error: %s — retry in %ds", e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global BOT_NAME
    me       = get_me()
    BOT_NAME = me.get("username", "")
    log.info("@%s is online.", BOT_NAME)
    log.info("ffmpeg    : %s", FFMPEG or "NOT FOUND — videos may not work correctly")
    log.info("ADMIN_ID  : %s", ADMIN_ID if ADMIN_ID else "not set (optional)")
    log.info("cookies   : %s", "loaded" if HAS_COOKIES else "not present (public content only)")
    log.info("keep-alive: port %d  /health", PORT)

    # Ping admin on startup so you know the bot came online
    if ADMIN_ID:
        try:
            send_msg(ADMIN_ID,
                f"✅ <b>@{BOT_NAME} is online</b>\n"
                f"ffmpeg: {'✓' if FFMPEG else '✗ NOT FOUND'}\n"
                f"cookies: {'✓ loaded' if HAS_COOKIES else '✗ not present'}"
            )
        except Exception:
            pass

    threading.Thread(target=_run_flask, daemon=True).start()
    threading.Thread(target=_cleanup,   daemon=True).start()

    _poll()   # blocks forever; auto-reconnects on any error


if __name__ == "__main__":
    main()
