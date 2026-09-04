import threading
import queue
from flask import Flask, render_template, request, jsonify, g, send_from_directory
import glob
import sqlite3
import os
from datetime import datetime, timedelta
import time
import requests
import feedparser
import re
import random
import subprocess
import signal
import json
from flask import redirect, url_for

# Configuration variables
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_NAME = os.path.join(BASE_DIR, 'focusfeed.db')
SESSION_FILE = os.path.join(BASE_DIR, 'session.json')
FAVORITES_FILE = os.path.join(BASE_DIR, 'favorites.json')
PINNED_FILE = os.path.join(BASE_DIR, 'pinned.json')
YTDLP_UPDATE_FILE = os.path.join(BASE_DIR, 'ytdlp_update.json')
YTDLP_UPDATE_INTERVAL_DAYS = 7
FEED_FETCH_DELAY_SECONDS = .03
GLOBAL_VIDEO_DISPLAY_LIMIT = 100
DEFAULT_DATE_LIMIT_DAYS = 60
WEEKDAY_COOLDOWN_DAYS = 3
WEEKDAY_DAILY_POOL_SIZE = 30
WEEKDAY_FAVORITES_TARGET = 10
WEEKEND_DAILY_POOL_SIZE = 120
WEEKEND_FAVORITES_TARGET = 20
MAX_VIDEOS_PER_CHANNEL_CANDIDATE = 1
WEEKDAY_WATCH_QUOTA = 10
WEEKEND_WATCH_QUOTA = 30
PINNED_MAX = 3
HTTP_PORT = 777

# Backfill (full back-catalog fetch) settings
BACKFILL_DELAY_SECONDS = .3  # politeness delay between per-video yt-dlp calls
YTDLP_TIMEOUT_SECONDS = 30

# Offline downloads settings
DOWNLOADS_DIR = "/mnt/files/.ffdl/"
DOWNLOADED_VIDEO_IDS = set()
DOWNLOADS_STORAGE_SIZE = "0 MB"
DOWNLOADED_VIDEO_SIZES = {}  # video_id -> size_bytes, captured in the same disk pass
DOWNLOADING_IDS = set()

# Download Queue & Concurrency Management
MAX_CONCURRENT_DOWNLOADS = 2
DOWNLOAD_QUEUE = queue.Queue()
ACTIVE_DOWNLOAD_PROCESSES = {}  # video_id -> subprocess.Popen

def download_worker():
    """Background worker that continuously consumes video_ids from DOWNLOAD_QUEUE."""
    while True:
        video_id = DOWNLOAD_QUEUE.get()
        try:
            download_video_task(video_id)
        except Exception as e:
            print(f" * [WORKER ERROR] {video_id}: {e}")
        finally:
            DOWNLOAD_QUEUE.task_done()

# Global backfill lock — only one channel can sync at a time, system-wide.
CURRENT_BACKFILL = {
    "channel_id": None,
    "channel_name": None,
    "current": 0,
    "total": 0,
    "status": "idle"  # idle | running | cancelling
}

def format_bytes(size_bytes):
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def sync_downloads_from_disk():
    """Scans DOWNLOADS_DIR (SOT) once, updates active ID set, per-video sizes,
    and total size — all in this single pass, so nothing else needs to touch
    the filesystem again to know a video's individual size (e.g. /storage)."""
    global DOWNLOADED_VIDEO_IDS, DOWNLOADS_STORAGE_SIZE, DOWNLOADED_VIDEO_SIZES
    if not os.path.exists(DOWNLOADS_DIR):
        os.makedirs(DOWNLOADS_DIR, mode=0o700, exist_ok=True)
    try:
        os.chmod(DOWNLOADS_DIR, 0o700)
    except Exception:
        pass

    video_ids = set()
    sizes = {}
    total_bytes = 0
    pattern = os.path.join(DOWNLOADS_DIR, "*.mp4")
    for filepath in glob.glob(pattern):
        filename = os.path.basename(filepath)
        vid = os.path.splitext(filename)[0]
        video_ids.add(vid)
        try:
            size = os.path.getsize(filepath)
            sizes[vid] = size
            total_bytes += size
        except OSError:
            pass

    DOWNLOADED_VIDEO_IDS = video_ids
    DOWNLOADED_VIDEO_SIZES = sizes
    DOWNLOADS_STORAGE_SIZE = format_bytes(total_bytes)
    print(f" * [STORAGE] Scanned {len(DOWNLOADED_VIDEO_IDS)} downloaded videos ({DOWNLOADS_STORAGE_SIZE}).")

def download_video_task(video_id):
    global DOWNLOADING_IDS, ACTIVE_DOWNLOAD_PROCESSES
    out_tmpl = os.path.join(DOWNLOADS_DIR, f"{video_id}.%(ext)s")
    cmd = [
        'yt-dlp',
        '--newline',
        '-f', 'bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best',
        '--merge-output-format', 'mp4',
        '-o', out_tmpl,
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    try:
        print(f"\n=======================================================")
        print(f" * [DOWNLOAD] Starting live fetch for {video_id}...")
        print(f"=======================================================")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        ACTIVE_DOWNLOAD_PROCESSES[video_id] = process

        for line in process.stdout:
            print(f"[yt-dlp:{video_id}] {line}", end='', flush=True)

        process.wait()

        if process.returncode == 0:
            print(f"\n * [DOWNLOAD] Finished downloading {video_id} successfully.")
        else:
            print(f"\n * [DOWNLOAD] Failed downloading {video_id} (exit code: {process.returncode}).")
    except Exception as e:
        print(f"\n * [DOWNLOAD] Error during {video_id}: {e}")
    finally:
        ACTIVE_DOWNLOAD_PROCESSES.pop(video_id, None)
        DOWNLOADING_IDS.discard(video_id)
        sync_downloads_from_disk()
        print(f"=======================================================\n")


app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True


# --- Database setup ---
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE_NAME)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channel (
                channel_id TEXT PRIMARY KEY,
                feed_url TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS video (
                video_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                thumbnail_url TEXT,
                published_at TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                is_hidden BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (channel_id) REFERENCES channel(channel_id) ON DELETE CASCADE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_update TEXT NOT NULL
            )
        ''')
        db.commit()

        # Initialize meta if empty
        cursor.execute("SELECT COUNT(*) FROM meta")
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO meta (last_update) VALUES (?)", (datetime.min.isoformat(),))
            db.commit()

        # --- Migration: add backfill_status column to channel if missing ---
        cursor.execute("PRAGMA table_info(channel)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if 'backfill_status' not in existing_columns:
            print(" * Migrating schema: adding channel.backfill_status")
            cursor.execute("ALTER TABLE channel ADD COLUMN backfill_status TEXT DEFAULT 'pending'")
            db.commit()

        # --- Migration: add last_shown_at column to video if missing ---
        cursor.execute("PRAGMA table_info(video)")
        video_columns = {row[1] for row in cursor.fetchall()}
        if 'last_shown_at' not in video_columns:
            print(" * Migrating schema: adding video.last_shown_at")
            cursor.execute("ALTER TABLE video ADD COLUMN last_shown_at TEXT DEFAULT NULL")
            db.commit()

# --- Feed Updating Logic ---
def fetch_and_store_feeds(force=False):
    with app.app_context():
        db = get_db()
        cursor = db.cursor()

        if not force:
            cursor.execute("SELECT last_update FROM meta ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                last_update_str = row['last_update']
                last_update_date = datetime.fromisoformat(last_update_str).date()
                if last_update_date == datetime.now().date():
                    # Even if skipping full fetch, still resolve any TEMP_ channels
                    cursor.execute("SELECT COUNT(*) FROM channel WHERE channel_id LIKE 'TEMP_%'")
                    if cursor.fetchone()[0] > 0:
                        print(" * Feeds already updated today, but found unresolved TEMP channels. Resolving...")
                        _resolve_temp_channels(db, cursor)
                    else:
                        print(" * Feeds already updated today. Skipping full fetch.")
                        print("")
                    return

        print(f" * Starting full feed update (force={force})...")
        print("")

        cursor.execute("SELECT channel_id, name, feed_url FROM channel")
        channels = cursor.fetchall()

        if not channels:
            print("No channels found in database to update.")
            return

        for i, channel_data in enumerate(channels):
            current_db_channel_id = channel_data['channel_id']
            channel_name = channel_data['name']
            feed_url = channel_data['feed_url']

            print(f"Fetching feed for '{channel_name}' ({i + 1} of {len(channels)})...")
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(feed_url, headers=headers, timeout=10)
                response.raise_for_status()

                feed = feedparser.parse(response.content)

                extracted_youtube_channel_id = None
                if hasattr(feed.feed, 'yt_channelid'):
                    extracted_youtube_channel_id = feed.feed.yt_channelid
                elif hasattr(feed.feed, 'id') and 'yt:channel:' in feed.feed.id:
                    extracted_youtube_channel_id = feed.feed.id.split('yt:channel:')[1]
                elif hasattr(feed.feed, 'link'):
                    match = re.search(r'(UC[\w-]{21}[AQgw])', feed.feed.link)
                    if match:
                        extracted_youtube_channel_id = match.group(1)

                if not extracted_youtube_channel_id:
                    print(f"WARNING: No channel ID for '{channel_name}'. Skipping.")
                    continue

                if extracted_youtube_channel_id != current_db_channel_id:
                    cursor.execute("SELECT feed_url FROM channel WHERE channel_id = ?", (extracted_youtube_channel_id,))
                    if cursor.fetchone():
                        print(f"Conflict: ID {extracted_youtube_channel_id} exists. Skipping.")
                        continue

                    if current_db_channel_id.startswith("TEMP_"):
                        print(f"Resolving TEMP ID for '{channel_name}' -> '{extracted_youtube_channel_id}'")
                        cursor.execute("DELETE FROM channel WHERE channel_id = ?", (current_db_channel_id,))
                        cursor.execute(
                            "INSERT INTO channel (channel_id, name, feed_url) VALUES (?, ?, ?)",
                            (extracted_youtube_channel_id, channel_name, feed_url)
                        )
                        db.commit()

                channel_id_for_videos = extracted_youtube_channel_id
                collected_for_channel = 0

                for entry in feed.entries:
                    video_id_match = None
                    if hasattr(entry, 'yt_videoid'):
                        video_id_match = entry.yt_videoid
                    elif hasattr(entry, 'link') and 'v=' in entry.link:
                        video_id_match = entry.link.split('v=')[1].split('&')[0]

                    if not video_id_match or (hasattr(entry, 'link') and "/shorts/" in entry.link):
                        continue

                    cursor.execute("SELECT video_id FROM video WHERE video_id = ?", (video_id_match,))
                    if cursor.fetchone():
                        continue

                    title = entry.title if hasattr(entry, 'title') else 'No Title'
                    thumbnail_url = entry.media_thumbnail[0]['url'] if 'media_thumbnail' in entry else f"https://i.ytimg.com/vi/{video_id_match}/hqdefault.jpg"

                    published_at = entry.published
                    try:
                        published_at_iso = datetime.fromisoformat(published_at.replace('Z', '+00:00')).isoformat()
                    except:
                        published_at_iso = datetime.now().isoformat()

                    cursor.execute(
                        "INSERT INTO video (video_id, title, thumbnail_url, published_at, channel_id, is_hidden) VALUES (?, ?, ?, ?, ?, ?)",
                        (video_id_match, title, thumbnail_url, published_at_iso, channel_id_for_videos, False)
                    )
                    collected_for_channel += 1

                db.commit()
                print(f"Finished '{channel_name}'. Added {collected_for_channel} videos.")

            except Exception as e:
                print(f"ERROR on '{channel_name}': {e}")

            time.sleep(FEED_FETCH_DELAY_SECONDS)

        cursor.execute("DELETE FROM meta")
        cursor.execute("INSERT INTO meta (last_update) VALUES (?)", (datetime.now().date().isoformat(),))
        db.commit()
        print("Full feed update completed.")


def _resolve_temp_channels(db, cursor):
    """Resolve only TEMP_ channels without doing a full fetch."""
    cursor.execute("SELECT channel_id, name, feed_url FROM channel WHERE channel_id LIKE 'TEMP_%'")
    temp_channels = cursor.fetchall()
    for ch in temp_channels:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(ch['feed_url'], headers=headers, timeout=10)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            extracted_id = None
            if hasattr(feed.feed, 'yt_channelid'):
                extracted_id = feed.feed.yt_channelid
            elif hasattr(feed.feed, 'id') and 'yt:channel:' in feed.feed.id:
                extracted_id = feed.feed.id.split('yt:channel:')[1]

            if extracted_id:
                cursor.execute("DELETE FROM channel WHERE channel_id = ?", (ch['channel_id'],))
                cursor.execute(
                    "INSERT OR IGNORE INTO channel (channel_id, name, feed_url) VALUES (?, ?, ?)",
                    (extracted_id, ch['name'], ch['feed_url'])
                )
                db.commit()
                print(f" * Resolved '{ch['name']}' -> {extracted_id}")
        except Exception as e:
            print(f" * Could not resolve TEMP for '{ch['name']}': {e}")
        time.sleep(FEED_FETCH_DELAY_SECONDS)


def backfill_channel(channel_id):
    """
    Full back-catalog fetch for a single channel using yt-dlp.
    Runs in a background thread. Uses its own sqlite connection
    (sqlite3 connections aren't safe to share across threads).

    Phase 1: flat-playlist listing (fast) -> get every video_id the channel has.
    Phase 2: for video_ids not already in our DB, slow --dump-json fetch for
             accurate title/date/thumbnail, inserted one at a time so the run
             is resumable if interrupted. Checks CURRENT_BACKFILL['status']
             between videos so it can be cancelled from any channel page.
    """
    global CURRENT_BACKFILL

    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT name FROM channel WHERE channel_id = ?", (channel_id,))
        row = cursor.fetchone()
        if not row:
            print(f" * [BACKFILL] Channel {channel_id} not found in DB. Aborting.")
            CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
            return
        channel_name = row['name']

        print(f" * [BACKFILL] Starting for '{channel_name}' ({channel_id})...")
        cursor.execute("UPDATE channel SET backfill_status = 'in_progress' WHERE channel_id = ?", (channel_id,))
        conn.commit()

        CURRENT_BACKFILL.update({
            "channel_id": channel_id,
            "channel_name": channel_name,
            "current": 0,
            "total": 0,
            "status": "starting"
        })

        videos_url = f"https://www.youtube.com/channel/{channel_id}/videos"

        # --- Phase 1: flat-playlist listing ---
        print(f" * [BACKFILL] Listing full catalog for '{channel_name}' (flat pass)...")
        try:
            flat_result = subprocess.run(
                ['yt-dlp', '--flat-playlist', '-J', videos_url],
                capture_output=True, text=True, timeout=120
            )
            if flat_result.returncode != 0:
                print(f" * [BACKFILL] yt-dlp flat listing failed for '{channel_name}': {flat_result.stderr[:300]}")
                cursor.execute("UPDATE channel SET backfill_status = 'failed' WHERE channel_id = ?", (channel_id,))
                conn.commit()
                CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
                return

            flat_data = json.loads(flat_result.stdout)
            entries = flat_data.get('entries', []) or []
        except subprocess.TimeoutExpired:
            print(f" * [BACKFILL] Timed out listing catalog for '{channel_name}'.")
            cursor.execute("UPDATE channel SET backfill_status = 'failed' WHERE channel_id = ?", (channel_id,))
            conn.commit()
            CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
            return
        except Exception as e:
            print(f" * [BACKFILL] Error during flat listing for '{channel_name}': {e}")
            cursor.execute("UPDATE channel SET backfill_status = 'failed' WHERE channel_id = ?", (channel_id,))
            conn.commit()
            CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
            return

        all_video_ids = [e['id'] for e in entries if e and e.get('id')]
        print(f" * [BACKFILL] '{channel_name}' has {len(all_video_ids)} videos total (per yt-dlp).")

        if not all_video_ids:
            print(f" * [BACKFILL] No videos found for '{channel_name}'. Done.")
            cursor.execute("UPDATE channel SET backfill_status = 'done' WHERE channel_id = ?", (channel_id,))
            conn.commit()
            CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
            return

        placeholders = ','.join(['?'] * len(all_video_ids))
        cursor.execute(f"SELECT video_id FROM video WHERE video_id IN ({placeholders})", all_video_ids)
        existing_ids = {r['video_id'] for r in cursor.fetchall()}
        missing_ids = [vid for vid in all_video_ids if vid not in existing_ids]

        print(f" * [BACKFILL] '{channel_name}': {len(missing_ids)} new videos to fetch, "
              f"{len(existing_ids)} already present.")

        if not missing_ids:
            print(f" * [BACKFILL] '{channel_name}' already fully up to date. Done.")
            cursor.execute("UPDATE channel SET backfill_status = 'done' WHERE channel_id = ?", (channel_id,))
            conn.commit()
            CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
            return

        # --- Set global progress state now that we know the real total ---
        CURRENT_BACKFILL.update({
            "channel_id": channel_id,
            "channel_name": channel_name,
            "current": 0,
            "total": len(missing_ids),
            "status": "running"
        })

        # --- Phase 2: slow, accurate per-video fetch ---
        added = 0
        was_cancelled = False

        for idx, video_id in enumerate(missing_ids):
            if CURRENT_BACKFILL["status"] == "cancelling":
                print(f" * [BACKFILL] Cancelled by user. Stopped at {idx}/{len(missing_ids)} for '{channel_name}'.")
                was_cancelled = True
                break

            try:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
                result = subprocess.run(
                    ['yt-dlp', '--dump-json', video_url],
                    capture_output=True, text=True, timeout=YTDLP_TIMEOUT_SECONDS
                )
                if result.returncode != 0:
                    print(f" * [BACKFILL] ({idx + 1}/{len(missing_ids)}) Skipping {video_id}: yt-dlp error.")
                else:
                    info = json.loads(result.stdout)
                    title = info.get('title') or 'No Title'
                    thumbnail_url = info.get('thumbnail') or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

                    upload_date_raw = info.get('upload_date')
                    if upload_date_raw:
                        try:
                            published_at_iso = datetime.strptime(upload_date_raw, "%Y%m%d").isoformat()
                        except ValueError:
                            published_at_iso = datetime.now().isoformat()
                    else:
                        published_at_iso = datetime.now().isoformat()

                    cursor.execute("SELECT video_id FROM video WHERE video_id = ?", (video_id,))
                    if not cursor.fetchone():
                        cursor.execute(
                            "INSERT INTO video (video_id, title, thumbnail_url, published_at, channel_id, is_hidden) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (video_id, title, thumbnail_url, published_at_iso, channel_id, False)
                        )
                        conn.commit()
                        added += 1

                    print(f" * [BACKFILL] '{channel_name}': {idx + 1}/{len(missing_ids)} processed, {added} added so far.")

            except subprocess.TimeoutExpired:
                print(f" * [BACKFILL] ({idx + 1}/{len(missing_ids)}) Timed out on {video_id}, skipping.")
            except Exception as e:
                print(f" * [BACKFILL] ({idx + 1}/{len(missing_ids)}) Error on {video_id}: {e}")

            CURRENT_BACKFILL["current"] = idx + 1
            time.sleep(BACKFILL_DELAY_SECONDS)

        if was_cancelled:
            cursor.execute("UPDATE channel SET backfill_status = 'cancelled' WHERE channel_id = ?", (channel_id,))
            print(f" * [BACKFILL] '{channel_name}' cancelled. {added} videos were added before stopping.")
        else:
            cursor.execute("UPDATE channel SET backfill_status = 'done' WHERE channel_id = ?", (channel_id,))
            print(f" * [BACKFILL] Complete for '{channel_name}'. Added {added} new videos.")
        conn.commit()

    except Exception as e:
        print(f" * [BACKFILL] Unexpected error for channel {channel_id}: {e}")
        try:
            cursor.execute("UPDATE channel SET backfill_status = 'failed' WHERE channel_id = ?", (channel_id,))
            conn.commit()
        except Exception:
            pass
    finally:
        CURRENT_BACKFILL.update({"channel_id": None, "channel_name": None, "current": 0, "total": 0, "status": "idle"})
        conn.close()

def is_weekend():
    return datetime.now().weekday() in (5, 6)
    #return True

def load_json(filepath, default):
    if not os.path.exists(filepath):
        save_json(filepath, default)
        return default
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_session():
    today_str = datetime.now().strftime('%Y-%m-%d')
    session = load_json(SESSION_FILE, {"date": today_str, "tokens_used": 0, "daily_pool_ids": []})
    if session.get("date") != today_str:
        session = {"date": today_str, "tokens_used": 0, "daily_pool_ids": []}
        save_json(SESSION_FILE, session)
    return session

def get_favorites():
    return load_json(FAVORITES_FILE, [])

def get_pinned():
    return load_json(PINNED_FILE, [])

def get_pinned_ids():
    return [p['video_id'] for p in get_pinned()]

def get_tokens_info():
    session = get_session()
    used = session.get("tokens_used", 0)
    max_quota = WEEKEND_WATCH_QUOTA if is_weekend() else WEEKDAY_WATCH_QUOTA
    remaining = max(0, max_quota - used)
    return {"is_weekend": is_weekend(), "remaining": remaining, "max": max_quota}

def get_or_create_daily_batch(db):
    """
    Builds (or retrieves the cached) merged weekday feed:
      - Up to WEEKDAY_FAVORITES_TARGET videos drawn from favorites (cooldown-gated,
        no 90-day window, no per-channel cap).
      - The remainder (to reach WEEKDAY_DAILY_POOL_SIZE) drawn from the fresh pool
        (90-day window, cooldown-gated, 1-per-channel candidate cap), excluding
        anything already picked from favorites.
      - Combined list is shuffled and cached for the day in session.json.
      - Every video_id selected (favorites or fresh) gets last_shown_at stamped,
        which is the single cooldown mechanism for both halves.
    """
    session = get_session()
    today_ids = session.get("daily_pool_ids", [])

    if not today_ids:
        cursor = db.cursor()
        cooldown_cutoff = (datetime.now() - timedelta(days=WEEKDAY_COOLDOWN_DAYS)).isoformat()

        pool_size = WEEKEND_DAILY_POOL_SIZE if is_weekend() else WEEKDAY_DAILY_POOL_SIZE
        fav_target = WEEKEND_FAVORITES_TARGET if is_weekend() else WEEKDAY_FAVORITES_TARGET

        # --- Favorites half ---
        pinned_ids_set = set(get_pinned_ids())
        fav_ids_all_raw = [f['video_id'] for f in get_favorites()]
        fav_ids_all = [vid for vid in fav_ids_all_raw if vid not in pinned_ids_set]
        favorites_batch = []
        eligible_fav_ids = []
        if fav_ids_all:
            placeholders = ','.join(['?'] * len(fav_ids_all))
            cursor.execute(f"""
                SELECT video_id FROM video
                WHERE video_id IN ({placeholders})
                  AND is_hidden = FALSE
                  AND (last_shown_at IS NULL OR last_shown_at <= ?)
            """, fav_ids_all + [cooldown_cutoff])
            eligible_fav_ids = [r['video_id'] for r in cursor.fetchall()]
            favorites_batch = random.sample(
                eligible_fav_ids, min(len(eligible_fav_ids), fav_target)
            )

        # --- Fresh half ---
        one_window_ago = (datetime.now() - timedelta(days=DEFAULT_DATE_LIMIT_DAYS)).isoformat()
        cursor.execute("""
            SELECT v.video_id, v.channel_id
            FROM video v
            WHERE v.is_hidden = FALSE
              AND v.published_at >= ?
              AND (v.last_shown_at IS NULL OR v.last_shown_at <= ?)
            ORDER BY v.published_at DESC
        """, (one_window_ago, cooldown_cutoff))
        recent_videos = cursor.fetchall()

        favorites_batch_set = set(favorites_batch)
        channel_counts = {}
        candidate_ids = []
        for video in recent_videos:
            vid = video['video_id']
            if vid in favorites_batch_set or vid in pinned_ids_set:
                continue  # no duplicates across halves, and pinned videos don't eat a pool slot
            ch_id = video['channel_id']
            if channel_counts.get(ch_id, 0) < MAX_VIDEOS_PER_CHANNEL_CANDIDATE:
                candidate_ids.append(vid)
                channel_counts[ch_id] = channel_counts.get(ch_id, 0) + 1

        fresh_target = pool_size - len(favorites_batch)
        fresh_batch = random.sample(candidate_ids, min(len(candidate_ids), fresh_target))

        # --- Combine & finalize ---
        today_ids = favorites_batch + fresh_batch
        random.shuffle(today_ids)

        # --- Build debug report ---
        cursor.execute("SELECT COUNT(*) as c FROM video WHERE is_hidden = FALSE")
        total_active_videos = cursor.fetchone()['c']

        frozen_favs_count = len(fav_ids_all) - len(eligible_fav_ids)
        frozen_fresh_count = len(recent_videos) - len(candidate_ids)  # includes both cooldown-frozen and per-channel-capped

        daily_report = {
            "built_at": datetime.now().isoformat(),
            "total_active_videos_scanned": total_active_videos,
            "videos_in_date_window": len(recent_videos),
            "total_favorites": len(fav_ids_all_raw),
            "favorites_excluded_pinned": len(fav_ids_all_raw) - len(fav_ids_all),
            "favorites_eligible": len(eligible_fav_ids),
            "favorites_frozen_cooldown": frozen_favs_count,
            "favorites_target": fav_target,
            "favorites_selected": len(favorites_batch),
            "fresh_candidates_after_caps": len(candidate_ids),
            "fresh_excluded_cooldown_or_cap": frozen_fresh_count,
            "fresh_target": fresh_target,
            "fresh_selected": len(fresh_batch),
            "pinned_excluded_from_pool": len(pinned_ids_set),
            "pool_size_target": pool_size,
            "final_batch_size": len(today_ids),
            "date_window_days": DEFAULT_DATE_LIMIT_DAYS,
            "cooldown_days": WEEKDAY_COOLDOWN_DAYS,
            "is_weekend": is_weekend(),
        }

        session["daily_pool_ids"] = today_ids
        session["daily_report"] = daily_report
        save_json(SESSION_FILE, session)

        if today_ids:
            now_iso = datetime.now().isoformat()
            placeholders = ','.join(['?'] * len(today_ids))
            cursor.execute(f"""
                UPDATE video
                SET last_shown_at = ?
                WHERE video_id IN ({placeholders})
            """, [now_iso] + today_ids)
            db.commit()

    if not today_ids:
        return [], session.get("daily_report", {})

    cursor = db.cursor()
    placeholders = ','.join(['?'] * len(today_ids))
    cursor.execute(f"""
        SELECT v.video_id, v.title, v.thumbnail_url, v.published_at, v.channel_id, v.is_hidden, v.last_shown_at, c.name as channel_name
        FROM video v
        JOIN channel c ON v.channel_id = c.channel_id
        WHERE v.video_id IN ({placeholders})
    """, today_ids)

    # Preserve the shuffled order stored in session (SQL IN-clause doesn't guarantee order)
    rows_by_id = {row['video_id']: dict(row) for row in cursor.fetchall()}
    fav_ids = {f['video_id'] for f in get_favorites()}
    pinned_ids = set(get_pinned_ids())
    results = []
    for vid in today_ids:
        d = rows_by_id.get(vid)
        if not d:
            continue
        d['is_favorite'] = 1 if d['video_id'] in fav_ids else 0
        d['is_pinned'] = 1 if d['video_id'] in pinned_ids else 0
        annotate_cooldown(d)
        results.append(d)
    return results, session.get("daily_report", {})


def annotate_cooldown(video_dict):
    """Adds is_cooling_down + cooldown_until_formatted to a video dict based on
    last_shown_at. Purely informational — doesn't affect what queries return,
    just lets the UI explain why a video isn't showing up in the daily feed."""
    last_shown_at = video_dict.get('last_shown_at')
    if not last_shown_at:
        video_dict['is_cooling_down'] = False
        video_dict['cooldown_until_formatted'] = None
        return video_dict

    try:
        last_shown_dt = datetime.fromisoformat(last_shown_at)
    except (ValueError, TypeError):
        video_dict['is_cooling_down'] = False
        video_dict['cooldown_until_formatted'] = None
        return video_dict

    cooldown_until = last_shown_dt + timedelta(days=WEEKDAY_COOLDOWN_DAYS)
    video_dict['is_cooling_down'] = datetime.now() < cooldown_until
    video_dict['cooldown_until_formatted'] = cooldown_until.strftime('%m/%d/%y %I:%M %p')
    return video_dict


def format_relative_time(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        # Strip timezone info if present to compare against local datetime.now()
        if dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        diff = datetime.now() - dt
        days = diff.days

        if days <= 0:
            return "today"
        elif days == 1:
            return "yesterday"
        elif days < 30:
            return f"{days} days ago"
        elif days < 365:
            months = max(1, days // 30)
            return f"{months} month{'s' if months > 1 else ''} ago"
        else:
            years = max(1, days // 365)
            return f"{years} year{'s' if years > 1 else ''} ago"
    except Exception:
        return iso_str

def get_pinned_videos(db):
    """Returns hydrated pinned video rows, ignoring is_hidden and cooldown entirely."""
    pinned_ids = get_pinned_ids()
    if not pinned_ids:
        return []
    cursor = db.cursor()
    placeholders = ','.join(['?'] * len(pinned_ids))
    cursor.execute(f"""
        SELECT v.video_id, v.title, v.thumbnail_url, v.published_at, v.channel_id, v.last_shown_at, c.name as channel_name
        FROM video v
        JOIN channel c ON v.channel_id = c.channel_id
        WHERE v.video_id IN ({placeholders})
    """, pinned_ids)
    rows_by_id = {row['video_id']: dict(row) for row in cursor.fetchall()}
    fav_ids = {f['video_id'] for f in get_favorites()}
    results = []
    for vid in pinned_ids:
        d = rows_by_id.get(vid)
        if not d:
            continue
        d['is_favorite'] = 1 if d['video_id'] in fav_ids else 0
        d['is_pinned'] = 1
        annotate_cooldown(d)
        results.append(d)
    return results

# --- Flask Routes ---
@app.route('/')
def index():
    db = get_db()
    tokens = get_tokens_info()
    main_feed_videos, daily_report = get_or_create_daily_batch(db)

    pinned_videos = get_pinned_videos(db)
    pinned_id_set = {p['video_id'] for p in pinned_videos}
    # Pinned videos render in their own fixed row; strip them out of the main grid
    # so they never appear twice (mutual-exclusion is display-only, per design).
    main_feed_videos = [v for v in main_feed_videos if v['video_id'] not in pinned_id_set]

    for video in main_feed_videos + pinned_videos:
        video['published_at_formatted'] = format_relative_time(video['published_at'])

    for video in main_feed_videos:
        video['is_downloaded'] = 1 if video['video_id'] in DOWNLOADED_VIDEO_IDS else 0
        video['is_downloading'] = 1 if video['video_id'] in DOWNLOADING_IDS else 0
    for video in pinned_videos:
        video['is_downloaded'] = 1 if video['video_id'] in DOWNLOADED_VIDEO_IDS else 0
        video['is_downloading'] = 1 if video['video_id'] in DOWNLOADING_IDS else 0

    return render_template(
        'index.html',
        videos=main_feed_videos,
        pinned_videos=pinned_videos,
        pinned_max=PINNED_MAX,
        tokens=tokens,
        is_favorites_page=False,
        downloads_storage_size=DOWNLOADS_STORAGE_SIZE,
        downloaded_count=len(DOWNLOADED_VIDEO_IDS),
        daily_report=daily_report
    )

@app.route('/favorites')
def favorites_view():
    tokens = get_tokens_info()
    if not tokens['is_weekend']:
        # Favorites are woven into the merged weekday feed; a standalone
        # favorites-only view would be redundant on weekdays.
        return redirect(url_for('index'))

    db = get_db()
    fav_list = get_favorites()
    fav_ids = [f['video_id'] for f in fav_list]

    if not fav_ids:
        return render_template(
            'index.html',
            videos=[],
            pinned_videos=get_pinned_videos(db),
            pinned_max=PINNED_MAX,
            tokens=tokens,
            is_favorites_page=True,
            downloads_storage_size=DOWNLOADS_STORAGE_SIZE,
            downloaded_count=len(DOWNLOADED_VIDEO_IDS)
        )

    cursor = db.cursor()
    placeholders = ','.join(['?'] * len(fav_ids))
    cursor.execute(f"""
        SELECT v.video_id, v.title, v.thumbnail_url, v.published_at, v.channel_id, v.is_hidden, v.last_shown_at, c.name as channel_name
        FROM video v
        JOIN channel c ON v.channel_id = c.channel_id
        WHERE v.video_id IN ({placeholders})
    """, fav_ids)
    
    # Map video_id to saved_at timestamp from favorites.json
    saved_at_map = {f['video_id']: f.get('saved_at', '') for f in fav_list}
    pinned_ids = set(get_pinned_ids())

    fav_videos = []
    for row in cursor.fetchall():
        d = dict(row)
        d['is_favorite'] = 1
        d['is_pinned'] = 1 if d['video_id'] in pinned_ids else 0
        d['is_downloaded'] = 1 if d['video_id'] in DOWNLOADED_VIDEO_IDS else 0
        d['is_downloading'] = 1 if d['video_id'] in DOWNLOADING_IDS else 0
        d['saved_at'] = saved_at_map.get(d['video_id'], '')
        video_dt = datetime.fromisoformat(d['published_at'])
        d['published_at_formatted'] = video_dt.strftime('%m/%d/%y')
        annotate_cooldown(d)
        fav_videos.append(d)

    # Sort descending by added date (most recently saved appears at the top)
    fav_videos.sort(key=lambda x: x['saved_at'], reverse=True)

    return render_template(
        'index.html',
        videos=fav_videos,
        pinned_videos=get_pinned_videos(db),
        pinned_max=PINNED_MAX,
        tokens=tokens,
        is_favorites_page=True,
        downloads_storage_size=DOWNLOADS_STORAGE_SIZE,
        downloaded_count=len(DOWNLOADED_VIDEO_IDS)
    )
@app.route('/feeds')
def list_feeds():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT name, channel_id, feed_url FROM channel ORDER BY name ASC")
    channels = cursor.fetchall()
    return render_template('feeds.html', channels=channels, is_weekend=is_weekend())

@app.route('/channel/<channel_id>')
def channel_view(channel_id):
    if not is_weekend():
        return redirect(url_for('index'))

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT name, backfill_status FROM channel WHERE channel_id = ?", (channel_id,))
    channel_row = cursor.fetchone()
    if not channel_row:
        return "Channel not found", 404
    channel_name = channel_row['name']
    backfill_status = channel_row['backfill_status'] or 'pending'

    cursor.execute(
        """SELECT v.video_id, v.title, v.thumbnail_url, v.published_at, v.is_hidden, v.last_shown_at
           FROM video v WHERE v.channel_id = ? ORDER BY v.published_at DESC""",
        (channel_id,)
    )
    videos = [dict(row) for row in cursor.fetchall()]

    fav_ids = {f['video_id'] for f in get_favorites()}
    pinned_ids = set(get_pinned_ids())
    for video in videos:
        video['is_favorite'] = 1 if video['video_id'] in fav_ids else 0
        video['is_pinned'] = 1 if video['video_id'] in pinned_ids else 0
        video['is_downloaded'] = 1 if video['video_id'] in DOWNLOADED_VIDEO_IDS else 0
        video['is_downloading'] = 1 if video['video_id'] in DOWNLOADING_IDS else 0
        video_dt = datetime.fromisoformat(video['published_at'])
        video['published_at_formatted'] = video_dt.strftime('%m/%d/%y')
        annotate_cooldown(video)

    tokens = get_tokens_info()
    return render_template(
        'channel.html',
        channel_name=channel_name,
        videos=videos,
        channel_id=channel_id,
        tokens=tokens,
        backfill_status=backfill_status,
        current_backfill=CURRENT_BACKFILL,
        downloads_storage_size=DOWNLOADS_STORAGE_SIZE,
        downloaded_count=len(DOWNLOADED_VIDEO_IDS)
    )

@app.route('/api/can_watch', methods=['GET'])
def can_watch():
    tokens = get_tokens_info()
    if tokens['remaining'] <= 0:
        return jsonify({"allowed": False, "message": "Daily watch quota reached."}), 403
    return jsonify({"allowed": True, "tokens": tokens})

@app.route('/api/consume_token', methods=['POST'])
def consume_token():
    session = get_session()
    session["tokens_used"] = session.get("tokens_used", 0) + 1
    save_json(SESSION_FILE, session)
    
    updated_tokens = get_tokens_info()
    print(f" * [TOKEN BURNED] Total used today: {session['tokens_used']} | Remaining: {updated_tokens['remaining']}")
    return jsonify({"success": True, "tokens": updated_tokens})




@app.route('/api/download_video', methods=['POST'])
def download_video_route():
    data = request.get_json(silent=True) or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID is required"}), 400

    # Download and favorite are independent intents — no auto-favoriting here.
    if video_id in DOWNLOADED_VIDEO_IDS:
        return jsonify({"success": True, "message": "Already downloaded"}), 200

    if video_id in DOWNLOADING_IDS:
        return jsonify({"success": True, "message": "Download already queued or in progress"}), 200

    DOWNLOADING_IDS.add(video_id)
    DOWNLOAD_QUEUE.put(video_id)
    return jsonify({"success": True, "message": f"Queued {video_id} for download."})

@app.route('/api/cancel_download', methods=['POST'])
def cancel_download_route():
    data = request.get_json(silent=True) or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID is required"}), 400

    DOWNLOADING_IDS.discard(video_id)

    proc = ACTIVE_DOWNLOAD_PROCESSES.pop(video_id, None)
    if proc:
        try:
            proc.kill()
        except Exception:
            pass

    for partial_path in glob.glob(os.path.join(DOWNLOADS_DIR, f"{video_id}.*")):
        try:
            os.remove(partial_path)
        except OSError:
            pass

    sync_downloads_from_disk()
    return jsonify({
        "success": True,
        "message": f"Download cancelled for {video_id}",
        "storage_size": DOWNLOADS_STORAGE_SIZE,
        "downloaded_count": len(DOWNLOADED_VIDEO_IDS)
    })





@app.route('/api/download_status', methods=['GET'])
def download_status_route():
    return jsonify({
        "downloaded_ids": list(DOWNLOADED_VIDEO_IDS),
        "downloading_ids": list(DOWNLOADING_IDS),
        "storage_size": DOWNLOADS_STORAGE_SIZE
    })

@app.route('/downloads/<video_id>.mp4')
def stream_downloaded_video(video_id):
    return send_from_directory(DOWNLOADS_DIR, f"{video_id}.mp4")

@app.route('/storage')
def storage_view():
    """Management-only page listing every downloaded video on disk.
    Not playable from here. Videos whose parent record was removed from the
    DB (e.g. their channel was deleted) are labeled 'Deleted from database'
    rather than silently omitted."""
    db = get_db()
    video_ids = list(DOWNLOADED_VIDEO_IDS)

    known_rows = {}
    if video_ids:
        cursor = db.cursor()
        placeholders = ','.join(['?'] * len(video_ids))
        cursor.execute(f"""
            SELECT v.video_id, v.title, v.thumbnail_url, c.name as channel_name
            FROM video v
            LEFT JOIN channel c ON v.channel_id = c.channel_id
            WHERE v.video_id IN ({placeholders})
        """, video_ids)
        known_rows = {row['video_id']: dict(row) for row in cursor.fetchall()}

    entries = []
    total_bytes = 0
    for vid in sorted(video_ids):
        size_bytes = DOWNLOADED_VIDEO_SIZES.get(vid, 0)
        total_bytes += size_bytes
        row = known_rows.get(vid)
        entries.append({
            "video_id": vid,
            "title": row['title'] if row else None,
            "thumbnail_url": row['thumbnail_url'] if row else None,
            "channel_name": row['channel_name'] if row else None,
            "size_bytes": size_bytes,
            "size_formatted": format_bytes(size_bytes),
            "is_orphaned": row is None,
        })

    # Largest first — most useful order when reclaiming space
    entries.sort(key=lambda e: e['size_bytes'], reverse=True)

    return render_template(
        'storage.html',
        entries=entries,
        total_count=len(entries),
        total_size_formatted=format_bytes(total_bytes),
        tokens=get_tokens_info()
    )

@app.route('/api/clear_downloads', methods=['POST'])
def clear_downloads_route():
    pattern = os.path.join(DOWNLOADS_DIR, "*.mp4")
    deleted_count = 0
    for filepath in glob.glob(pattern):
        try:
            os.remove(filepath)
            deleted_count += 1
        except OSError as e:
            print(f" * [CLEAR] Error removing {filepath}: {e}")

    sync_downloads_from_disk()
    return jsonify({
        "success": True,
        "message": f"Removed {deleted_count} video(s).",
        "storage_size": DOWNLOADS_STORAGE_SIZE
    })


@app.route('/api/delete_download', methods=['POST'])
def delete_single_download_route():
    data = request.get_json(silent=True) or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID required"}), 400

    target_file = os.path.join(DOWNLOADS_DIR, f"{video_id}.mp4")
    if os.path.exists(target_file):
        try:
            os.remove(target_file)
            sync_downloads_from_disk()
            return jsonify({
                "success": True,
                "storage_size": DOWNLOADS_STORAGE_SIZE,
                "downloaded_count": len(DOWNLOADED_VIDEO_IDS)
            })
        except OSError as e:
            return jsonify({"success": False, "message": f"Error deleting file: {e}"}), 500

    sync_downloads_from_disk()
    return jsonify({
        "success": True,
        "storage_size": DOWNLOADS_STORAGE_SIZE,
        "downloaded_count": len(DOWNLOADED_VIDEO_IDS)
    })

def toggle_favorite(video_id):
    """Toggles favorite membership only. Local download status is a fully
    independent concern and is never touched here in either direction."""
    favorites = get_favorites()
    exists = any(f['video_id'] == video_id for f in favorites)

    if exists:
        favorites = [f for f in favorites if f['video_id'] != video_id]
        is_fav = False
    else:
        favorites.append({"video_id": video_id, "saved_at": datetime.now().isoformat()})
        is_fav = True

    save_json(FAVORITES_FILE, favorites)
    return is_fav


def toggle_pinned(video_id):
    """Toggles pinned membership only, enforcing the hard PINNED_MAX cap.
    Returns (success, is_pinned, message)."""
    pinned = get_pinned()
    exists = any(p['video_id'] == video_id for p in pinned)

    if exists:
        pinned = [p for p in pinned if p['video_id'] != video_id]
        save_json(PINNED_FILE, pinned)
        return True, False, None

    if len(pinned) >= PINNED_MAX:
        return False, False, f"Pinned list is full ({PINNED_MAX} max). Unpin something first."

    pinned.append({"video_id": video_id, "pinned_at": datetime.now().isoformat()})
    save_json(PINNED_FILE, pinned)
    return True, True, None

@app.route('/api/toggle_favorite', methods=['POST'])
def api_toggle_favorite():
    data = request.get_json() or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID is required"}), 400

    is_fav = toggle_favorite(video_id)
    return jsonify({
        "success": True, 
        "is_favorite": is_fav,
        "storage_size": DOWNLOADS_STORAGE_SIZE,
        "downloaded_count": len(DOWNLOADED_VIDEO_IDS)
    })

@app.route('/api/toggle_pinned', methods=['POST'])
def api_toggle_pinned():
    data = request.get_json() or {}
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID is required"}), 400

    success, is_pinned, message = toggle_pinned(video_id)
    if not success:
        return jsonify({"success": False, "message": message}), 409

    return jsonify({"success": True, "is_pinned": is_pinned})

@app.route('/api/hide_video', methods=['POST'])
def hide_video():
    """Toggles is_hidden. In feed contexts the video simply disappears on hide;
    in channel view (the only place a hidden video can be seen again) the
    symbol just flips state so it can be unhidden."""
    data = request.get_json()
    video_id = data.get('video_id')
    if not video_id:
        return jsonify({"success": False, "message": "Video ID is required"}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT is_hidden FROM video WHERE video_id = ?", (video_id,))
    row = cursor.fetchone()
    if not row:
        return jsonify({"success": False, "message": "Video not found"}), 404

    new_state = not bool(row['is_hidden'])
    cursor.execute("UPDATE video SET is_hidden = ? WHERE video_id = ?", (new_state, video_id))
    db.commit()
    return jsonify({"success": True, "is_hidden": new_state})

@app.route('/refresh')
def refresh():
    # Available on both weekday and weekend. On weekdays this only grows the
    # pool available for *tomorrow's* daily batch — today's already-cached
    # selection is unaffected.
    print(" * Manual force refresh triggered.")
    fetch_and_store_feeds(force=True)
    return redirect(url_for('index'))

def resolve_channel_via_ytdlp(url):
    """Resolve any YouTube URL/handle to (channel_id, channel_name) using yt-dlp.
    Returns (None, None) on failure."""
    try:
        result = subprocess.run(
            ['yt-dlp', '--print', '%(channel_id)s|||%(channel)s', '--playlist-items', '1', url],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return None, None
        line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else None
        if not line or '|||' not in line:
            return None, None
        cid, name = line.split('|||', 1)
        cid = cid.strip()
        name = name.strip()
        if cid.startswith('UC'):
            return cid, (name or None)
        return None, None
    except Exception:
        return None, None

def _insert_channel_row(cursor, name, feed_url, channel_id):
    cursor.execute("SELECT channel_id FROM channel WHERE channel_id = ? OR feed_url = ?", (channel_id, feed_url))
    if cursor.fetchone():
        return False, f"Channel with ID or Feed URL '{feed_url}' already exists."
    cursor.execute(
        "INSERT INTO channel (name, feed_url, channel_id) VALUES (?, ?, ?)",
        (name, feed_url, channel_id)
    )
    return True, None

@app.route('/api/resolve_channel', methods=['POST'])
def resolve_channel_route():
    data = request.get_json(silent=True) or {}
    url = data.get('url', '').strip()
    if not url:
        return jsonify({"success": False, "message": "URL is required"}), 400
    if 'feeds/videos.xml' in url:
        return jsonify({"success": False, "message": "This is already a raw feed URL; no name to resolve."}), 200
    cid, name = resolve_channel_via_ytdlp(url)
    if not cid:
        return jsonify({"success": False, "message": "Could not resolve a channel from that URL."}), 400
    return jsonify({"success": True, "channel_name": name})

@app.route('/api/add_channel', methods=['POST'])
def add_channel():
    data = request.get_json()
    name = (data.get('name') or '').strip()
    submitted_url = data.get('feed_url')
    content_type = data.get('content_type', 'videos')  # 'videos' | 'shorts' | 'both'

    if not submitted_url:
        return jsonify({"success": False, "message": "URL is required"}), 400

    db = get_db()
    cursor = db.cursor()

    is_raw_feed = 'feeds/videos.xml' in submitted_url

    try:
        if is_raw_feed:
            # Backward-compatible path: user pasted an actual RSS feed URL directly.
            response = requests.get(submitted_url, timeout=10)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            extracted_channel_id = None
            if hasattr(feed.feed, 'yt_channelid'):
                extracted_channel_id = feed.feed.yt_channelid
            elif hasattr(feed.feed, 'id') and 'yt:channel:' in feed.feed.id:
                extracted_channel_id = feed.feed.id.split('yt:channel:')[1]
            elif hasattr(feed.feed, 'link'):
                match = re.search(r'(UC[\w-]{21}[AQgw])', feed.feed.link)
                if match:
                    extracted_channel_id = match.group(1)

            if not extracted_channel_id:
                return jsonify({"success": False, "message": "Could not extract YouTube Channel ID from the provided feed URL."}), 400

            if not name:
                name = getattr(feed.feed, 'title', None) or extracted_channel_id

            ok, err = _insert_channel_row(cursor, name, submitted_url, extracted_channel_id)
            if not ok:
                return jsonify({"success": False, "message": err}), 409

            db.commit()
            fetch_and_store_feeds()
            return jsonify({"success": True, "message": f"Channel '{name}' added successfully. Initial fetch initiated."}), 201

        # --- New path: handle / channel URL, resolved via yt-dlp ---
        base_channel_id, resolved_name = resolve_channel_via_ytdlp(submitted_url)
        if not base_channel_id:
            return jsonify({"success": False, "message": "Could not resolve a channel ID from that URL."}), 400

        if not name:
            name = resolved_name or base_channel_id

        uploads_id = 'UU' + base_channel_id[2:]      # videos-only playlist
        shorts_id = 'UUSH' + base_channel_id[2:]     # shorts-only playlist

        added_names = []

        if content_type in ('videos', 'both'):
            feed_url = f"https://www.youtube.com/feeds/videos.xml?playlist_id={uploads_id}"
            ok, err = _insert_channel_row(cursor, name, feed_url, base_channel_id)
            if not ok:
                return jsonify({"success": False, "message": err}), 409
            added_names.append(name)

        if content_type in ('shorts', 'both'):
            shorts_name = f"{name} (SHORTS)"
            feed_url = f"https://www.youtube.com/feeds/videos.xml?playlist_id={shorts_id}"
            ok, err = _insert_channel_row(cursor, shorts_name, feed_url, base_channel_id + "_SHORTS")
            if not ok:
                return jsonify({"success": False, "message": err}), 409
            added_names.append(shorts_name)

        db.commit()
        fetch_and_store_feeds()
        return jsonify({
            "success": True,
            "message": f"Added: {', '.join(added_names)}. Initial fetch initiated."
        }), 201

    except sqlite3.IntegrityError:
        db.rollback()
        return jsonify({"success": False, "message": "Channel with this Feed URL or Channel ID already exists."}), 409
    except requests.exceptions.RequestException as e:
        db.rollback()
        return jsonify({"success": False, "message": f"Error fetching feed URL: {str(e)}"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"success": False, "message": f"Error adding channel: {str(e)}"}), 500

@app.route('/api/delete_channel', methods=['POST'])
def delete_channel():
    data = request.get_json()
    channel_id = data.get('channel_id')

    if not channel_id:
        return jsonify({"success": False, "message": "Channel ID is required"}), 400

    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("DELETE FROM channel WHERE channel_id = ?", (channel_id,))
        db.commit()
        return jsonify({"success": True, "message": f"Channel {channel_id} and its videos deleted."})
    except Exception as e:
        db.rollback()
        return jsonify({"success": False, "message": f"Error deleting channel: {str(e)}"}), 500

@app.route('/api/backfill_channel', methods=['POST'])
def backfill_channel_route():
    data = request.get_json(silent=True) or {}
    channel_id = data.get('channel_id')

    if not channel_id:
        return jsonify({"success": False, "message": "Channel ID is required"}), 400

    if CURRENT_BACKFILL["status"] != "idle":
        return jsonify({
            "success": False,
            "message": f"A sync is already in progress for '{CURRENT_BACKFILL['channel_name']}'. Cancel it first."
        }), 409

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT name FROM channel WHERE channel_id = ?", (channel_id,))
    row = cursor.fetchone()
    if not row:
        return jsonify({"success": False, "message": "Channel not found."}), 404

    print(f" * [BACKFILL] Queued for '{row['name']}' ({channel_id}). Check terminal for progress.")
    threading.Thread(target=backfill_channel, args=(channel_id,), daemon=True).start()

    return jsonify({"success": True, "message": "Backfill started. Progress will print in the server terminal."})

@app.route('/api/cancel_backfill', methods=['POST'])
def cancel_backfill_route():
    if CURRENT_BACKFILL["status"] != "running":
        return jsonify({"success": False, "message": "No active sync to cancel."}), 409

    CURRENT_BACKFILL["status"] = "cancelling"
    print(f" * [BACKFILL] Cancel requested for '{CURRENT_BACKFILL['channel_name']}'.")
    return jsonify({"success": True, "message": "Cancelling..."})

@app.route('/api/backfill_status', methods=['GET'])
def backfill_status_route():
    return jsonify(CURRENT_BACKFILL)


def check_and_update_yt_dlp():
    """Runs pip install --upgrade yt-dlp if YTDLP_UPDATE_INTERVAL_DAYS have
    passed since the last recorded update. Uses sys.executable so it always
    targets the same venv the app itself is running in."""
    import sys
    state = load_json(YTDLP_UPDATE_FILE, {"last_update": None})

    if state.get("last_update"):
        try:
            last_update_dt = datetime.fromisoformat(state["last_update"])
            days_since = (datetime.now() - last_update_dt).days
            if days_since < YTDLP_UPDATE_INTERVAL_DAYS:
                print(f" * [YT-DLP] Last updated {days_since} day(s) ago. Skipping (interval: {YTDLP_UPDATE_INTERVAL_DAYS}d).")
                return
        except (ValueError, TypeError):
            pass

    print(" * [YT-DLP] Checking for updates...")
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '--upgrade', 'yt-dlp'],
            capture_output=True, text=True, timeout=60
        )
        if 'Successfully installed' in result.stdout:
            last_line = result.stdout.strip().splitlines()[-1]
            print(f" * [YT-DLP] Updated: {last_line}")
        else:
            print(" * [YT-DLP] Already at latest version.")

        state["last_update"] = datetime.now().isoformat()
        save_json(YTDLP_UPDATE_FILE, state)
    except Exception as e:
        print(f" * [YT-DLP] Update check failed: {e}")


def free_port(port):
    """Force kill any process occupying the port and wait for the OS to release the socket."""
    try:
        result = subprocess.run(
            ['lsof', '-t', f'-i:{port}'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        pids = [p for p in result.stdout.strip().split() if p and p != str(os.getpid())]
        
        if pids:
            for pid in pids:
                print(f" * Force-killing stale process PID {pid} on port {port}...")
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass

            # Wait briefly for socket TIME_WAIT state to clear
            for _ in range(10):
                check = subprocess.run(['lsof', '-t', f'-i:{port}'], stdout=subprocess.PIPE, text=True)
                remaining = [p for p in check.stdout.strip().split() if p and p != str(os.getpid())]
                if not remaining:
                    break
                time.sleep(0.2)
    except Exception as e:
        print(f" * Port release warning: {e}")

if __name__ == '__main__':
    free_port(HTTP_PORT)
    init_db()
    check_and_update_yt_dlp()
    sync_downloads_from_disk()

    # Launch background download workers
    for _ in range(MAX_CONCURRENT_DOWNLOADS):
        threading.Thread(target=download_worker, daemon=True).start()

    def delayed_fetch():
        time.sleep(0.5)
        with app.app_context():
            print("")
            print(" * Starting background fetch")
            fetch_and_store_feeds(force=False)

    threading.Thread(target=delayed_fetch, daemon=True).start()
    app.run(debug=False, host='0.0.0.0', port=HTTP_PORT)