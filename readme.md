# 🧘‍♂️ MonkFeeds (FocusFeeds)

YouTube is one of the best places on the web to learn and follow great creators — but the platform's recommendation engine is built to trap your attention, not serve it.

Opening a tab for a quick 10-minute video can easily spiral into hours of algorithmic doom-scrolling, rabbit holes, and lost evenings. Traditional site blockers often fail because they rely on binary discipline: either you cut off access entirely, or you disable the blocker and immediately fall back into the binge loop.

**MonkFeeds** is a lightweight, distraction-free RSS frontend designed to turn YouTube into an intentional tool instead of an endless casino.

---

## How It Works

Instead of infinite feeds and autoplay traps, MonkFeeds introduces intentional friction, structure, and artificial scarcity:

- **Daily Token Economy** — Video playback is powered by a strict daily quota of watch tokens. When your tokens run out, playback locks until tomorrow.
- **Finite Daily Batches** — No endless scrolling. The app builds a single, limited batch of videos from your subscriptions every morning. Once you browse through the list, the feed is finished.
- **A 10-Second Preview Buffer** — Impulsively clicked a video? You get a 10-second trial window before a token actually burns. Clickbait? Close it and keep your token safe.
- **Weekday vs. Weekend Cadence** — Weekdays are reserved for focused, bite-sized consumption. Deep-dive channel browsing and back-catalog spelunking unlock exclusively on weekends, protecting your work nights.
- **No Algorithmic Interference** — Zero comment sections, zero recommended sidebars, zero autoplay loops. Just raw RSS feeds from the channels you explicitly choose to follow.

It's designed for intentionality over endless engagement — built to let you enjoy the good parts of YouTube without letting the algorithm take over your time.

---

## The Main Feed

- Every morning, a single batch of videos is drawn from your subscriptions and shuffled for the day.
- To prevent one prolific creator from overwhelming your day, each channel is capped at a single candidate video in the fresh pool.
- Once a video appears in your feed, it enters a temporary cooldown so the same content doesn't loop back day after day.
- When you reach the end of the batch, you're done. No infinite scroll, no pull-to-refresh.

## Weekdays vs. Weekends

**Weekdays (Focus Mode)** — Quotas are tight (10 watch tokens/day). The feed stays lean (30 videos total). Channel pages remain locked so you can't fall into a rabbit hole.

**Weekends (Relax Mode)** — Your daily budget expands to 50 tokens, and the pool grows to 120 videos. All channels unlock, letting you browse creators directly, explore full back-catalogs, and binge older uploads at your own pace.

## Favorites

- Marking a video as a favorite saves it to your library for easy access.
- On weekdays, the engine automatically weaves a small handful of your saved favorites into your daily feed pool, keeping familiar favorites in rotation.
- On weekends, the dedicated Favorites view unlocks completely, letting you freely browse your full library chronologically.

## Pinned Slots

- Up to 3 persistent pinned slots, fixed at the top of your feed.
- Pinned videos ignore cooldowns and hiding rules entirely, and never consume a daily-batch slot — perfect for long-form tutorials, ongoing courses, or reference material you're currently working through.

---

## Why This Exists & Why It's Free

I built MonkFeeds to help manage my own ADHD.

My brain is particularly vulnerable to the hyper-stimulating rabbit holes and dopamine loops built into modern video platforms. Building this system gave me back control over my attention without forcing total digital isolation, and I'm open-sourcing it in the hope it helps other people dealing with ADHD or chronic digital distraction reclaim their focus too.

As much as I'd love to polish this into a full consumer release with one-click installers and an active support forum, my current bandwidth doesn't allow it. Right now, the most honest way to share this is by dropping the raw, working codebase into a repository for anyone to use and adapt freely.

---

## Tech Stack & Installation

A lightweight local application built on standard, reliable tools:

- **Python 3** (Flask, Requests, Feedparser)
- **SQLite** (bundled with Python — no external database server needed)
- **yt-dlp** (channel resolution and offline video fetching)
- **ffmpeg** (required by `yt-dlp` to merge audio and video streams)

### Quick Setup

1. Clone or download the repository.
2. Install `ffmpeg` via your system package manager:
   - macOS: `brew install ffmpeg`
   - Debian/Ubuntu: `sudo apt install ffmpeg`
   - Windows: `winget install ffmpeg`
3. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```
4. Adjust the local path variables at the top of `focusfeed.py` (storage directory, etc.) to match your machine.
5. Launch the server:
   ```bash
   python focusfeed.py
   ```
6. Open your browser to `http://localhost:777`.

---

## Pro Tip: Let an LLM Tailor It for You

If you aren't deeply technical, or just want to tweak how the system runs, you don't have to figure it out alone. Drop `focusfeed.py` and the HTML templates into your favorite LLM (Claude, ChatGPT, etc.) and ask something like:

> *"Here's a small local Flask app. Help me configure the paths, install the dependencies on [Mac / Windows / Linux], and adapt it to my personal workflow."*

The architecture is clean, vanilla, and dependency-light, so AI assistants can easily help you adjust quotas, tweak timers, or containerize it for your exact setup.

---

## Critical Setup Note: Complete the Lockdown

Using MonkFeeds alone won't solve the problem if standard YouTube remains one impulsive click away. For this system to actually work, you need friction against bypassing it:

- **Block distracting sites at the DNS/system level.** Use tools like OpenDNS, NextDNS, or local `/etc/hosts` rules to hard-block `youtube.com` and other doom-scrolling sites on your machine.
- **Keep the player working.** Do not block `youtube-nocookie.com` — the embedded MonkFeeds player streams playback from that domain specifically, so it must stay allowed on your network for the app to function.

---

## Support & Disclaimer

- **As-is, no guarantees.** This project is provided strictly as-is, with zero warranties or guarantees of fitness for any purpose. It was tailored for my personal machine and workflow — your mileage may vary.
- **Support availability.** Because I need to protect my own focus and daily commitments, I can't offer free technical support, troubleshooting, or custom feature implementations. If you're stuck or need hands-on configuration, custom adaptations, or dedicated assistance, I'm open to consulting on a paid basis. Otherwise, you have full freedom to fork, modify, and build on top of this codebase.