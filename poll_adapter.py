#!/usr/bin/env python3
"""Mention detection adapter for the A2A Slack bridge (polling source).

Run by daemon.py as a subprocess each poll loop. Configuration comes
from the bridge config file (env A2A_CONFIG, default: config.json next
to this file): channel_id, bot_user_id, bot_id. The state file path
comes from env A2A_POLL_STATE, so the daemon keeps its OWN watermark
and never shares state with any other poller.

Detection works in three passes against the Slack API (via
slack_client.py, which holds the token resolution):

  1. Channel history since the watermark. New top-level messages
     containing the bot's real mention token (<@BOT_USER_ID>) become
     events with scope "channel".
  2. Thread discovery: a second history read (latest 20) registers any
     message with replies as a tracked thread.
  3. Tracked-thread scans: conversations.replies per tracked thread,
     from that thread's own watermark; new replies containing the
     mention token become events with scope "thread". Scans are
     round-robin batched (env A2A_THREAD_BATCH, default 0 = all): at a
     5-second cadence, scanning every thread every cycle would exceed
     Slack Tier-3 rate limits (~50 req/min/method); batching keeps
     each cycle cheap while every thread is covered within a few
     cycles. The cursor persists in the state file.

Mention matching is exact: only the real mention token counts. A
plain-text "@handle" is not a mention and fires nothing.

Read-only. Never posts. Emits exactly one JSON object on stdout:
{"ok": true, "new_mentions": [...], "channel_last_ts": ...} or
{"ok": false, "error": ..., "step": ...} with exit code 1.
The watermark is advanced only on full success: state is saved as the
last step, so a failed run retries the same range next cycle.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import slack_client  # noqa: E402
from slack_client import call  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))


def load_config():
    path = os.environ.get("A2A_CONFIG") or os.path.join(_HERE,
                                                         "config.json")
    with open(path) as f:
        return json.load(f)


CFG = load_config()
CHANNEL = CFG["channel_id"]
BOT_USER_ID = CFG["bot_user_id"]
BOT_ID = CFG.get("bot_id")            # bot id, for self-filtering
MENTION_TOKEN = f"<@{BOT_USER_ID}>"

STATE_FILE = os.environ.get(
    "A2A_POLL_STATE", os.path.join(_HERE, "state", "poll-state.json"))
THREAD_BATCH = int(os.environ.get("A2A_THREAD_BATCH", "0"))


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"channel_last_ts": "0", "threads": {}}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def is_self(msg):
    return (BOT_ID and msg.get("bot_id") == BOT_ID) \
        or msg.get("user") == BOT_USER_ID


def is_mention(msg):
    return MENTION_TOKEN in (msg.get("text") or "")


def main():
    state = load_state()
    state.setdefault("threads", {})
    events = []

    # 1. Top-level channel history since watermark.
    history = call("conversations.history", {
        "channel": CHANNEL,
        "oldest": state["channel_last_ts"],
        "limit": 50,
    })
    if not history.get("ok"):
        print(json.dumps({"ok": False, "error": history.get("error"),
                          "step": "history"}))
        return 1
    messages = history.get("messages", [])
    max_ts = state["channel_last_ts"]
    for msg in messages:
        ts = msg.get("ts", "0")
        if ts > max_ts:
            max_ts = ts
        # Track any thread that appears so future scans cover it.
        thread_ts = msg.get("thread_ts")
        if thread_ts and thread_ts not in state["threads"]:
            state["threads"][thread_ts] = "0"
        if ts <= state["channel_last_ts"]:
            continue
        if is_self(msg):
            continue
        if is_mention(msg):
            events.append({"scope": "channel", "ts": ts,
                           "user": msg.get("user"),
                           "text": msg.get("text")})
    state["channel_last_ts"] = max_ts

    # 1b. Thread discovery: a parent seen before it had replies never
    # got registered (it carries no thread_ts itself). Re-scan recent
    # history for any message with replies and register its ts as the
    # thread id, seeded at the parent ts so only later replies count.
    recent = call("conversations.history",
                  {"channel": CHANNEL, "limit": 20})
    if not recent.get("ok"):
        print(json.dumps({"ok": False, "error": recent.get("error"),
                          "step": "history:discover"}))
        return 1
    for msg in recent.get("messages", []):
        pts = msg.get("ts", "0")
        if msg.get("reply_count", 0) > 0 and pts not in state["threads"]:
            state["threads"][pts] = pts

    # 2. Tracked threads (history alone misses mentions in older
    # threads). Optionally scan only a rotating batch per run.
    thread_ids = list(state["threads"].keys())
    if THREAD_BATCH and thread_ids:
        cursor = int(state.get("thread_cursor", 0)) % len(thread_ids)
        batch = [thread_ids[(cursor + i) % len(thread_ids)]
                 for i in range(min(THREAD_BATCH, len(thread_ids)))]
        state["thread_cursor"] = (cursor + len(batch)) % len(thread_ids)
    else:
        batch = thread_ids
    for thread_ts in batch:
        replies = call("conversations.replies", {
            "channel": CHANNEL,
            "ts": thread_ts,
            "oldest": state["threads"][thread_ts],
            "limit": 50,
        })
        if not replies.get("ok"):
            print(json.dumps({"ok": False, "error": replies.get("error"),
                              "step": f"replies:{thread_ts}"}))
            return 1
        tmax = state["threads"][thread_ts]
        for msg in replies.get("messages", []):
            ts = msg.get("ts", "0")
            if ts > tmax:
                tmax = ts
            if ts <= state["threads"][thread_ts]:
                continue
            if is_self(msg):
                continue
            if is_mention(msg):
                events.append({"scope": "thread",
                               "thread_ts": thread_ts,
                               "ts": ts, "user": msg.get("user"),
                               "text": msg.get("text")})
        state["threads"][thread_ts] = tmax

    save_state(state)
    print(json.dumps({"ok": True, "new_mentions": events,
                      "channel_last_ts": state["channel_last_ts"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
