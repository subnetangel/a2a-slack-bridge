#!/usr/bin/env python3
"""A2A Slack bridge wake daemon.

Watches one Slack channel for real mentions of your agent's bot and,
for each admitted mention, launches one headless agent session to
answer it in-thread. Detection latency is a few seconds.

Design notes (each of these was learned the hard way):

  * One consumer rule: never run two live consumers (pollers and/or
    event receivers) for the same bot/channel pair at once. Parallel
    consumers create duplicate processing and reply loops.
  * Shadow mode first: with config mode "shadow" the daemon detects,
    dedupes, decides, and logs what it WOULD do, but launches nothing
    and posts nothing. Flip to "live" only after an end-to-end check.
  * The daemon makes no content judgments. It detects, dedupes,
    decides admission by deterministic rules, launches the session,
    and records what happened. Everything the session does is governed
    by the brief template (brief.md, rendered per mention).
  * Sessions never post to Slack themselves. A session that owes a
    reply writes its exact text to state/sessions/<ts>.reply.txt and
    records outcome "reply_queued"; after the session exits, the
    daemon posts the file in-thread itself, renames it to
    <ts>.reply.txt.posted (the at-most-once marker, durable across
    restarts), and records the posted ts in the outcome file.
  * Runner fallback and escalation: a session runs on the primary
    runner from config. If it fails with a subscription quota /
    rate-limit error, the daemon records that runner as blocked until
    the reset time named in the error (state in quota_state_file) and
    retries the same brief once on the fallback runner. Blocked
    runners are skipped until their reset passes, then the primary
    resumes automatically. Any session that still produces no outcome
    (all runners blocked or failed, timeout, crash) is escalated: the
    daemon writes an escalation record (escalations_dir/<ts>.json) so
    a supervisor hook or a human can answer the mention. A failed
    mention is never just logged and dropped.
  * Admission is by Slack user ID, from config `admitted_senders`.
    Senders have a role: "human" (uncapped) or "peer" (another agent;
    capped per thread by `peer_thread_budget`, an anti-loop guard,
    not an authorization gate).

Everything site-specific lives in config.json (see
config.example.json). This file contains no channel IDs, user IDs,
tokens, or product names.

Stdlib only. State lives under the state paths in config.json:
heartbeat.json (rewritten every loop), dedupe.json, the poll
watermark, per-mention briefs/logs/outcomes under sessions/.

Subcommands:
  daemon.py run [--config PATH]     run the daemon (default)
  daemon.py status [--config PATH]  print config + heartbeat + liveness
  daemon.py set-mode shadow|live    edit config.json mode (restart applies)
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

DAEMON_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(DAEMON_DIR, "config.json")

LOG_MAX_BYTES = 5_000_000

# Config keys whose values are filesystem paths. Relative values are
# resolved against the daemon directory, so a checkout can live
# anywhere and config.example.json stays portable.
PATH_KEYS = (
    "brief_template_path", "dedupe_file", "heartbeat_file", "lock_file",
    "log_file", "poll_adapter_path", "poll_state_file",
    "poll_state_seed_from", "quota_state_file", "escalations_dir",
    "sessions_dir", "slack_token_file",
)


def now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def now_ts():
    return time.time()


# ------------------------------------------------------- quota detection

QUOTA_MARKERS = ("rate_limit_error", "quota exhausted",
                 "subscription quota", "rate limit exceeded",
                 "usage limit", "429")
RESET_RE = re.compile(r"resets at ([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z)",
                      re.IGNORECASE)


def parse_iso_utc(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def detect_quota_reset(log_text):
    """Classify a finished session's log. Returns the reset instant
    (ISO string) if the log shows a quota / rate-limit failure, '' if
    it shows one without a parseable reset time, or None if it shows
    no quota failure. Marker wording varies by agent CLI; extend
    QUOTA_MARKERS for your runner if its errors read differently."""
    if not log_text:
        return None
    low = log_text.lower()
    quota = any(m in low for m in QUOTA_MARKERS)
    if not quota:
        return None
    m = RESET_RE.search(log_text)
    return m.group(1) if m else ""


# ---------------------------------------------------------------- state io

def atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


class Logger:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def log(self, msg):
        line = f"{now_iso()} {msg}\n"
        try:
            if os.path.exists(self.path) \
                    and os.path.getsize(self.path) > LOG_MAX_BYTES:
                os.replace(self.path, self.path + ".1")
        except OSError:
            pass
        with open(self.path, "a") as f:
            f.write(line)
        # Also stdout: under systemd this lands in the journal.
        sys.stdout.write(line)
        sys.stdout.flush()


# ------------------------------------------------------------- slack access

sys.path.insert(0, DAEMON_DIR)
import slack_client  # noqa: E402


def slack_post_message(channel, thread_ts, text):
    """Post one message in-thread as the bot. Returns Slack's
    response dict. The token comes from slack_client's env/file
    resolution; it never appears in this code or in config.json."""
    return slack_client.call(
        "chat.postMessage",
        {"channel": channel, "thread_ts": thread_ts, "text": text},
        http_method="POST")


# ---------------------------------------------------------------- adapters

class SlackPollAdapter:
    """Runs poll_adapter.py as a subprocess each loop.

    The adapter keeps the daemon's OWN watermark (env A2A_POLL_STATE),
    so a second poller (for example a cron-based one) never shares or
    advances this daemon's state. On first ever run the daemon state
    may be seeded from a read-only copy of an existing watermark file
    (config poll_state_seed_from), so cutover does not replay history.
    """

    name = "slack_poll"

    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.script = cfg["poll_adapter_path"]
        self.state_file = cfg["poll_state_file"]
        self.seed_from = cfg.get("poll_state_seed_from")

    def ensure_seeded(self):
        if os.path.exists(self.state_file):
            return
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        if self.seed_from and os.path.exists(self.seed_from):
            shutil.copyfile(self.seed_from, self.state_file)
            seed = load_json(self.state_file, {})
            self.logger.log(
                f"SEED poll state copied from {self.seed_from} "
                f"channel_last_ts={seed.get('channel_last_ts')}")
        else:
            atomic_write_json(self.state_file,
                              {"channel_last_ts": "0", "threads": {}})
            self.logger.log(
                "SEED poll state initialized empty (no seed source)")

    def detect(self):
        env = dict(os.environ)
        env["A2A_POLL_STATE"] = self.state_file
        env["A2A_CONFIG"] = self.cfg["_config_path"]
        env["A2A_THREAD_BATCH"] = str(self.cfg.get("thread_batch", 0))
        try:
            proc = subprocess.run(
                [sys.executable, self.script],
                capture_output=True, text=True, timeout=120, env=env)
        except subprocess.TimeoutExpired:
            return None, {"error": "poll_timeout", "step": "adapter"}
        out = (proc.stdout or "").strip().splitlines()
        if not out:
            return None, {"error": f"empty_output rc={proc.returncode}",
                          "step": "adapter",
                          "stderr_tail": (proc.stderr or "")[-200:]}
        try:
            result = json.loads(out[-1])
        except ValueError:
            return None, {"error": "unparseable_output", "step": "adapter",
                          "stdout_tail": out[-1][-200:]}
        if not result.get("ok"):
            return None, result
        return result.get("new_mentions", []), None


class QueueAdapter:
    """Stub for an event-driven receiver source.

    The polling source is the proven default. If you build an
    event-driven receiver instead (Slack Events API over HTTPS, or
    Socket Mode), implement detect() to return the same event shape
    the poll adapter emits:

        {"scope": "channel"|"thread", "ts": ..., "thread_ts": ...,
         "user": ..., "text": ...}

    Until then this adapter is deliberately inert: it detects nothing
    and says so at startup. Never run a poller and an event receiver
    for the same bot/channel at the same time.
    """

    name = "queue"

    def __init__(self, cfg, logger):
        self.logger = logger
        logger.log("QUEUE adapter selected: STUB ONLY (no receiver "
                   "implemented). No events will be detected.")

    def ensure_seeded(self):
        pass

    def detect(self):
        return [], None


# ---------------------------------------------------------------- decider

def admitted_map(cfg):
    """user_id -> {"name": ..., "role": "human"|"peer"} from config."""
    out = {}
    for entry in cfg.get("admitted_senders", []):
        out[entry["user_id"]] = {
            "name": entry.get("name", entry["user_id"]),
            "role": entry.get("role", "peer"),
        }
    return out


def self_ids(cfg):
    ids = {cfg.get("bot_user_id")}
    if cfg.get("bot_id"):
        ids.add(cfg["bot_id"])
    return {i for i in ids if i}


def decide(event, dedupe, cfg):
    """Pure decision for one detected event. Returns (decision, root_ts).

    Rules, in order:
      - never handle our own posts (skip_self)
      - durable dedupe on message ts (skip_duplicate)
      - admitted senders only (ignore_ineligible_sender)
      - peer budget: at most cfg peer_thread_budget admitted mentions
        per peer sender per root thread, as an anti-loop guard only
        (skip_peer_budget); humans are uncapped
      - otherwise admit
    Content judgments (pure acknowledgments, quotes, approval claims)
    are made by the woken session, per the brief; the daemon admits.
    """
    admitted = admitted_map(cfg)
    ts = event.get("ts", "")
    sender = event.get("user")
    root = event.get("thread_ts") or ts
    if sender in self_ids(cfg):
        return "skip_self", root
    if ts in dedupe["processed"]:
        return "skip_duplicate", root
    if sender not in admitted:
        return "ignore_ineligible_sender", root
    if admitted[sender]["role"] == "peer":
        peer_key = f"{sender}:{root}"
        uses = dedupe["peer_roots"].count(peer_key)
        if uses >= int(cfg.get("peer_thread_budget", 5)):
            return "skip_peer_budget", root
    return "admit", root


def record_decision(dedupe, event, decision, root, cfg):
    ts = event.get("ts", "")
    dedupe["processed"][ts] = {"decision": decision, "at": now_iso()}
    # Bound the store: keep the most recent 500 entries.
    if len(dedupe["processed"]) > 500:
        keys = list(dedupe["processed"].keys())
        for k in keys[:len(keys) - 500]:
            del dedupe["processed"][k]
    info = admitted_map(cfg).get(event.get("user"))
    if decision == "admit" and info and info["role"] == "peer":
        dedupe["peer_roots"].append(f"{event.get('user')}:{root}")


# ---------------------------------------------------------------- launcher

class Launcher:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.sessions_dir = cfg["sessions_dir"]
        os.makedirs(self.sessions_dir, exist_ok=True)
        with open(cfg["brief_template_path"]) as f:
            self.template = f.read()

    def reply_path(self, ts):
        return os.path.join(self.sessions_dir, f"{ts}.reply.txt")

    def _sender_info(self, event):
        return admitted_map(self.cfg).get(
            event.get("user"),
            {"name": str(event.get("user")), "role": "unknown"})

    def _admitted_list_text(self):
        lines = []
        for entry in self.cfg.get("admitted_senders", []):
            lines.append(f"- {entry.get('name', entry['user_id'])} "
                         f"({entry.get('role', 'peer')}, "
                         f"Slack ID {entry['user_id']})")
        return "\n".join(lines) if lines else "- (none configured)"

    def write_brief(self, event, root):
        ts = event.get("ts", "")
        info = self._sender_info(event)
        outcome_path = os.path.join(self.sessions_dir, f"{ts}.outcome.json")
        brief = (self.template
                 .replace("__AGENT_NAME__",
                          self.cfg.get("agent_name", "the agent"))
                 .replace("__WORKSPACE_NAME__",
                          self.cfg.get("workspace_name", "the workspace"))
                 .replace("__CHANNEL_NAME__",
                          self.cfg.get("channel_name", "the channel"))
                 .replace("__CHANNEL_PURPOSE__",
                          self.cfg.get("channel_purpose",
                                       "team collaboration"))
                 .replace("__CHANNEL__", self.cfg["channel_id"])
                 .replace("__ADMITTED_LIST__", self._admitted_list_text())
                 .replace("__SENDER_ID__", str(event.get("user")))
                 .replace("__SENDER_NAME__", info["name"])
                 .replace("__SENDER_KIND__", info["role"])
                 .replace("__EVENT_TS__", ts)
                 .replace("__THREAD_TS__", root)
                 .replace("__SCOPE__", str(event.get("scope", "channel")))
                 .replace("__EVENT_TEXT__", (event.get("text") or "")[:4000])
                 .replace("__OUTCOME_PATH__", outcome_path)
                 .replace("__REPLY_PATH__", self.reply_path(ts)))
        brief_path = os.path.join(self.sessions_dir, f"{ts}.brief.md")
        with open(brief_path, "w") as f:
            f.write(brief)
        return brief_path, outcome_path

    # ------------------------------------------------ runner quota state

    def _quota_state(self):
        path = self.cfg.get("quota_state_file")
        if not path:
            return {"runners": {}}
        return load_json(path, {"runners": {}})

    def _blocked_until(self, runner_name):
        """Datetime until which `runner_name` is quota-blocked, or None."""
        info = self._quota_state().get("runners", {}).get(runner_name)
        if not info:
            return None
        until = parse_iso_utc(info.get("blocked_until", ""))
        if until is None or until <= datetime.now(timezone.utc):
            return None
        return until

    def _mark_quota_blocked(self, runner_name, reset_iso, ts):
        path = self.cfg.get("quota_state_file")
        if not path:
            return
        until = parse_iso_utc(reset_iso) if reset_iso else None
        if until is None:
            until = datetime.now(timezone.utc) + timedelta(hours=1)
            reset_iso = until.strftime("%Y-%m-%dT%H:%M:%SZ")
        state = self._quota_state()
        state.setdefault("runners", {})[runner_name] = {
            "blocked_until": reset_iso, "marked_at": now_iso(),
            "event_ts": ts}
        atomic_write_json(path, state)
        self.logger.log(
            f"RUNNER QUOTA runner={runner_name} "
            f"blocked_until={reset_iso} ts={ts}")

    # ------------------------------------------------ session execution

    def _runner_argv(self, runner, brief_path):
        """Expand a runner's command template. Placeholders:
        {brief} = brief file path, {model} = runner's model id,
        {workspace} = workspace_root, {prompt} = brief file CONTENTS
        (for CLIs that take the prompt as an argument instead of a
        prompt file). Returns None if the runner is unconfigured."""
        cmd = runner.get("command")
        if not cmd:
            return None
        prompt_text = None
        argv = []
        for part in cmd:
            if "{prompt}" in part:
                if prompt_text is None:
                    with open(brief_path) as f:
                        prompt_text = f.read()
                part = part.replace("{prompt}", prompt_text)
            part = (part.replace("{brief}", brief_path)
                        .replace("{model}", runner.get("model", ""))
                        .replace("{workspace}",
                                 self.cfg.get("workspace_root", ".")))
            argv.append(part)
        return argv

    def _exec_session(self, ts, brief_path, runner, log_path):
        """Run one headless session on `runner`. Returns the exit code
        (int), 'timeout', or 'unconfigured'; the session log lands in
        log_path."""
        argv = self._runner_argv(runner, brief_path)
        if argv is None:
            with open(log_path, "w") as logf:
                logf.write(f"runner {runner.get('name')!r} has no "
                           f"command configured\n")
            return "unconfigured"
        env = dict(os.environ)
        if self.cfg.get("sanitize_no_proxy", True):
            # Some sandboxes ship a NO_PROXY value that crashes HTTP
            # clients in child sessions; the loopback-only value is
            # safe everywhere. See README "Setup from scratch".
            env["NO_PROXY"] = "localhost,127.0.0.1"
            env["no_proxy"] = "localhost,127.0.0.1"
        env.update(runner.get("env", {}))
        cwd = runner.get("cwd") or self.cfg.get("workspace_root", ".")
        timeout = int(self.cfg.get("session_timeout_seconds", 600))
        self.logger.log(
            f"LAUNCH session ts={ts} runner={runner.get('name')} "
            f"model={runner.get('model', '')} brief={brief_path} "
            f"timeout={timeout}s")
        with open(log_path, "w") as logf:
            proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=logf,
                                    stderr=subprocess.STDOUT,
                                    start_new_session=True)
            try:
                return proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.log(
                    f"SESSION TIMEOUT ts={ts} "
                    f"runner={runner.get('name')} after {timeout}s; "
                    f"killing process group")
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=10)
                except Exception:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except Exception:
                        pass
                return "timeout"

    def escalate(self, event, root, brief_path, outcome_path,
                 reason, detail):
        """Hand a mention the daemon could not answer to a human or a
        supervising agent session.

        Writes escalations_dir/<ts>.json (a supervisor hook can watch
        that directory and wake the main agent to answer the mention
        itself; see README "Failure modes") and records outcome
        'escalated'. The mention is never silently dropped.
        """
        ts = event.get("ts", "")
        info = self._sender_info(event)
        esc_dir = self.cfg.get("escalations_dir")
        esc_file = os.path.join(esc_dir, f"{ts}.json") if esc_dir else None
        record = {
            "ts": ts,
            "channel": self.cfg["channel_id"],
            "sender": event.get("user"),
            "sender_name": info["name"],
            "sender_kind": info["role"],
            "thread_ts": root,
            "scope": event.get("scope", "channel"),
            "text": (event.get("text") or "")[:4000],
            "reason": reason,
            "detail": detail[:500],
            "brief_path": brief_path,
            "at": now_iso(),
        }
        if esc_dir:
            os.makedirs(esc_dir, exist_ok=True)
            atomic_write_json(esc_file, record)
        atomic_write_json(outcome_path, {
            "outcome": "escalated", "reason": reason,
            "note": detail[:500], "escalation_file": esc_file,
            "at": now_iso()})
        self.logger.log(f"ESCALATE ts={ts} reason={reason} "
                        f"detail={detail[:200]}")

    def run_session(self, event, root):
        """Live mode only: launch one headless session for a mention.

        Runner order: config runners.primary, then runners.fallback.
        A runner recorded as quota-blocked is skipped until its reset
        passes, so launches resume on the primary automatically once
        the quota lifts. A quota failure marks the runner and moves to
        the next one. A non-quota failure goes straight to escalation.
        If no attempt produces an outcome file, the mention is
        escalated instead of being dropped.
        """
        ts = event.get("ts", "")
        brief_path, outcome_path = self.write_brief(event, root)
        if os.path.exists(outcome_path):
            os.remove(outcome_path)
        reply_path = self.reply_path(ts)
        if os.path.exists(reply_path):
            os.remove(reply_path)
        runners_cfg = self.cfg.get("runners", {})
        order = []
        for slot in ("primary", "fallback"):
            runner = runners_cfg.get(slot)
            if not runner:
                continue
            until = self._blocked_until(runner.get("name", slot))
            if until is not None:
                self.logger.log(
                    f"RUNNER SKIP ts={ts} "
                    f"runner={runner.get('name')} quota-blocked until "
                    f"{until.strftime('%Y-%m-%dT%H:%M:%SZ')}")
            else:
                order.append(runner)
        if not order:
            self.escalate(event, root, brief_path, outcome_path, "quota",
                          "all configured runners are quota-blocked")
            return {"outcome": "escalated"}
        started = now_ts()
        last_reason = "session_failure"
        last_detail = ""
        for attempt, runner in enumerate(order):
            log_path = os.path.join(
                self.sessions_dir,
                f"{ts}.session.log" if attempt == 0
                else f"{ts}.session.fallback.log")
            rc = self._exec_session(ts, brief_path, runner, log_path)
            dur = round(now_ts() - started, 1)
            outcome = load_json(outcome_path, None)
            if outcome is not None:
                self.logger.log(
                    f"SESSION DONE ts={ts} rc={rc} dur={dur}s "
                    f"runner={runner.get('name')} "
                    f"outcome={outcome.get('outcome')} "
                    f"note={outcome.get('note')}")
                self.post_queued_reply(event, root, outcome_path, outcome)
                return outcome
            try:
                with open(log_path) as f:
                    log_text = f.read()
            except OSError:
                log_text = ""
            lines = log_text.strip().splitlines()
            tail = lines[-1][-200:] if lines else ""
            reset = detect_quota_reset(log_text)
            if reset is not None and rc != "unconfigured":
                self._mark_quota_blocked(runner.get("name"), reset, ts)
                last_reason = "quota"
                last_detail = f"runner={runner.get('name')} rc={rc} " \
                              f"log_tail={tail}"
                self.logger.log(
                    f"RUNNER FALLBACK ts={ts} "
                    f"failed_runner={runner.get('name')} rc={rc} "
                    f"reason=quota detail={tail}")
                continue
            last_reason = "session_failure"
            last_detail = f"runner={runner.get('name')} rc={rc} " \
                          f"log_tail={tail}"
            self.logger.log(
                f"SESSION FAILED ts={ts} runner={runner.get('name')} "
                f"rc={rc} dur={dur}s no outcome file; log_tail={tail}")
            break
        self.escalate(event, root, brief_path, outcome_path,
                      last_reason, last_detail)
        return {"outcome": "escalated"}

    def post_queued_reply(self, event, root, outcome_path, outcome):
        """Post a session's queued reply file, exactly once.

        The session writes sessions/<ts>.reply.txt when a reply is
        owed. The daemon posts it in-thread via slack_post_message,
        renames the file to <ts>.reply.txt.posted (the marker that
        makes reposting impossible, including across daemon restarts),
        and records the posted ts in the outcome file. A failed post is
        logged and annotated in the outcome file; the reply file is
        left un-renamed so the failure stays visible, and it is never
        retried blind.
        """
        ts = event.get("ts", "")
        reply_path = self.reply_path(ts)
        posted_path = reply_path + ".posted"
        if os.path.exists(posted_path):
            self.logger.log(
                f"REPLY SKIP ts={ts} already posted (marker present)")
            return
        if not os.path.exists(reply_path):
            if outcome.get("outcome") == "reply_queued":
                self.logger.log(
                    f"REPLY MISSING ts={ts} outcome=reply_queued but "
                    f"no reply file")
            return
        try:
            with open(reply_path) as f:
                text = f.read().strip()
        except OSError as exc:
            self.logger.log(f"REPLY READ FAILED ts={ts}: {exc!r}")
            return
        if not text:
            self.logger.log(f"REPLY EMPTY ts={ts}; nothing to post")
            return
        try:
            result = slack_post_message(self.cfg["channel_id"], root, text)
        except Exception as exc:  # never let a post failure kill the loop
            result = {"ok": False, "error": f"exception: {exc!r}"}
        if result.get("ok"):
            posted_ts = result.get("ts")
            os.replace(reply_path, posted_path)
            outcome["reply_ts"] = posted_ts
            outcome["reply_posted_at"] = now_iso()
            atomic_write_json(outcome_path, outcome)
            self.logger.log(
                f"REPLY POSTED ts={ts} reply_ts={posted_ts} thread={root}")
        else:
            outcome["reply_post_failed"] = str(result.get("error"))
            outcome["reply_post_attempted_at"] = now_iso()
            atomic_write_json(outcome_path, outcome)
            self.logger.log(
                f"REPLY POST FAILED ts={ts} error={result.get('error')}")

    def simulate(self, event, root):
        """Shadow mode: build the brief, record a simulated outcome,
        launch nothing, post nothing."""
        ts = event.get("ts", "")
        brief_path, outcome_path = self.write_brief(event, root)
        atomic_write_json(outcome_path, {
            "outcome": "shadow_simulated",
            "reply_ts": None,
            "note": "shadow mode: brief written, no session launched, "
                    "nothing posted",
            "brief": brief_path,
            "at": now_iso(),
        })
        self.logger.log(f"SHADOW would_launch ts={ts} brief={brief_path}")
        return {"outcome": "shadow_simulated"}


# ---------------------------------------------------------------- daemon

class Daemon:
    def __init__(self, cfg):
        self.cfg = cfg
        self.logger = Logger(cfg["log_file"])
        self.mode = cfg.get("mode", "shadow")
        source = cfg.get("source", "slack_poll")
        if source == "queue":
            self.adapter = QueueAdapter(cfg, self.logger)
        else:
            self.adapter = SlackPollAdapter(cfg, self.logger)
        self.launcher = Launcher(cfg, self.logger)
        self.dedupe = load_json(cfg["dedupe_file"],
                                {"processed": {}, "peer_roots": []})
        self.stop = False
        self.loops = 0
        self.events_total = 0
        self.errors_consecutive = 0
        self.last_event = None
        self._recent_loops = []

    def handle_event(self, event):
        detected_at = now_ts()
        ts = event.get("ts", "0")
        try:
            latency = round(detected_at - float(ts), 1)
        except (TypeError, ValueError):
            latency = None
        decision, root = decide(event, self.dedupe, self.cfg)
        record_decision(self.dedupe, event, decision, root, self.cfg)
        atomic_write_json(self.cfg["dedupe_file"], self.dedupe)
        info = admitted_map(self.cfg).get(event.get("user"))
        kind = info["role"] if info else "other"
        self.logger.log(
            f"EVENT ts={ts} sender={event.get('user')} kind={kind} "
            f"scope={event.get('scope')} root={root} latency_s={latency} "
            f"decision={decision} mode={self.mode}")
        self.last_event = {"ts": ts, "decision": decision,
                           "at": now_iso()}
        if decision != "admit":
            return
        if self.mode == "live":
            self.launcher.run_session(event, root)
        else:
            self.launcher.simulate(event, root)

    def heartbeat(self, last_poll_ok, poll_ms):
        hb = {
            "pid": os.getpid(),
            "mode": self.mode,
            "source": self.adapter.name,
            "agent_name": self.cfg.get("agent_name"),
            "last_loop_at": now_iso(),
            "last_poll_ok": last_poll_ok,
            "last_poll_ms": poll_ms,
            "loops_total": self.loops,
            "events_total": self.events_total,
            "last_event": self.last_event,
            "recent_loops": self._recent_loops,
        }
        atomic_write_json(self.cfg["heartbeat_file"], hb)

    def run(self):
        self.adapter.ensure_seeded()
        self.logger.log(
            f"START pid={os.getpid()} mode={self.mode} "
            f"source={self.adapter.name} "
            f"interval={self.cfg.get('poll_interval_seconds', 5)}s "
            f"channel={self.cfg['channel_id']} "
            f"agent={self.cfg.get('agent_name')}")
        if self.mode != "live":
            self.logger.log("SHADOW MODE: detecting and logging only; "
                            "no sessions launched, nothing posted.")
        while not self.stop:
            loop_start = now_ts()
            self.loops += 1
            self._recent_loops = (self._recent_loops + [now_iso()])[-12:]
            events, error = self.adapter.detect()
            poll_ms = round((now_ts() - loop_start) * 1000)
            if error is not None and self.stop:
                # Poll subprocess was killed by the shutdown signal
                # (shared process group); not a real poll failure.
                self.logger.log(
                    "poll interrupted by shutdown; exiting loop")
                break
            if error is not None:
                self.errors_consecutive += 1
                self.logger.log(
                    f"POLL ERROR {json.dumps(error)} "
                    f"consecutive={self.errors_consecutive}")
                self.heartbeat(False, poll_ms)
                backoff = min(
                    self.cfg.get("poll_interval_seconds", 5)
                    * (2 ** self.errors_consecutive), 60)
                self._sleep(backoff)
                continue
            self.errors_consecutive = 0
            for event in events or []:
                self.events_total += 1
                try:
                    self.handle_event(event)
                except Exception as exc:  # one event must not kill the loop
                    self.logger.log(
                        f"EVENT HANDLER ERROR ts={event.get('ts')}: "
                        f"{exc!r}")
            self.heartbeat(True, poll_ms)
            if self.loops <= 25 or self.loops % 20 == 0:
                self.logger.log(
                    f"LOOP n={self.loops} ok "
                    f"events={len(events or [])} poll_ms={poll_ms}")
            elapsed = now_ts() - loop_start
            self._sleep(max(0.2, self.cfg.get(
                "poll_interval_seconds", 5) - elapsed))
        self.logger.log("STOP clean")

    def _sleep(self, seconds):
        end = now_ts() + seconds
        while not self.stop and now_ts() < end:
            time.sleep(min(0.25, end - now_ts()))


# ---------------------------------------------------------------- lock

def acquire_lock(lock_path, logger):
    if os.path.exists(lock_path):
        try:
            pid = int(open(lock_path).read().strip())
            os.kill(pid, 0)
            logger.log(f"FATAL lock held by live pid {pid}; exiting")
            return False
        except (ValueError, ProcessLookupError, OSError):
            pass  # stale lock
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    with open(lock_path, "w") as f:
        f.write(str(os.getpid()))
    return True


# ---------------------------------------------------------------- cli

def load_config(path):
    with open(path) as f:
        cfg = json.load(f)
    for key in PATH_KEYS:
        val = cfg.get(key)
        if val:
            val = os.path.expanduser(val)
            if not os.path.isabs(val):
                val = os.path.join(DAEMON_DIR, val)
            cfg[key] = val
    if cfg.get("workspace_root"):
        cfg["workspace_root"] = os.path.expanduser(cfg["workspace_root"])
    cfg["_config_path"] = os.path.abspath(path)
    return cfg


def service_name(cfg):
    return cfg.get("service_name", "a2a-slack-bridge")


def cmd_run(args):
    cfg = load_config(args.config)
    logger = Logger(cfg["log_file"])
    if not acquire_lock(cfg["lock_file"], logger):
        return 2
    daemon = Daemon(cfg)

    def on_signal(signum, frame):
        daemon.stop = True
        logger.log(f"SIGNAL {signum} received; stopping after "
                   f"current cycle")

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    try:
        daemon.run()
    finally:
        try:
            os.remove(cfg["lock_file"])
        except OSError:
            pass
    return 0


def cmd_status(args):
    cfg = load_config(args.config)
    hb = load_json(cfg["heartbeat_file"], None)
    dedupe = load_json(cfg["dedupe_file"],
                       {"processed": {}, "peer_roots": []})
    print(f"agent:           {cfg.get('agent_name')}")
    print(f"channel:         {cfg.get('channel_id')}")
    print(f"mode (config):   {cfg.get('mode')}")
    print(f"source:          {cfg.get('source')}")
    print(f"poll interval:   {cfg.get('poll_interval_seconds')}s")
    print(f"admitted:        {len(cfg.get('admitted_senders', []))} "
          f"sender(s)")
    runners = cfg.get("runners", {})
    for slot in ("primary", "fallback"):
        r = runners.get(slot)
        if r:
            print(f"runner {slot}: {r.get('name')} "
                  f"(model={r.get('model', '')})")
    if hb is None:
        print("heartbeat:       none yet (daemon has not run)")
    else:
        alive = False
        try:
            os.kill(int(hb.get("pid", 0)), 0)
            alive = True
        except (OSError, ValueError):
            pass
        print(f"daemon pid:      {hb.get('pid')} "
              f"({'alive' if alive else 'NOT running'})")
        print(f"mode (running):  {hb.get('mode')}")
        print(f"last loop:       {hb.get('last_loop_at')} "
              f"(poll ok={hb.get('last_poll_ok')}, "
              f"{hb.get('last_poll_ms')}ms)")
        print(f"loops total:     {hb.get('loops_total')}")
        print(f"events total:    {hb.get('events_total')}")
        print(f"last event:      {hb.get('last_event')}")
    print(f"dedupe store:    {len(dedupe.get('processed', {}))} "
          f"processed ts, {len(dedupe.get('peer_roots', []))} peer "
          f"thread uses admitted")
    qpath = cfg.get("quota_state_file")
    if qpath:
        qstate = load_json(qpath, {"runners": {}})
        for name, info in (qstate.get("runners") or {}).items():
            print(f"runner quota:    {name} blocked until "
                  f"{info.get('blocked_until')} "
                  f"(marked {info.get('marked_at')})")
    esc_dir = cfg.get("escalations_dir")
    if esc_dir and os.path.isdir(esc_dir):
        pending = [f for f in os.listdir(esc_dir)
                   if f.endswith(".json")]
        print(f"escalations:     {len(pending)} pending in {esc_dir}")
    svc = service_name(cfg)
    print()
    print(f"Flip to live:    python3 daemon.py set-mode live && "
          f"sudo systemctl restart {svc}")
    print(f"Back to shadow:  python3 daemon.py set-mode shadow && "
          f"sudo systemctl restart {svc}")
    return 0


def cmd_set_mode(args):
    cfg = load_config(args.config)
    cfg["mode"] = args.mode
    # Write back without the runtime-only keys load_config added.
    cfg.pop("_config_path", None)
    atomic_write_json(args.config, cfg)
    print(f"config mode -> {args.mode!r}. Restart the service to "
          f"apply: sudo systemctl restart {service_name(cfg)}")
    return 0


def extract_config(argv):
    """Pull --config <path> out of argv wherever it appears (systemd
    ExecStart and status.sh pass it after the subcommand; humans tend
    to put it first). Returns (config_path, remaining_argv)."""
    out = []
    config = DEFAULT_CONFIG
    i = 0
    while i < len(argv):
        if argv[i] == "--config" and i + 1 < len(argv):
            config = argv[i + 1]
            i += 2
        else:
            out.append(argv[i])
            i += 1
    return config, out


def main():
    config, argv = extract_config(sys.argv[1:])
    parser = argparse.ArgumentParser(
        description="A2A Slack bridge wake daemon")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("run")
    sub.add_parser("status")
    p = sub.add_parser("set-mode")
    p.add_argument("mode", choices=["shadow", "live"])
    args = parser.parse_args(argv)
    args.config = config
    if args.cmd == "status":
        return cmd_status(args)
    if args.cmd == "set-mode":
        return cmd_set_mode(args)
    return cmd_run(args)


if __name__ == "__main__":
    sys.exit(main())
