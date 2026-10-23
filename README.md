# A2A Slack bridge: a wake daemon for agent bots on Slack

Give your agent a near-instant presence in one Slack channel. When an
admitted person or peer agent really @-mentions your bot, the daemon
wakes one headless session of your agent, which reads the mention in
context and replies in-thread when a reply is warranted. Detection
latency is a few seconds, and the whole thing is plain Python (stdlib
only) plus a systemd service.

This design comes from a production agent-to-agent (A2A) setup where
several agents and their humans share one channel. The parts that are
hard to get right are the point of this repo:

- **Exactly-once replies.** Durable dedupe on message IDs, a durable
  poll watermark, and reply files the daemon posts itself and then
  marks `.posted`. Restarts and machine replacements do not re-answer
  old mentions.
- **No silent failures.** If the agent session dies on a quota or
  rate limit, the daemon retries once on a fallback runner. If the
  mention still gets no answer, it is *escalated* into a record a
  supervisor or human picks up. A mention is never just logged and
  dropped.
- **Loop safety.** Peer agents are admitted with a per-thread budget
  (an anti-loop guard), the daemon never handles its own posts, and
  sessions never reply to pure acknowledgments.
- **Any agent CLI.** The daemon launches a configured command per
  mention (Meta Muse, Anthropic Claude, your own script). See
  [docs/agent-runners.md](docs/agent-runners.md).

## Architecture and data flow

```
systemd (<service_name>.service, Restart=on-failure)
  +-- daemon.py run                    (foreground)
        loop every config.poll_interval_seconds (default 5s):
          poll_adapter.py (subprocess) -> new mention events
          decide(event)                  -> skip_self / skip_duplicate /
                                            ignore_ineligible_sender /
                                            skip_peer_budget / admit
          live mode:   one headless session per mention
                       (single-flight: the loop is sequential), on the
                       primary runner, with quota fallback and
                       escalation as described below
          shadow mode: brief written, outcome recorded, nothing runs
        heartbeat.json rewritten every loop
```

Detection (`poll_adapter.py`, a subprocess each loop) works in three
passes against the Slack API, with the bot token resolved by
`slack_client.py`:

1. **Channel history.** `conversations.history` from the daemon's own
   watermark (`channel_last_ts` in `state/poll-state.json`). New
   top-level messages containing the real mention token
   (`<@BOT_USER_ID>`) become events with scope `channel`. Mention
   matching is exact: a plain-text "@handle" is not a mention and
   fires nothing.
2. **Thread discovery.** A second history read (latest 20 messages)
   registers any message with replies as a tracked thread.
3. **Tracked-thread scans.** `conversations.replies` per tracked
   thread, from that thread's own watermark. Because scanning every
   thread every cycle would exceed Slack's Tier-3 rate limits
   (~50 req/min per method), scans are round-robin batched:
   `config.thread_batch` threads per cycle, cursor persisted in the
   state file. Every thread is revisited within a few cycles.

The watermark advances only on full success: the adapter saves state
as its last step, so a failed poll exits nonzero, saves nothing, and
the daemon retries the same range next cycle (with exponential
backoff, capped at 60s). The daemon keeps its **own** watermark;
`config.poll_state_seed_from` can seed it once from a read-only copy
of an earlier poller's state so a cutover does not replay history.

Admission is by Slack user ID, from `config.admitted_senders`. Each
entry has a `name` and a `role`: `human` (uncapped) or `peer`
(another agent; capped per root thread by `peer_thread_budget`,
default 5). The budget is an anti-loop guard, not an authorization
gate: decide carefully who is admitted, because admission is the
authorization.

**One consumer rule:** never run two live consumers (pollers, event
receivers, or one of each) for the same bot/channel pair at once.

## The wake path

On `admit` in live mode, the daemon:

1. Renders the brief: `brief.md` (copy `brief.example.md`) with the
   channel, sender, mention text (truncated at 4,000 characters),
   thread root, and the per-mention file paths substituted in. Saved
   to `state/sessions/<ts>.brief.md`.
2. Launches the primary runner's command in its own process group,
   capturing output to `state/sessions/<ts>.session.log`, and waits up
   to `config.session_timeout_seconds` (default 600s; on timeout the
   process group is terminated).
3. Reads the outcome file the session must write as its final action,
   `state/sessions/<ts>.outcome.json`:

   ```json
   {"outcome": "reply_queued" | "skipped" | "flagged" | "no_reply",
    "reply_ts": null, "note": "<one short line>"}
   ```

   `reply_queued` means the session also wrote its exact reply text
   to `state/sessions/<ts>.reply.txt`. `skipped` = no reply warranted
   (acknowledgment, self-quote). `flagged` = the session recorded an
   unsupported approval claim or a workflow-adoption request for a
   human to review (it may still queue a brief, non-committal
   acknowledgment). `no_reply` = the session could not complete.
4. Posts the queued reply itself, in-thread, via `chat.postMessage`,
   renames the reply file to `<ts>.reply.txt.posted` (the marker that
   makes reposting impossible, including across restarts), and
   records the posted ts in the outcome file. A failed post logs
   `REPLY POST FAILED`, is annotated in the outcome file, and leaves
   the reply file un-renamed so the failure stays visible; it is
   never retried blind.

Sessions never post to Slack themselves: a headless session's sandbox
may be unable to reach the API even when its work succeeded. The
reply-file handoff removes that whole class of failure.

## Resilience: fallback, escalation, supervision

Three tiers keep a mention from dying quietly:

1. **Runner fallback.** Sessions run on `runners.primary`. A quota /
   rate-limit failure (detected from the session log) records that
   runner as blocked until the reset named in the error
   (`state/runner-quota.json`) and retries the same brief once on
   `runners.fallback`. Blocked runners are skipped on later mentions;
   when the reset passes, the primary resumes automatically. A
   fallback only helps if it draws on a different quota than the
   primary.
2. **Escalation.** Any session that still produces no outcome (all
   runners blocked or failed, timeout, crash, unconfigured runner) is
   escalated: the daemon writes `state/escalations/<ts>.json` with
   the mention, sender, reason, and brief path, and records outcome
   `escalated`. Pair this with a watcher on that directory (a hook,
   cron job, or supervisor agent) that wakes your main agent to
   answer the mention itself and then moves the record to
   `state/escalations/handled/`. The daemon's job ends at writing a
   complete, actionable record; what consumes it is your choice.
3. **Supervision.** The daemon is a systemd service with
   `Restart=on-failure`. Add a watchdog: a scheduled check (every
   10-15 minutes) that reads `state/heartbeat.json`; if
   `last_loop_at` is stale, reinstall/restart the service (rerun
   `install_service.py`) and verify a fresh heartbeat. A supervisor
   hook that notices the process has stopped and reruns the installer
   covers the gap between watchdogs.

**Machine replacement.** The systemd unit file lives in ephemeral
storage on most VMs and dies with the machine; everything durable
(code, config, all of `state/`) lives in this directory, ideally on
persistent storage. A reinstalled daemon resumes from the stored
watermark and dedupe state instead of replaying history, so the net
effect of a replacement is a few minutes of latency, no state loss,
and no duplicate replies.

## Files

| File | What it is |
| --- | --- |
| `daemon.py` | The daemon. Subcommands: `run`, `status`, `set-mode shadow\|live`. Stdlib only. |
| `poll_adapter.py` | Slack mention detection (three passes, batched thread scans). Read-only, never posts. |
| `slack_client.py` | Tiny Slack Web API client + CLI (`auth-test`, `call`). Holds token resolution (env or chmod-600 file). |
| `brief.example.md` | The headless-session brief template: authorized-asker rules, reply discipline, outcome contract. Copy to `brief.md` and adapt. |
| `config.example.json` | Every setting, with placeholders. Copy to `config.json` and fill in. `config.json` is git-ignored. |
| `slack-app-manifest.example.json` | Slack app manifest with exactly the scopes the polling design needs. |
| `install_service.py` | Renders and installs the systemd unit (parameterized service name, paths, run user), enables and restarts it. Idempotent; run as root. |
| `a2a-slack-bridge.service.tmpl` | The systemd unit template the installer fills in. |
| `status.sh` | One-line wrapper around `daemon.py status`. |
| `docs/agent-runners.md` | Runner contract, quota fallback contract, and adapter templates (Muse, Claude, generic shell, hosted agents). |

State (all under `state/`, created at runtime, never committed):

| File | Holds |
| --- | --- |
| `state/poll-state.json` | The daemon's own Slack watermark: `channel_last_ts`, per-thread watermarks, thread-scan cursor. |
| `state/dedupe.json` | `processed`: message ts to decision and time (bounded to the most recent 500). `peer_roots`: admitted peer uses per thread (the loop budget). |
| `state/heartbeat.json` | pid, mode, source, last loop time, last poll ok/duration, loop and event totals, last event. Rewritten every loop. |
| `state/runner-quota.json` | Per-runner quota blocks: `blocked_until`, when marked, for which mention. |
| `state/daemon.lock` | The running daemon's pid. A second instance refuses to start if that pid is alive; a stale pid is ignored. |
| `state/logs/daemon.log` | Decisions, launches, outcomes, poll errors. Rotates at 5 MB. Message text never appears here (only in brief files). |
| `state/sessions/<ts>.*` | Per mention: `.brief.md`, `.session.log` (plus `.session.fallback.log`), `.outcome.json`, `.reply.txt` / `.reply.txt.posted`. |
| `state/escalations/<ts>.json` | Escalation records awaiting a supervisor or human. |

## Setup from scratch

You need: a Linux machine (VM is fine) with Python 3.10+, systemd,
persistent storage for this directory, and your agent's CLI installed
and signed in on that machine.

1. **Create the Slack app.** At https://api.slack.com/apps, create an
   app **from a manifest** in your workspace and paste
   `slack-app-manifest.example.json` (rename the bot to your agent).
   The scopes are the minimum the polling design uses:
   - `channels:history` — read public-channel history
     (`conversations.history`, `conversations.replies`);
   - `groups:history` — the same for a **private** channel (drop it
     if your channel is public);
   - `chat:write` — post the queued replies (`chat.postMessage`).

   No public URL, no Events API subscription, and no Socket Mode are
   needed for polling: the daemon only makes outbound API calls. (If
   you later build an event-driven receiver instead: enable Socket
   Mode in the manifest settings, add the app-level token scope
   `connections:write`, and subscribe to `app_mention` / message
   events. Never run the receiver and the poller at the same time.)
2. **Install the app** to the workspace and copy the **Bot User OAuth
   Token** (`xoxb-...`).
3. **Store the token — never in this repo.** Either export it as an
   environment variable for the service, or write it to a file only
   the service user can read:

   ```sh
   mkdir -p ~/.config/a2a-slack-bridge
   printf '%s' 'xoxb-your-token' > ~/.config/a2a-slack-bridge/slack-token
   chmod 600 ~/.config/a2a-slack-bridge/slack-token
   ```

   `slack_client.py` resolves the token from the env var named by
   config `slack_token_env` (default `SLACK_BOT_TOKEN`) first, then
   the file named by `slack_token_file` (default the path above).
   Verify: `python3 slack_client.py auth-test` should print your
   bot's user ID — you need it for the next step.
4. **Invite the bot** to the channel (`/invite @YourBot` in the
   channel). A bot cannot read a channel it has not joined.
5. **Collect the IDs.** Channel ID: open the channel details in Slack
   (it is the `C...` string). User IDs: each person's/agent's profile
   ("Copy member ID"). Bot ID (`B...`): shown by `auth-test` output
   or the app page; it is used only for self-filtering and may be
   left out if unknown.
6. **Configure.** `cp config.example.json config.json`, then fill in:
   `agent_name`, `workspace_name`, `channel_id`, `channel_name`,
   `bot_user_id`, `admitted_senders` (start with yourself as `human`
   and one peer if you have one), and `runners` (see
   [docs/agent-runners.md](docs/agent-runners.md)). Relative paths in
   config resolve against this directory. Keep `mode: "shadow"` for
   now. Also `cp brief.example.md brief.md` and adapt the wording;
   the `__PLACEHOLDER__` tokens must stay.
7. **Install the service** as root:

   ```sh
   sudo python3 install_service.py
   ```

   The installer renders the unit with your install path, service
   name, and run user (`A2A_SERVICE_USER` if the daemon should not
   run as root), injects proxy environment variables from your shell
   if present, and restarts the service.
8. **Verify in shadow mode** (checklist below): mentions from
   admitted senders show as `SHADOW would_launch` in the log and
   nothing is posted.
9. **Go live.** `python3 daemon.py set-mode live`, restart the
   service, and run the end-to-end check: an admitted sender posts a
   real mention; the daemon log shows `EVENT ... decision=admit`,
   then `LAUNCH`, then `SESSION DONE`, and — when a reply is
   warranted — `REPLY POSTED`, with the reply visible in-thread.

To stop: `sudo systemctl stop <service_name>`; add `disable` to keep
it off across restarts. To run without systemd, `python3 daemon.py
run` works in the foreground (you lose restart supervision — pair it
with the heartbeat watchdog).

## Verification checklist

After any install, reinstall, or change:

1. `systemctl is-active <service_name>` prints `active`.
2. `./status.sh` shows the daemon pid alive and the intended mode.
3. `state/heartbeat.json`: `last_loop_at` within the last few
   seconds, `last_poll_ok` true, and `loops_total` increases between
   two reads about 10 seconds apart.
4. `state/logs/daemon.log` tail shows `LOOP ... ok` lines and no
   repeating `POLL ERROR`.
5. Shadow mode: an admitted sender's real mention produces an `EVENT`
   line with `decision=admit` and a `SHADOW would_launch` line;
   nothing appears in the channel.
6. Live mode end-to-end: the full chain from the setup step 9, plus
   `state/sessions/<ts>.reply.txt.posted` exists and the outcome
   file's `reply_ts` is set.

## Failure modes

- **Poll errors and backoff.** A failed Slack API call makes the
  adapter exit nonzero without advancing the watermark; the daemon
  logs `POLL ERROR` and backs off exponentially (cap 60s). A run of
  poll errors with a fresh heartbeat means Slack or network trouble,
  not lost mentions: the watermark only moves on success.
- **Session sandbox failures.** A headless session's shell sandbox
  can fail to start (for example a mount error in the session log).
  The session then cannot do the work; by design it writes outcome
  `no_reply` with the failure in the note, or dies without an outcome
  — which the daemon escalates. The daemon itself is unaffected and
  keeps polling.
- **In-session posting fails — designed out.** Sessions never post;
  the daemon posts the reply file. If a reply seems missing, look for
  a leftover `.reply.txt` with no `.posted` marker and a
  `reply_post_failed` note in the outcome file, not for a session-side
  post attempt.
- **Quota / rate-limit exhaustion.** Covered by the fallback +
  escalation tiers above. The incident that motivated them: a mention
  woke the daemon on time, the session died in seconds on a
  subscription quota error, and the old code logged it and moved on —
  nobody knew until the human asked why the agent was silent.
- **Flagged sessions wait for a human.** Outcome `flagged` means the
  session recorded an approval claim or workflow-adoption request.
  Flags surface through your main agent session; check recent
  `flagged` entries in `state/sessions/*.outcome.json` if one seems
  to sit unanswered.
- **Non-mention posts are not this daemon's job.** The daemon wakes
  only on the real mention token. If you also need untagged requests
  noticed, run a separate periodic channel watch that surfaces them
  to your main session — do not widen this daemon.
- **Duplicate daemon.** The lock file holds the running pid; a second
  instance logs `FATAL lock held by live pid` and exits.
- **Stale heartbeat.** The daemon died or is wedged: restart the
  service (or rerun the installer) and verify a fresh heartbeat.

## Making changes

- **Admit a new sender.** Add their Slack user ID to
  `admitted_senders` in `config.json` with the right role, restart
  the service. Admission is a human owner's decision; announce it in
  the channel so the team knows who the agent will answer.
- **Change the peer budget.** Edit `peer_thread_budget` in
  `config.json` and restart. Remember it is a loop guard, not an
  authorization control; do not use it to silence a member.
- **Change session behavior.** Edit the rules in `brief.md`. The
  daemon reads the template at startup, so a restart applies it. Keep
  rule 5's scope intact: questions are answered, action-approval
  claims are flagged.
- **Change cadence or timeouts.** Edit `poll_interval_seconds`,
  `session_timeout_seconds`, or `thread_batch` in `config.json` and
  restart. Mind the rate-limit reasoning before raising
  `thread_batch` or shortening the interval.

## Security notes

- The bot token never lives in this repo: not in code, not in config
  (config.json is git-ignored; commit only the example), not in logs
  or briefs. Store it as an env var or a chmod-600 file outside the
  repo, exactly as in setup step 3.
- Each agent gets its **own** Slack app and token. Never share one
  agent's token with another agent or paste it into a chat.
- Admission is the authorization boundary. Anyone in
  `admitted_senders` can wake your agent and get answers in the
  channel; the brief's rules are what keep woken sessions from taking
  consequential actions without a human's approval. Review the brief
  before going live, and keep the flagging rule.
- Request only the scopes listed in setup step 1. If Slack or your
  receiving design asks for more, decide explicitly before broadening.
- Sessions run with your agent CLI's unattended mode on your machine,
  under your account. Run the daemon (and therefore the sessions) as
  the least-privileged user that can read the token file and execute
  the agent CLI.
- Rotate the bot token in the Slack app settings if it is ever
  exposed; update the token file and restart. Nothing else changes.

## License

MIT (see [LICENSE](LICENSE)).
