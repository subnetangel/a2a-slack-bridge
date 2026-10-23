# Agent runners: plugging your agent into the daemon

The daemon never talks to an agent directly. On an admitted mention it
renders a **brief** (see `brief.example.md`) and runs a configured
**runner command** whose job is to execute one headless agent session
against that brief. The session reads the brief, does the work, and
must finish by writing two files the daemon understands:

- optionally, `state/sessions/<ts>.reply.txt` — the exact reply text,
  when a reply is owed;
- always, `state/sessions/<ts>.outcome.json` —
  `{"outcome": "reply_queued" | "skipped" | "flagged" | "no_reply",
    "reply_ts": null, "note": "<one short line>"}`.

The daemon posts the reply file itself, exactly once. If no outcome
file appears, the daemon escalates the mention (see the README's
failure modes), so a runner that crashes loudly is safe, and a runner
that hangs is bounded by `session_timeout_seconds`.

## Runner configuration

Configured in `config.json` under `runners`:

```json
"runners": {
  "primary": {
    "name": "muse",
    "model": "your-primary-model-id",
    "command": ["muse", "exec", "--prompt-file", "{brief}",
                "--model", "{model}", "--trust-workspace",
                "--disable-approval"]
  },
  "fallback": { "...": "same shape, different name/model" }
}
```

Command template placeholders:

| Placeholder | Expands to |
| --- | --- |
| `{brief}` | Path to the rendered brief file |
| `{model}` | The runner's `model` value |
| `{workspace}` | `workspace_root` from config |
| `{prompt}` | The brief file's **contents** (for CLIs that take a prompt argument instead of a prompt file) |

Optional per-runner keys: `cwd` (default `workspace_root`) and `env`
(extra environment variables for the session).

## Quota fallback contract

If a session ends without an outcome file and its log shows a quota /
rate-limit failure, the daemon marks that runner blocked until the
reset time named in the error and retries the same brief once on the
fallback runner. Blocked runners are skipped on later mentions until
the reset passes; the primary then resumes automatically. Detection
looks for the markers in `daemon.py` (`QUOTA_MARKERS`) plus a
`resets at <ISO time>` phrase; if your CLI words its quota errors
differently, extend the markers. Note that a fallback only helps when
it draws on a *different* quota or limit than the primary; two models
on one account share the account's exhaustion.

## Known adapter points

These are starting templates, verified against the CLIs' documented
headless modes. Test yours in shadow mode and with a manual run
before going live; CLI flags change between versions.

### Meta Muse CLI

The reference setup this daemon was generalized from:

```json
{"name": "muse", "model": "<model id>",
 "command": ["muse", "exec", "--prompt-file", "{brief}",
             "--model", "{model}", "--trust-workspace",
             "--disable-approval"]}
```

### Anthropic Claude CLI

```json
{"name": "claude", "model": "<model id>",
 "command": ["claude", "-p", "{prompt}", "--model", "{model}",
             "--dangerously-skip-permissions"]}
```

`{prompt}` inlines the whole brief; for very long briefs prefer a
wrapper script that pipes the brief file to the CLI's stdin instead.

### A generic shell command

Anything executable works, including your own script:

```json
{"name": "my-agent", "model": "",
 "command": ["/path/to/run-agent-session.sh", "{brief}"]}
```

Your script receives the brief path, runs your agent however it runs,
and writes the reply/outcome files named inside the brief. This is
the right shape for agents without a headless CLI.

### OpenAI "dot" agents and Grok-based bots (adapter points)

Some agents do not expose a local headless CLI at all: they are
hosted (a ChatGPT-style "dot") or run on someone else's runtime (a
Grok bot wired through another platform). There is no honest generic
command for those, so this repo does not ship fake ones. The adapter
points are:

1. **If the platform offers any local entry point** (a CLI, an API
   your machine can call, a webhook you control), write a thin shell
   script around it and use the generic-shell template above. The
   script's contract is only: brief path in, reply/outcome files out.
2. **If the agent can only be reached inside Slack itself**, the
   pattern inverts: the hosted agent keeps its own receiving path,
   and this daemon's role shrinks to detection and escalation — or
   the hosted agent becomes a *peer*: it posts in the channel under
   its own bot, the daemon's agent answers it, and it is admitted in
   `admitted_senders` with role `peer` (which is exactly how
   peer agents participate in the reference deployment).

What does not change in any variant: the brief contract, the
reply-file posting discipline, the dedupe watermark, the peer thread
budget, and the escalation record when a mention gets no answer.

## Writing your own runner checklist

- It runs unattended: no interactive prompts, no approval dialogs
  (that is what flags like `--disable-approval` are for).
- It finishes by writing the outcome file, including on failure
  (`"no_reply"` with the reason in the note).
- It never posts to Slack itself; it writes the reply file.
- It respects the brief's rules: in-thread, short, no secrets, no
  consequential actions on its own authority.
- It exits when done; a hung runner is killed at the session timeout
  and its mention escalated.
