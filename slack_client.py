#!/usr/bin/env python3
"""Minimal Slack Web API client for the A2A Slack bridge (stdlib only).

Token resolution order (the token value is NEVER stored in this repo):
  1. Environment variable named by config `slack_token_env`
     (default: SLACK_BOT_TOKEN).
  2. The file named by config `slack_token_file`
     (default: ~/.config/a2a-slack-bridge/slack-token).
     Keep that file chmod 600. It should contain only the bot token
     (an "xoxb-..." value) and a trailing newline.

The daemon and the poll adapter both import `call` from here. There is
also a tiny CLI, handy for setup checks and for a woken session that
needs read-only thread context:

    python3 slack_client.py auth-test
    python3 slack_client.py call conversations.replies '{"channel": "C0EXAMPLE", "ts": "1700000000.000000"}'
"""
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = "https://slack.com/api/"
DEFAULT_TOKEN_ENV = "SLACK_BOT_TOKEN"
DEFAULT_TOKEN_FILE = "~/.config/a2a-slack-bridge/slack-token"


def _token_file_from_config():
    """Best-effort: if a config.json sits next to this file, honor its
    slack_token_file / slack_token_env settings."""
    cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "config.json")
    try:
        with open(cfg_path) as f:
            cfg = json.load(f)
    except (OSError, ValueError):
        return None, None
    return cfg.get("slack_token_env"), cfg.get("slack_token_file")


def get_token():
    cfg_env, cfg_file = _token_file_from_config()
    env_name = os.environ.get("A2A_SLACK_TOKEN_ENV") or cfg_env \
        or DEFAULT_TOKEN_ENV
    token = os.environ.get(env_name)
    if token:
        return token.strip()
    file_path = os.environ.get("A2A_SLACK_TOKEN_FILE") or cfg_file \
        or DEFAULT_TOKEN_FILE
    file_path = os.path.expanduser(file_path)
    if not os.path.isabs(file_path):
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), file_path)
    with open(file_path) as f:
        return f.read().strip()


def call(method, params=None, http_method="GET"):
    """Call one Slack Web API method. Returns the parsed JSON dict.

    params values must be JSON scalars (Slack rejects nested objects in
    most read methods; that matches how the daemon uses this client).
    """
    params = params or {}
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    if http_method.upper() == "GET":
        qs = urllib.parse.urlencode(params)
        url = BASE + method + ("?" + qs if qs else "")
        request = urllib.request.Request(url, method="GET", headers=headers)
    else:
        body = json.dumps(params).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
        request = urllib.request.Request(BASE + method, data=body,
                                         headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def main(argv):
    if len(argv) >= 2 and argv[1] == "auth-test":
        print(json.dumps(call("auth.test"), indent=2))
        return 0
    if len(argv) >= 3 and argv[1] == "call":
        params = json.loads(argv[3]) if len(argv) > 3 else {}
        method = "POST" if argv[2] == "chat.postMessage" else "GET"
        print(json.dumps(call(argv[2], params, http_method=method),
                         indent=2))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
