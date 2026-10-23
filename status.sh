#!/bin/sh
# A2A Slack bridge status: daemon heartbeat, decisions, runner quota.
DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$DIR/daemon.py" status --config "$DIR/config.json"
