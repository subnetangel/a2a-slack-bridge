#!/usr/bin/env python3
"""Install the A2A Slack bridge systemd unit (system manager).

Reads a2a-slack-bridge.service.tmpl and config.json (for the service
name), injects paths and the proxy environment from the CURRENT
process environment (values are proxy endpoints, not credentials;
only key names are printed), writes
/etc/systemd/system/<service>.service, then daemon-reload + enable +
restart, and prints a short status excerpt.

Run as root:  sudo python3 install_service.py   (or plain, as root)
Idempotent: re-running rewrites the unit and restarts the service.

Environment knobs:
  A2A_SERVICE_NAME   override the service name (default: config
                     `service_name`, else "a2a-slack-bridge")
  A2A_SERVICE_USER   user the service runs as (default: root; the
                     daemon needs read access to the token file and
                     write access to the install dir's state/)
  A2A_PYTHON         python3 binary for ExecStart (default: the
                     interpreter running this installer)
NO_PROXY is sanitized to localhost,127.0.0.1: some sandbox
environments ship a NO_PROXY value that crashes HTTP clients in
child sessions; the sanitized value is safe everywhere.
"""
import json
import os
import subprocess
import sys

DAEMON_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(DAEMON_DIR, "a2a-slack-bridge.service.tmpl")
PROXY_KEYS = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy"]


def service_name():
    if os.environ.get("A2A_SERVICE_NAME"):
        return os.environ["A2A_SERVICE_NAME"]
    try:
        with open(os.path.join(DAEMON_DIR, "config.json")) as f:
            return json.load(f).get("service_name") or "a2a-slack-bridge"
    except (OSError, ValueError):
        return "a2a-slack-bridge"


def build_unit(name):
    with open(TEMPLATE) as f:
        unit = f.read()
    lines = []
    for key in PROXY_KEYS:
        val = os.environ.get(key)
        if val:
            lines.append(f"Environment={key}={val}")
            print(f"  env injected: {key} (value not printed)")
    lines.append("Environment=NO_PROXY=localhost,127.0.0.1")
    lines.append("Environment=no_proxy=localhost,127.0.0.1")
    print("  env injected: NO_PROXY/no_proxy (sanitized)")
    run_user = os.environ.get("A2A_SERVICE_USER", "root")
    home = os.path.expanduser(f"~{run_user}") if run_user != "root" \
        else "/root"
    unit = (unit.replace("__PROXY_ENV_LINES__", "\n".join(lines))
                .replace("__INSTALL_DIR__", DAEMON_DIR)
                .replace("__SERVICE_NAME__", name)
                .replace("__RUN_USER__", run_user)
                .replace("__HOME_DIR__", home)
                .replace("__PYTHON3__",
                         os.environ.get("A2A_PYTHON", sys.executable)))
    return unit


def run(*cmd):
    print("+", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        print(proc.stderr.rstrip(), file=sys.stderr)
        raise SystemExit(
            f"command failed: {' '.join(cmd)} rc={proc.returncode}")


def main():
    if os.geteuid() != 0:
        raise SystemExit("must run as root (system manager unit install)")
    name = service_name()
    unit_path = f"/etc/systemd/system/{name}.service"
    unit = build_unit(name)
    with open(unit_path, "w") as f:
        f.write(unit)
    os.chmod(unit_path, 0o644)
    print(f"wrote {unit_path}")
    run("systemctl", "daemon-reload")
    run("systemctl", "enable", name)
    run("systemctl", "restart", name)
    run("systemctl", "is-active", name)
    run("systemctl", "status", name, "--no-pager", "-n", "5")


if __name__ == "__main__":
    main()
