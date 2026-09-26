#!/usr/bin/env python3
"""Create a systemd unit so the backend runs as a managed background service.

Intended for the manual deployment layout (system Python + run.sh), where the
service otherwise runs in the foreground or under nohup. The generated unit:

* starts uvicorn directly; ``run.sh`` is only the bootstrap/fallback launcher;
* reads the existing ``marketing-copilot.env`` so paths stay in one place;
* uses ``KillMode=process`` so the online-update runner survives
  ``systemctl stop`` during a self-update;
* optionally installs a polkit rule letting the service account manage its own
  unit, and flips ``MC_UPDATE_MODE`` to ``systemd`` in the env file.

Run as root on the target:

    sudo /opt/marketing-copilot-manual/venv/bin/python -m tools.install_systemd_service

Review without changing anything (no root required):

    python -m tools.install_systemd_service --dry-run
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

UNIT_TEMPLATE = """\
[Unit]
Description={description}
After=network.target

[Service]
Type=simple
User={user}
Group={group}
WorkingDirectory={working_dir}
EnvironmentFile={env_file}
Environment=PYTHONUNBUFFERED=1
Environment=MC_SERVICE_UNIT={unit_name}
Environment=MC_RUN_UNDER_SYSTEMD=1
ExecStart={exec_start}
Restart=always
RestartSec=5
TimeoutStopSec=30
KillSignal=SIGTERM
# Only the main process is signalled on stop so the detached update runner
# (spawned by the web process) can finish the directory swap before restart.
KillMode=process
UMask=0027
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true

[Install]
WantedBy=multi-user.target
"""

POLKIT_TEMPLATE = """\
// Allow {user} to manage its own {unit_name} unit (needed for online updates).
polkit.addRule(function (action, subject) {{
  if (
    action.id === "org.freedesktop.systemd1.manage-units" &&
    action.lookup("unit") === "{unit_name}" &&
    subject.user === "{user}"
  ) {{
    return polkit.Result.YES;
  }}
}});
"""


def app_root() -> Path:
    return Path(__file__).resolve().parents[1]


def project_root() -> Path:
    return app_root().parent


def read_env_value(env_file: Path, key: str, default: str = "") -> str:
    if not env_file.is_file():
        return default
    try:
        for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return default


def normalize_env_paths(env_file: Path, root: Path) -> None:
    """Expand deployment-relative APP_HOME paths for direct systemd execution."""
    if not env_file.is_file():
        return
    original = env_file.read_text(encoding="utf-8", errors="replace")
    app_home = str(root)
    normalized = original.replace("${APP_HOME}", app_home).replace("$APP_HOME", app_home)
    if normalized == original:
        return
    backup = env_file.with_suffix(env_file.suffix + ".paths.bak")
    if not backup.exists():
        shutil.copy2(env_file, backup)
    env_file.write_text(normalized, encoding="utf-8")
    print(f"expanded APP_HOME paths in {env_file} (backup: {backup})")


def find_python(root: Path) -> Path:
    for rel in (
        "venv/bin/python",
        ".venv/bin/python",
        "venv/Scripts/python.exe",
        ".venv/Scripts/python.exe",
    ):
        candidate = root / rel
        if candidate.is_file():
            return candidate
    return root / "venv/bin/python"


def build_exec_start(args, root: Path) -> str:
    if args.exec_start:
        return args.exec_start
    python = find_python(root)
    env_path = Path(args.env_file) if args.env_file else Path()
    host = args.host or read_env_value(env_path, "MC_HOST", "0.0.0.0")
    port = args.port or read_env_value(env_path, "MC_PORT", "19090")
    return f"{python} -m uvicorn main:app --host {host} --port {port} --workers 1"


def build_unit(args, root: Path) -> str:
    unit_name = args.service_name if args.service_name.endswith(".service") else args.service_name + ".service"
    exec_start = quote_exec_start(build_exec_start(args, root))
    return UNIT_TEMPLATE.format(
        description=args.description,
        user=args.user,
        group=args.group or args.user,
        working_dir=app_root(),
        env_file=args.env_file,
        unit_name=unit_name,
        exec_start=exec_start,
    )


def quote_exec_start(command: str) -> str:
    """Quote systemd ExecStart tokens: only tokens containing whitespace get quotes."""
    return " ".join(
        f'"{token}"' if any(ch.isspace() for ch in token) else token
        for token in command.split()
    )


def set_env_value(env_file: Path, key: str, value: str) -> None:
    if not env_file.is_file():
        raise RuntimeError(f"environment file not found: {env_file}")
    lines = env_file.read_text(encoding="utf-8", errors="replace").splitlines()
    out = []
    replaced = False
    for line in lines:
        if line.strip().startswith(f"{key}="):
            out.append(f"{key}={value}")
            replaced = True
        else:
            out.append(line)
    if not replaced:
        out.append(f"{key}={value}")
    backup = env_file.with_suffix(env_file.suffix + ".bak")
    shutil.copy2(env_file, backup)
    env_file.write_text("\n".join(out) + "\n", encoding="utf-8")


def install_polkit(user: str, unit_name: str, service_name: str) -> None:
    rules_dir = Path("/etc/polkit-1/rules.d")
    if not rules_dir.is_dir():
        return
    rule_path = rules_dir / f"60-{service_name}.rules"
    rule_path.write_text(POLKIT_TEMPLATE.format(user=user, unit_name=unit_name), encoding="utf-8")
    os.chmod(rule_path, 0o644)


def prepare_dirs(root: Path, user: str) -> None:
    for sub in ("data", "logs", "run", "backups", "updates"):
        target = root / sub
        target.mkdir(parents=True, exist_ok=True)
        if os.name == "posix":
            os.chown(target, _uid(user), _gid(user))


def _uid(user: str) -> int:
    import pwd
    return pwd.getpwnam(user).pw_uid


def _gid(user: str) -> int:
    import grp
    try:
        return grp.getgrnam(user).gr_gid
    except KeyError:
        import pwd
        return pwd.getpwnam(user).pw_gid


def _run(cmd: list) -> None:
    import subprocess
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}\n{(proc.stdout or '') + (proc.stderr or '')}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Install a systemd unit for the Marketing Copilot backend.")
    parser.add_argument("--service-name", default="marketing-copilot.service", help="unit file name (default: marketing-copilot.service)")
    parser.add_argument("--description", default="Marketing Copilot backend", help="systemd unit description")
    parser.add_argument("--user", default="marketing-copilot", help="service account (default: marketing-copilot)")
    parser.add_argument("--group", default="", help="service group (default: same as user)")
    parser.add_argument("--env-file", default="", help="environment file (default: <app root>/marketing-copilot.env)")
    parser.add_argument("--exec-start", default="", help="override ExecStart (e.g. full path to run.sh or uvicorn)")
    parser.add_argument("--host", default="", help="listen host when starting uvicorn directly (default: from env or 0.0.0.0)")
    parser.add_argument("--port", default="", help="listen port when starting uvicorn directly (default: from env or 19090)")
    parser.add_argument("--update-mode", choices=("systemd", "manual", ""), default="systemd",
                        help="MC_UPDATE_MODE written into the env file; empty keeps the current value")
    parser.add_argument("--no-polkit", action="store_true", help="skip the polkit rule for self start/stop")
    parser.add_argument("--no-start", action="store_true", help="install the unit without starting it")
    parser.add_argument("--no-enable", action="store_true", help="install the unit without enabling it at boot")
    parser.add_argument("--force", action="store_true", help="overwrite an existing unit file")
    parser.add_argument("--dry-run", action="store_true", help="print the generated unit and exit without changing anything")
    args = parser.parse_args()

    root = project_root()
    env_file = Path(args.env_file) if args.env_file else root / "marketing-copilot.env"
    args.env_file = str(env_file)
    unit_name = args.service_name if args.service_name.endswith(".service") else args.service_name + ".service"
    if not args.dry_run:
        normalize_env_paths(env_file, root)
    if not env_file.is_file():
        print(f"WARNING: environment file not found: {env_file}", file=sys.stderr)

    unit = build_unit(args, root)
    if args.dry_run:
        print(unit)
        if env_file.is_file() and args.update_mode:
            print(f"# would set {env_file}: MC_UPDATE_MODE={args.update_mode}", file=sys.stderr)
        if not args.no_polkit:
            print("# would install polkit rule for user=%s unit=%s" % (args.user, unit_name), file=sys.stderr)
        return 0

    if os.name != "posix" or not os.path.exists("/etc/systemd/system"):
        print("systemd is not available here; use --dry-run to preview the unit.", file=sys.stderr)
        return 2
    if os.geteuid() != 0:
        print("run this tool as root (sudo ... python -m tools.install_systemd_service)", file=sys.stderr)
        return 2

    unit_path = Path("/etc/systemd/system") / unit_name
    if unit_path.exists() and not args.force:
        raise SystemExit(f"unit already exists: {unit_path} (use --force to overwrite)")

    if args.update_mode:
        set_env_value(env_file, "MC_UPDATE_MODE", args.update_mode)
        print(f"updated {env_file}: MC_UPDATE_MODE={args.update_mode} (backup: {env_file}.bak)")
    prepare_dirs(root, args.user)
    unit_path.write_text(unit, encoding="utf-8")
    os.chmod(unit_path, 0o644)
    if not args.no_polkit:
        install_polkit(args.user, unit_name, args.service_name)
    _run(["systemctl", "daemon-reload"])
    if not args.no_enable:
        _run(["systemctl", "enable", unit_name])
    if not args.no_start:
        _run(["systemctl", "start", unit_name])
    print(f"installed {unit_path}")
    states = []
    if not args.no_start:
        states.append("started")
    if not args.no_enable:
        states.append("enabled at boot")
    print("service " + (" and ".join(states) if states else "installed (not started, not enabled at boot)"))
    print("verify with: systemctl status %s" % unit_name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
