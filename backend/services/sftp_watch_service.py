"""SSH/SFTP helpers for realtime campaign watch directories."""

import stat
import threading
import time

import paramiko

_observed_files = {}
_observation_lock = threading.Lock()
SFTP_OPERATION_TIMEOUT = 5


def _client(config: dict) -> paramiko.SSHClient:
    host = (config.get("sftp_host") or "").strip()
    username = (config.get("sftp_username") or "").strip()
    remote_dir = (config.get("watch_dir") or "").strip()
    password = config.get("sftp_password") or ""
    if not host or not username or not remote_dir or not password:
        raise ValueError("SFTP host, username, password, and remote directory are required")

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect(
            hostname=host,
            port=int(config.get("sftp_port") or 22),
            username=username,
            password=password,
            allow_agent=False,
            look_for_keys=False,
            timeout=5,
            banner_timeout=5,
            auth_timeout=5,
        )
        return client
    except Exception:
        client.close()
        raise


def test_connection(config: dict) -> dict:
    """Validate SSH host key, authentication, and remote directory access."""
    client = _client(config)
    try:
        sftp = client.open_sftp()
        sftp.get_channel().settimeout(SFTP_OPERATION_TIMEOUT)
        try:
            attrs = sftp.stat((config.get("watch_dir") or "").strip())
            if not stat.S_ISDIR(attrs.st_mode):
                raise ValueError("Configured remote path is not a directory")
            entries = sftp.listdir_attr((config.get("watch_dir") or "").strip())
        finally:
            sftp.close()
        return {"success": True, "directory": config["watch_dir"], "entry_count": len(entries)}
    finally:
        client.close()


def list_directory(config: dict):
    """Return the open SFTP client and directory attributes to avoid reconnecting."""
    client = _client(config)
    try:
        sftp = client.open_sftp()
        sftp.get_channel().settimeout(SFTP_OPERATION_TIMEOUT)
        attrs = sftp.listdir_attr((config.get("watch_dir") or "").strip())
        return client, sftp, attrs
    except Exception:
        client.close()
        raise


def file_is_stable(campaign_id: int, remote_path: str, attrs, stable_seconds: int = 10) -> bool:
    """Require unchanged size and mtime across polls before a remote download."""
    now = time.monotonic()
    key = (int(campaign_id), remote_path)
    fingerprint = (int(attrs.st_size or 0), int(attrs.st_mtime or 0))
    with _observation_lock:
        # Bound process-local state when remote directories contain old files.
        for old_key, (_, observed_at) in list(_observed_files.items()):
            if now - observed_at > 86400:
                _observed_files.pop(old_key, None)
        previous = _observed_files.get(key)
        if not previous or previous[0] != fingerprint:
            _observed_files[key] = (fingerprint, now)
            return False
        return now - previous[1] >= stable_seconds
