from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

try:  # pragma: no cover
    import boto3  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip().strip('"').strip("'")


def _env_bool(name: str) -> bool:
    value = _env(name).lower()
    return value in {"1", "true", "yes", "y", "on"}


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _timestamp() -> str:
    return _now_utc().strftime("%Y%m%d-%H%M%S")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _mask_db_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        if not parsed.scheme:
            return "***"
        username = parsed.username or ""
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        dbname = (parsed.path or "").lstrip("/")
        user_part = f"{username}@" if username else ""
        query = f"?{parsed.query}" if parsed.query else ""
        return f"{parsed.scheme}://{user_part}{host}{port}/{dbname}{query}"
    except Exception:
        return "***"


def _build_pg_dump_target(database_url: str) -> tuple[str, str]:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError("Unsupported DATABASE_URL scheme (expected postgresql://...)")
    password = unquote(parsed.password or "")
    username = unquote(parsed.username or "")
    host = parsed.hostname or ""
    port = parsed.port or 5432
    dbname = (parsed.path or "").lstrip("/")
    if not host or not username or not dbname:
        raise RuntimeError("Invalid DATABASE_URL (missing host/user/db)")
    query = f"?{parsed.query}" if parsed.query else ""
    safe_url = f"{parsed.scheme}://{username}@{host}:{port}/{dbname}{query}"
    return safe_url, password


def _require_pg_dump() -> str:
    from shutil import which

    pg_dump_path = which("pg_dump")
    if not pg_dump_path:
        raise RuntimeError("pg_dump not found (install postgresql-client in the image)")
    return pg_dump_path


def _run_pg_dump(*, database_url: str, out_path: Path) -> dict[str, Any]:
    pg_dump = _require_pg_dump()
    safe_url, password = _build_pg_dump_target(database_url)

    cmd = [
        pg_dump,
        "--format=custom",
        "--no-owner",
        "--no-acl",
        "--dbname",
        safe_url,
        "--file",
        str(out_path),
    ]
    env = dict(os.environ)
    if password:
        env["PGPASSWORD"] = password
    env.setdefault("PGCONNECT_TIMEOUT", "15")
    env.setdefault("PGSSLMODE", "require")

    started = _now_utc()
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    finished = _now_utc()
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(f"pg_dump failed (code={proc.returncode}) stdout={stdout[:400]} stderr={stderr[:400]}")
    return {
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "seconds": max(0.0, (finished - started).total_seconds()),
        "masked_db": _mask_db_url(database_url),
    }


def _make_tar_gz(*, source_dir: Path, out_path: Path) -> dict[str, Any]:
    if not source_dir.exists() or not source_dir.is_dir():
        raise RuntimeError(f"Data dir not found: {source_dir}")
    started = _now_utc()
    with tarfile.open(str(out_path), "w:gz") as tar:
        tar.add(str(source_dir), arcname=source_dir.name)
    finished = _now_utc()
    return {
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "seconds": max(0.0, (finished - started).total_seconds()),
        "source_dir": str(source_dir),
    }


def _s3_client() -> Any:
    if boto3 is None:
        raise RuntimeError("boto3 not installed")
    kwargs: dict[str, Any] = {}
    region = _env("AWS_REGION")
    endpoint = _env("AWS_S3_ENDPOINT_URL")
    if region:
        kwargs["region_name"] = region
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def _s3_put_file(*, client: Any, bucket: str, key: str, path: Path, content_type: str = "") -> None:
    extra: dict[str, Any] = {}
    if content_type:
        extra["ContentType"] = content_type
    client.upload_file(str(path), bucket, key, ExtraArgs=extra if extra else None)


def _backup_s3_prefix() -> str:
    prefix = _env("BACKUP_S3_PREFIX", "backups").strip().strip("/")
    return prefix or "backups"


def _backup_env_slug() -> str:
    return (
        _env("BACKUP_ENV")
        or _env("SENTRY_ENVIRONMENT")
        or _env("RENDER_SERVICE_NAME")
        or _env("APP_NAME").lower().replace(" ", "-")
        or "prod"
    )


def _retention_days() -> int:
    raw = _env("BACKUP_RETENTION_DAYS", "")
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except Exception:
        return 0


def _cleanup_old_backups(*, client: Any, bucket: str, prefix: str, retention_days: int) -> int:
    if retention_days <= 0:
        return 0
    cutoff = _now_utc() - dt.timedelta(days=int(retention_days))
    token: str | None = None
    to_delete: list[dict[str, str]] = []
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            key = str(item.get("Key") or "").strip()
            last_modified = item.get("LastModified")
            if not key or not last_modified:
                continue
            try:
                if isinstance(last_modified, dt.datetime):
                    last_modified_utc = last_modified.astimezone(dt.timezone.utc)
                else:
                    continue
            except Exception:
                continue
            if last_modified_utc < cutoff:
                to_delete.append({"Key": key})
        if not resp.get("IsTruncated"):
            break
        token = str(resp.get("NextContinuationToken") or "").strip() or None
        if not token:
            break

    deleted = 0
    for idx in range(0, len(to_delete), 1000):
        chunk = to_delete[idx : idx + 1000]
        if not chunk:
            continue
        client.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
        deleted += len(chunk)
    return deleted


def main() -> int:
    database_url = _env("DATABASE_URL")
    if not database_url:
        print("Missing DATABASE_URL", file=sys.stderr)
        return 2

    bucket = _env("S3_BUCKET_NAME")
    if not bucket:
        print("Missing S3_BUCKET_NAME (required for backups upload)", file=sys.stderr)
        return 2

    data_dir = Path(_env("SEO_AGENT_DATA_DIR", "/var/data/data")).expanduser()
    if not data_dir.is_absolute():
        data_dir = Path("/app") / data_dir

    include_data_dir = not _env_bool("BACKUP_SKIP_DATA_DIR")
    include_runs_dir = _env_bool("BACKUP_INCLUDE_RUNS_DIR")
    runs_dir = Path(_env("SEO_AGENT_RUNS_DIR", "/var/data/seo-runs")).expanduser()
    if not runs_dir.is_absolute():
        runs_dir = Path("/app") / runs_dir

    backup_prefix = _backup_s3_prefix()
    env_slug = _backup_env_slug()
    stamp = _timestamp()
    prefix = f"{backup_prefix}/{env_slug}".strip("/")

    print(f"[BACKUP] start ts={stamp} bucket={bucket} prefix={prefix}", flush=True)

    client = _s3_client()
    manifest: dict[str, Any] = {
        "ts": stamp,
        "now_utc": _now_utc().isoformat(),
        "bucket": bucket,
        "prefix": prefix,
        "database": {"masked_url": _mask_db_url(database_url)},
        "files": [],
    }

    with tempfile.TemporaryDirectory(prefix="seo-agent-backup-") as tmp_dir:
        tmp = Path(tmp_dir)

        # 1) Database
        db_path = tmp / f"db-{stamp}.dump"
        db_meta = _run_pg_dump(database_url=database_url, out_path=db_path)
        db_key = f"{prefix}/db-{stamp}.dump"
        _s3_put_file(client=client, bucket=bucket, key=db_key, path=db_path, content_type="application/octet-stream")
        manifest["files"].append(
            {
                "type": "postgres_dump_custom",
                "key": db_key,
                "bytes": db_path.stat().st_size,
                "sha256": _sha256_file(db_path),
                "meta": db_meta,
            }
        )
        print(f"[BACKUP] uploaded db key={db_key} bytes={db_path.stat().st_size}", flush=True)

        # 2) Data dir (small but useful: OAuth caches, local state)
        if include_data_dir:
            data_path = tmp / f"data-{stamp}.tar.gz"
            data_meta = _make_tar_gz(source_dir=data_dir, out_path=data_path)
            data_key = f"{prefix}/data-{stamp}.tar.gz"
            _s3_put_file(client=client, bucket=bucket, key=data_key, path=data_path, content_type="application/gzip")
            manifest["files"].append(
                {
                    "type": "tar_gz",
                    "key": data_key,
                    "bytes": data_path.stat().st_size,
                    "sha256": _sha256_file(data_path),
                    "meta": data_meta,
                }
            )
            print(f"[BACKUP] uploaded data key={data_key} bytes={data_path.stat().st_size}", flush=True)

        # 3) Runs dir (optional, can be large)
        if include_runs_dir:
            runs_path = tmp / f"seo-runs-{stamp}.tar.gz"
            runs_meta = _make_tar_gz(source_dir=runs_dir, out_path=runs_path)
            runs_key = f"{prefix}/seo-runs-{stamp}.tar.gz"
            _s3_put_file(client=client, bucket=bucket, key=runs_key, path=runs_path, content_type="application/gzip")
            manifest["files"].append(
                {
                    "type": "tar_gz",
                    "key": runs_key,
                    "bytes": runs_path.stat().st_size,
                    "sha256": _sha256_file(runs_path),
                    "meta": runs_meta,
                }
            )
            print(f"[BACKUP] uploaded runs key={runs_key} bytes={runs_path.stat().st_size}", flush=True)

        # 4) Manifest
        manifest_path = tmp / f"manifest-{stamp}.json"
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest_key = f"{prefix}/manifest-{stamp}.json"
        _s3_put_file(client=client, bucket=bucket, key=manifest_key, path=manifest_path, content_type="application/json")
        print(f"[BACKUP] uploaded manifest key={manifest_key}", flush=True)

    # 5) Retention cleanup (optional)
    retention_days = _retention_days()
    if retention_days > 0:
        try:
            deleted = _cleanup_old_backups(
                client=client, bucket=bucket, prefix=f"{prefix}/", retention_days=retention_days
            )
            if deleted:
                print(f"[BACKUP] retention deleted={deleted} days={retention_days}", flush=True)
        except Exception as exc:
            print(f"[BACKUP] retention cleanup error: {type(exc).__name__}: {exc}", flush=True)

    print("[BACKUP] done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
