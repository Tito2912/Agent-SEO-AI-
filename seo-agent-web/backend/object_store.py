from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any

try:  # pragma: no cover - optional dependency in local dev
    import boto3  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore

    class ClientError(Exception):
        pass


def _clean_env(name: str) -> str:
    return str(os.environ.get(name) or "").strip()


def _default_runs_root() -> Path:
    raw = _clean_env("SEO_AGENT_RUNS_DIR")
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path(__file__).resolve().parents[2] / "seo-runs").resolve()


def _storage_root(runs_dir: Path) -> Path:
    default_root = _default_runs_root()
    try:
        runs_dir.resolve().relative_to(default_root)
        return default_root
    except Exception:
        return runs_dir.resolve()


def s3_bucket_name() -> str:
    return _clean_env("S3_BUCKET_NAME")


def s3_prefix() -> str:
    raw = _clean_env("S3_PREFIX") or "seo-runs"
    parts = [part for part in raw.strip("/").split("/") if part]
    return "/".join(parts)


def s3_enabled() -> bool:
    return bool(s3_bucket_name()) and boto3 is not None


def s3_available_reason() -> str | None:
    if not s3_bucket_name():
        return "missing_bucket"
    if boto3 is None:
        return "missing_boto3"
    return None


@lru_cache(maxsize=1)
def _s3_client() -> Any | None:
    if not s3_enabled():
        return None
    kwargs: dict[str, Any] = {}
    region = _clean_env("AWS_REGION")
    endpoint = _clean_env("AWS_S3_ENDPOINT_URL")
    if region:
        kwargs["region_name"] = region
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)  # type: ignore[union-attr]


def _relative_under_runs(runs_dir: Path, path: Path) -> Path | None:
    storage_root = _storage_root(runs_dir)
    try:
        return path.resolve().relative_to(storage_root)
    except Exception:
        return None


def _key_for_relative(relative_path: Path) -> str:
    rel = PurePosixPath(*relative_path.parts).as_posix() if relative_path.parts else ""
    prefix = s3_prefix()
    if prefix and rel:
        return f"{prefix}/{rel}"
    return prefix or rel


def _prefix_for_relative_dir(relative_dir: Path) -> str:
    key = _key_for_relative(relative_dir)
    return f"{key.rstrip('/')}/" if key else ""


def _iter_object_keys(*, prefix: str) -> list[str]:
    client = _s3_client()
    bucket = s3_bucket_name()
    if client is None or not bucket:
        return []
    token: str | None = None
    keys: list[str] = []
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("Contents") or []:
            key = str(item.get("Key") or "").strip()
            if key:
                keys.append(key)
        if not resp.get("IsTruncated"):
            break
        token = str(resp.get("NextContinuationToken") or "").strip() or None
        if not token:
            break
    return keys


def list_runs_subdirs(runs_dir: Path, path: Path) -> list[str]:
    relative_dir = _relative_under_runs(runs_dir, path)
    if relative_dir is None:
        return []
    client = _s3_client()
    bucket = s3_bucket_name()
    prefix = _prefix_for_relative_dir(relative_dir)
    if client is None or not bucket or not prefix:
        return []
    token: str | None = None
    names: set[str] = set()
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for item in resp.get("CommonPrefixes") or []:
            child_prefix = str(item.get("Prefix") or "").strip()
            if not child_prefix.startswith(prefix):
                continue
            name = child_prefix[len(prefix) :].strip("/")
            if name:
                names.add(name)
        if not resp.get("IsTruncated"):
            break
        token = str(resp.get("NextContinuationToken") or "").strip() or None
        if not token:
            break
    return sorted(names)


def upload_runs_path(runs_dir: Path, path: Path) -> int:
    relative = _relative_under_runs(runs_dir, path)
    client = _s3_client()
    bucket = s3_bucket_name()
    if relative is None or client is None or not bucket:
        return 0
    uploaded = 0
    path = path.resolve()
    if path.is_file():
        key = _key_for_relative(relative)
        client.upload_file(str(path), bucket, key)
        return 1
    if not path.exists() or not path.is_dir():
        return 0
    for file_path in path.rglob("*"):
        if not file_path.is_file():
            continue
        rel = _relative_under_runs(runs_dir, file_path)
        if rel is None:
            continue
        client.upload_file(str(file_path), bucket, _key_for_relative(rel))
        uploaded += 1
    return uploaded


def restore_runs_file(runs_dir: Path, path: Path) -> bool:
    relative = _relative_under_runs(runs_dir, path)
    client = _s3_client()
    bucket = s3_bucket_name()
    if relative is None or client is None or not bucket:
        return False
    if path.exists() and path.is_file():
        return True
    key = _key_for_relative(relative)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(path))
        return True
    except ClientError:
        return False
    except Exception:
        return False


def restore_runs_tree(runs_dir: Path, path: Path) -> bool:
    relative = _relative_under_runs(runs_dir, path)
    client = _s3_client()
    bucket = s3_bucket_name()
    if relative is None or client is None or not bucket:
        return False
    prefix = _prefix_for_relative_dir(relative)
    if not prefix:
        return False
    keys = _iter_object_keys(prefix=prefix)
    if not keys:
        return False
    restored = False
    base_prefix = s3_prefix().strip("/")
    for key in keys:
        rel_key = key
        if base_prefix:
            if not key.startswith(f"{base_prefix}/"):
                continue
            rel_key = key[len(base_prefix) + 1 :]
        rel_path = Path(PurePosixPath(rel_key))
        target = (runs_dir / rel_path).resolve()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(target))
            restored = True
        except Exception:
            continue
    return restored


def delete_runs_path(runs_dir: Path, path: Path, *, recursive: bool = False) -> int:
    relative = _relative_under_runs(runs_dir, path)
    client = _s3_client()
    bucket = s3_bucket_name()
    if relative is None or client is None or not bucket:
        return 0
    deleted = 0
    if recursive:
        prefix = _prefix_for_relative_dir(relative)
        keys = _iter_object_keys(prefix=prefix)
        if not keys:
            return 0
        for idx in range(0, len(keys), 1000):
            chunk = keys[idx : idx + 1000]
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
            )
            deleted += len(chunk)
        return deleted
    key = _key_for_relative(relative)
    try:
        client.delete_object(Bucket=bucket, Key=key)
        deleted = 1
    except Exception:
        deleted = 0
    return deleted
