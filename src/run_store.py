"""Versioned, credential-free run bundles. Stages are write-once."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA_VERSION = 1
ROOT = Path(__file__).resolve().parent.parent


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def default_run_dir():
    """A unique bundle directory, used when the caller gives no --run-dir."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return ROOT / "reports" / (stamp + "-" + uuid.uuid4().hex[:8])


def provenance():
    def git(*args):
        try:
            return subprocess.check_output(
                ["git", *args], cwd=ROOT, stderr=subprocess.DEVNULL, text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            return "unavailable"

    versions = {}
    for name in ("boto3", "botocore", "aioboto3", "PyYAML"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = "unavailable"
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        memory = None
    if memory is None and platform.system() == "Darwin":
        try:
            memory = int(
                subprocess.check_output(
                    ["/usr/sbin/sysctl", "-n", "hw.memsize"], text=True
                )
            )
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
    # Include source fingerprints even for uncommitted or downloaded code.
    source_files = [ROOT / "run_certification.py", *sorted((ROOT / "src").glob("*.py"))]
    source_hash = hashlib.sha256(
        "".join(digest(p) for p in source_files).encode()
    ).hexdigest()
    return {
        "commit": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain")),
        "source_sha256": source_hash,
        "python": platform.python_version(),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory,
        "dependencies": versions,
    }


def public_config(cfg):
    # An explicit allowlist prevents future credentials from leaking via asdict().
    return {
        name: getattr(cfg, name)
        for name in (
            "endpoint_url",
            "region",
            "force_path_style",
            "disable_multi_object_delete",
            "disable_multipart_upload",
            "checksum_algorithm",
            "region_override",
            "max_concurrency",
            "verify_tls",
        )
    }


class RunSession:
    def __init__(self, args, config):
        self.args, self.config = args, config
        self.stage = args.cmd
        self.path = Path(args.run_dir) if args.run_dir else default_run_dir()
        self.lock = None

    def __enter__(self):
        url = urlsplit(self.args.endpoint)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "Use an HTTP(S) endpoint without credentials, query parameters, or fragments."
            )
        self.path.mkdir(parents=True, exist_ok=True)
        try:
            self.lock = (self.path / ".running").open("x")
        except FileExistsError:
            raise ValueError(
                "This run is active or was interrupted. Use a new --run-dir; do not mix runs."
            ) from None
        try:
            file = self.path / "manifest.json"
            identity = {
                k: getattr(self.args, k) for k in ("endpoint", "bucket", "region")
            }
            if file.exists():
                self.manifest = read_json(file)
                if self.manifest.get("schema_version") != SCHEMA_VERSION:
                    raise ValueError("Unsupported run schema.")
                if (
                    self.manifest["identity"] != identity
                    or self.manifest["config"] != self.config
                ):
                    raise ValueError(
                        "Endpoint, bucket, region, or configuration differs from this run. Use a new --run-dir."
                    )
                if self.stage in self.manifest["stages"]:
                    raise ValueError(
                        f"Stage {self.stage} already exists. Use a new --run-dir to repeat it."
                    )
            else:
                if any(p.name != ".running" for p in self.path.iterdir()):
                    raise ValueError(
                        "An unversioned run directory must be empty. Use a new --run-dir."
                    )
                self.manifest = {
                    "schema_version": SCHEMA_VERSION,
                    "run_id": uuid.uuid4().hex,
                    "created_at": utc_now(),
                    "identity": identity,
                    "config": self.config,
                    "stages": {},
                }
            options = {
                k: getattr(self.args, k)
                for k in (
                    "tier",
                    "duration_min",
                    "flavor",
                    "levels",
                    "repeats",
                    "object_size_mb",
                    "object_size_kb",
                    "runner_location",
                )
                if hasattr(self.args, k)
            }
            self.manifest["stages"][self.stage] = {
                "status": "RUNNING",
                "started_at": utc_now(),
                "started_epoch": time.time(),
                "options": options,
                "environment": provenance(),
                "artifacts": {},
            }
            self.started = time.monotonic()
            self.save()
            return self
        except BaseException:
            self._unlock()
            raise

    @property
    def record(self):
        return self.manifest["stages"][self.stage]

    def save(self):
        write_json(self.path / "manifest.json", self.manifest)

    def _unlock(self):
        if self.lock is not None:
            self.lock.close()
            (self.path / ".running").unlink(missing_ok=True)
            self.lock = None

    def __exit__(self, typ, value, traceback):
        try:
            self.record.update(
                status="COMPLETED"
                if typ is None
                else "INTERRUPTED"
                if typ is KeyboardInterrupt
                else "FAILED",
                finished_at=utc_now(),
                actual_duration_s=time.monotonic() - self.started,
            )
            if typ:
                self.record["error_type"] = (
                    typ.__name__
                )  # Never serialize arbitrary exception credentials.
            filenames = {
                "compat": ["compat.json"],
                "fanout": ["fanout.json"],
                "put-fanout": ["put-fanout.json"],
                "load": ["ingest_merge.jsonl", "query.jsonl", "consistency.jsonl"],
            }
            self.record["artifacts"] = {
                name: digest(self.path / name)
                for name in filenames[self.stage]
                if (self.path / name).exists()
            }
            self.save()
        finally:
            self._unlock()


def load_bundle(path):
    path = Path(path)
    manifest = read_json(path / "manifest.json")
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported run schema.")
    issues = []
    for stage in manifest["stages"].values():
        for name, expected in stage.get("artifacts", {}).items():
            if Path(name).name != name:
                raise ValueError("Invalid artifact filename in manifest.")
            file = path / name
            if not file.exists() or digest(file) != expected:
                issues.append(f"Missing or changed evidence: {name}")
    return manifest, issues
