"""Lazy-download bridge to the Rust `omx` binary.

The Python package carries the pure-Python subcommands (metrics, tables,
setup, compiler, auth, ...). The heavier subcommands — `ci`, `dbt`,
`discover`, `scaffold` — are implemented in Rust for the SQL parser,
compiler reuse, and perf. They ship as a binary attached to this repo's
GitHub Releases and are fetched on first use.

Design notes:
- No maturin / native wheel yet. Simpler packaging; slower first run.
- Binary cached in ~/.cache/onlymetrix/omx-<version>.
- Platform detected at runtime; unsupported platforms fail fast.
- Set OMX_BINARY to an absolute path to bypass download entirely
  (for dev + for pre-seeded CI environments).
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from . import __version__

RELEASE_REPO = "dreynow/onlymetrix-python"
RELEASE_TAG = f"v{__version__}"
URL_TEMPLATE = (
    "https://github.com/{repo}/releases/download/{tag}/omx-{plat}.tar.gz"
)

# Subcommands the Rust binary owns. Everything else stays pure Python.
RUST_SUBCOMMANDS = {"ci", "dbt", "discover", "scaffold"}


def _platform_tag() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "linux" and machine in ("x86_64", "amd64"):
        return "linux-x64"
    if system == "darwin" and machine in ("x86_64", "amd64"):
        return "macos-x64"
    if system == "darwin" and machine in ("arm64", "aarch64"):
        return "macos-arm64"
    raise RuntimeError(
        f"onlymetrix: no Rust binary published for {system}/{machine} yet. "
        "Set OMX_BINARY to a locally built `omx` to unblock."
    )


def _cache_dir() -> Path:
    env = os.environ.get("OMX_CACHE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "onlymetrix"


def _cached_binary_path() -> Path:
    return _cache_dir() / f"omx-{__version__}"


def _download_binary(plat: str, dest: Path) -> None:
    url = URL_TEMPLATE.format(repo=RELEASE_REPO, tag=RELEASE_TAG, plat=plat)
    sys.stderr.write(f"onlymetrix: fetching omx {__version__} ({plat})...\n")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as f:
        tarball = Path(f.name)
    try:
        urllib.request.urlretrieve(url, tarball)
        with tarfile.open(tarball) as t:
            members = [m for m in t.getmembers() if m.name == "omx" or m.name.endswith("/omx")]
            if not members:
                raise RuntimeError(f"omx binary not found in tarball from {url}")
            with t.extractfile(members[0]) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
        dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    finally:
        tarball.unlink(missing_ok=True)


def resolve_binary() -> Path:
    """Return a path to an executable omx binary, downloading if necessary."""
    override = os.environ.get("OMX_BINARY")
    if override:
        p = Path(override)
        if not p.exists():
            raise RuntimeError(f"OMX_BINARY points at missing file: {p}")
        return p

    cached = _cached_binary_path()
    if cached.exists() and os.access(cached, os.X_OK):
        return cached

    plat = _platform_tag()
    _download_binary(plat, cached)
    return cached


def exec_omx(args: list[str]) -> "int | None":
    """Run the Rust omx with the given args and return its exit code.

    Does not return under normal flow — sys.exit() carries the child's code.
    """
    binary = resolve_binary()
    result = subprocess.run([str(binary), *args])
    sys.exit(result.returncode)


def maybe_dispatch_to_rust(argv: list[str]) -> None:
    """If the top-level subcommand belongs to the Rust binary, exec it.

    Called from the `omx` entry point before click enters so the CLI
    surface appears unified even though the implementation is split.
    """
    if len(argv) >= 2 and argv[1] in RUST_SUBCOMMANDS:
        exec_omx(argv[1:])
