#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-click cleanup for cache/log/temp files.

Default behavior keeps analysis outputs and source code.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Iterable, List


def _collect_targets(root: Path) -> List[Path]:
    targets: List[Path] = []

    for name in (".pytest_cache", "_ref_wechat_spider"):
        path = root / name
        if path.exists():
            targets.append(path)

    log_file = root / "geo_analyzer.log"
    if log_file.exists():
        targets.append(log_file)

    for path in root.rglob("__pycache__"):
        if path.is_dir():
            targets.append(path)

    for path in (root / "out").glob("runs/**/raw_crawl_*.json"):
        if path.is_file():
            targets.append(path)

    # De-duplicate while preserving order.
    unique: List[Path] = []
    seen = set()
    for path in targets:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _collect_old_run_dirs(root: Path, keep_latest_runs: int) -> List[Path]:
    if keep_latest_runs <= 0:
        return []
    runs_root = root / "out" / "runs"
    if not runs_root.exists():
        return []

    run_dirs = sorted([p for p in runs_root.iterdir() if p.is_dir()], key=lambda p: p.name)
    if len(run_dirs) <= keep_latest_runs:
        return []
    return run_dirs[: len(run_dirs) - keep_latest_runs]


def _remove_paths(paths: Iterable[Path], dry_run: bool = False) -> int:
    removed = 0
    for path in paths:
        if not path.exists():
            continue
        print(f"[clean] {'would remove' if dry_run else 'remove'}: {path}")
        if dry_run:
            removed += 1
            continue
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)
        removed += 1
    return removed


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean cache/log/temp files in workspace.")
    parser.add_argument("--root", type=str, default=".", help="Workspace root path (default: current dir)")
    parser.add_argument("--dry-run", action="store_true", help="Show what will be removed without deleting")
    parser.add_argument("--purge-output", action="store_true", help="Also remove the whole out/ directory")
    parser.add_argument(
        "--keep-latest-runs",
        type=int,
        default=0,
        help="Keep latest N run folders under out/runs, remove older ones (default: 0 = disabled)",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists():
        print(f"[clean] root not found: {root}")
        return 1

    targets = _collect_targets(root)

    if args.purge_output:
        out_dir = root / "out"
        if out_dir.exists():
            targets.append(out_dir)
    else:
        targets.extend(_collect_old_run_dirs(root, max(0, int(args.keep_latest_runs))))

    removed = _remove_paths(targets, dry_run=args.dry_run)
    print(f"[clean] done, {'matched' if args.dry_run else 'removed'} {removed} item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

