"""Public-release checks for the repository.

The checks are intentionally conservative. They look for tracked local files,
uncleared notebook outputs, oversized artifacts and private project markers in
text files and the rendered report.
"""

from __future__ import annotations

import fnmatch
import json
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAX_TRACKED_FILE_BYTES = 8 * 1024 * 1024


def _term(*parts: str) -> str:
    return "".join(parts)


BLOCKED_TEXT_MARKERS = [
    _term("cc", "c37"),
    _term("team", "37"),
    _term("comp", "90024"),
    _term("git", "lab"),
    _term("you", "tube"),
    _term("l", "lm"),
    _term("large", " ", "language"),
    _term("xiao", "jibao"),
    _term("black", "mirean"),
    _term("university", " ", "of", " ", "melbourne"),
    _term("m", "rc"),
    chr(0x4E2D) + chr(0x6587),
    _term("trans", "lated"),
    _term("trans", "lation"),
]

BLOCKED_PATH_PATTERNS = [
    ".env",
    ".env.*",
    "config.*.yaml",
    "*.kubeconfig",
    "data/backfill_state/*",
    "data/local_store/*",
    "data/private/*",
    "data/raw/*",
    "teamwork/*",
    "video/*",
    "tmp/*",
    "docs/generated/*",
    "docs/zh/*",
    "README.zh.md",
]

ALLOWED_PATHS = {
    ".env.example",
}

BINARY_SUFFIXES = {
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".pdf",
    ".png",
    ".webp",
}


def run_git_ls_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return [ROOT / line for line in result.stdout.splitlines() if line.strip()]


def is_text_candidate(path: Path) -> bool:
    return path.suffix.lower() not in BINARY_SUFFIXES


def read_text(path: Path) -> str | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\0" in data[:4096]:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-8", errors="ignore")


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def check_paths(files: list[Path]) -> list[str]:
    failures: list[str] = []
    for path in files:
        relative = rel(path)
        if relative in ALLOWED_PATHS:
            continue
        for pattern in BLOCKED_PATH_PATTERNS:
            if fnmatch.fnmatch(relative, pattern):
                failures.append(f"tracked blocked path: {relative}")
                break
    return failures


def check_file_sizes(files: list[Path]) -> list[str]:
    failures: list[str] = []
    for path in files:
        try:
            size = path.stat().st_size
        except OSError:
            continue
        if size > MAX_TRACKED_FILE_BYTES:
            failures.append(f"tracked file exceeds {MAX_TRACKED_FILE_BYTES} bytes: {rel(path)}")
    return failures


def check_text_markers(files: list[Path]) -> list[str]:
    failures: list[str] = []
    lowered_markers = [marker.casefold() for marker in BLOCKED_TEXT_MARKERS]
    for path in files:
        if not is_text_candidate(path):
            continue
        text = read_text(path)
        if not text:
            continue
        lowered = text.casefold()
        for marker, marker_lower in zip(BLOCKED_TEXT_MARKERS, lowered_markers):
            if marker_lower in lowered:
                failures.append(f"blocked text marker in {rel(path)}: {marker}")
    return failures


def check_notebook_outputs(files: list[Path]) -> list[str]:
    failures: list[str] = []
    for path in files:
        if path.suffix != ".ipynb":
            continue
        text = read_text(path)
        if not text:
            continue
        try:
            notebook = json.loads(text)
        except json.JSONDecodeError:
            failures.append(f"invalid notebook JSON: {rel(path)}")
            continue
        for index, cell in enumerate(notebook.get("cells", []), start=1):
            if cell.get("outputs"):
                failures.append(f"notebook output not cleared: {rel(path)} cell {index}")
            if cell.get("execution_count") is not None:
                failures.append(f"notebook execution count not cleared: {rel(path)} cell {index}")
    return failures


def check_report_pdf() -> list[str]:
    report = ROOT / "report" / "source" / "main.pdf"
    if not report.exists() or shutil.which("pdftotext") is None:
        return []
    result = subprocess.run(
        ["pdftotext", str(report), "-"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        return [f"could not read report PDF text: {result.stderr.strip()}"]
    text = result.stdout.casefold()
    failures = []
    for marker in BLOCKED_TEXT_MARKERS:
        if marker.casefold() in text:
            failures.append(f"blocked text marker in report PDF: {marker}")
    return failures


def main() -> int:
    files = run_git_ls_files()
    failures = []
    failures.extend(check_paths(files))
    failures.extend(check_file_sizes(files))
    failures.extend(check_text_markers(files))
    failures.extend(check_notebook_outputs(files))
    failures.extend(check_report_pdf())

    if failures:
        print("Public release check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print(f"Public release check passed for {len(files)} release files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
