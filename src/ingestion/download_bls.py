# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
Step 1: Land the BLS productivity time-series folder in a UC volume.
Source: https://download.bls.gov/pub/time.series/pr/
Target: /Volumes/{catalog}/{bronze_schema}/{volume_name}/bls/

The target is built from job parameters the bundle supplies, so deploying to a different
target writes to that target's catalog instead of always writing to `dev`.

BLS requires a User-Agent header with contact info to avoid 403 Forbidden.

Re-run safety
-------------
  - New files are downloaded.
  - Changed files are picked up, because every file is re-downloaded on every run. The
    whole folder is under 5MB, so skipping unchanged files buys nothing and BLS exposes
    no reliable per-file version to compare against.
  - Removed files are detected and reported but NOT deleted -- see below.
  - Each file is buffered fully in memory before anything is written, so a failed
    transfer leaves the previous copy intact rather than truncating it.
  - The task fails if any download fails, rather than letting the pipeline build Bronze
    from a partial landing zone and quietly publishing stale gold tables.

Removed source files: an acknowledged open question
---------------------------------------------------
When BLS retires a file, this script reports it and leaves it in place.

A literal reading of the quest ("keep the S3 bucket in sync") implies deleting it, and
that is a one-line change (flip RETAIN_UNPUBLISHED_FILES to False -- the delete branch
is implemented below). We default to retaining because:

  - Deleting raw landed data is irreversible and BLS serves no historical versions, so
    the volume is the only copy. Once deleted, results that were produced from that file
    can never be reproduced or audited.
  - A disappearing file is more often an upstream glitch, a renamed series, or a
    publication-schedule artifact than a true retirement. Deleting on first absence
    turns a transient upstream problem into permanent local data loss.
  - Retention is reversible; deletion is not. Where the two conflict and the requirement
    is ambiguous, the reversible default is the safer engineering choice.

Whether this landing zone should be a mirror of the source or an archive of its history
is a business/compliance decision, not a technical default. It needs a real answer
before this goes to production, and the answer drives the design:

  - Mirror: set RETAIN_UNPUBLISHED_FILES = False. Simplest, lowest storage, no history.
  - Archive with history: land into date-partitioned prefixes
    (raw/bls/ingest_date=YYYY-MM-DD/) so every run is a complete point-in-time snapshot
    and Bronze reads the latest partition. Costs storage, gives full reproducibility.
  - Soft delete: move unpublished files to raw/bls/_retired/ with the retirement date,
    keeping the active prefix clean while preserving the data.
  - Manifest table: record first_seen / last_seen per filename so retirements are
    queryable and alertable without touching the files at all.

Known risk of the retain default: the Bronze layer reads a fixed set of filenames. If
BLS retired one of those (e.g. pr.measure), Bronze would keep reading the retained copy
indefinitely and silently serve stale lookup labels. The warning this script prints is
the only signal today; a production version wants an expectation or alert on landing-zone
staleness, not just a log line.
"""
import os
import re
import urllib.request
from typing import List, Set, Tuple

USER_AGENT = "RearcQuest/1.0 (contact: bcodernet@gmail.com)"
BLS_BASE = "https://download.bls.gov"
BLS_DIR = "/pub/time.series/pr/"
BLS_URL = f"{BLS_BASE}{BLS_DIR}"


def job_param(name: str, default: str) -> str:
    """
    Read a job parameter supplied by the bundle, with a fallback.

    The job declares catalog / bronze_schema / volume_name as parameters so this script
    writes wherever the deploy target points, rather than hardcoding `dev`. Databricks
    auto-creates a widget per job parameter, so `get` resolves inside a job run; it
    raises when the script is run interactively with no widgets, which is what the
    fallback covers.

    Args:
        name (str): Parameter name.
        default (str): Value to use outside a job run.
    Returns:
        str: The parameter value, or `default`.
    """
    try:
        value = dbutils.widgets.get(name)  # noqa: F821 - injected by Databricks
    except Exception:
        return default
    return value or default


CATALOG = job_param("catalog", "dev")
BRONZE_SCHEMA = job_param("bronze_schema", "bronze")
VOLUME_NAME = job_param("volume_name", "raw")

VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/bls"

# True  -> report files BLS no longer publishes, keep them (default; see docstring).
# False -> delete them, making the volume a strict mirror of the source folder.
RETAIN_UNPUBLISHED_FILES = True


def discover_file_links(html_content: str) -> List[Tuple[str, str]]:
    """
    Parse the BLS directory listing page for all file links.
    Args:
        html_content (str): HTML content of the listing page.
    Returns:
        List[Tuple[str, str]]: List of (filename, url_path) pairs.
    """
    links = re.findall(r'HREF="([^"]+)"', html_content, re.IGNORECASE)
    file_links = []
    for link in links:
        if link.endswith("/") or link.startswith("?"):
            continue
        filename = link.split("/")[-1]
        if filename:
            file_links.append((filename, link))
    return file_links


def download_file(url: str, dest_path: str) -> int:
    """
    Download a single file with the required User-Agent header.

    The whole body is read before the destination is opened for writing. That keeps an
    interrupted transfer from leaving a truncated file behind for the Bronze layer to
    read -- a .tmp-and-rename would be the usual trick, but UC Volumes do not reliably
    support rename through the FUSE mount, and these files are small enough to buffer.

    Args:
        url (str): URL to download.
        dest_path (str): Path to save file.
    Returns:
        int: Number of bytes written.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as response:
        data = response.read()

    with open(dest_path, "wb") as f:
        f.write(data)
    return len(data)


def find_unpublished(expected: Set[str]) -> List[str]:
    """
    List files present in the volume that BLS no longer publishes.

    Detection only -- the caller decides what to do about them.

    Args:
        expected (Set[str]): Filenames currently published by BLS.
    Returns:
        List[str]: Filenames in the volume but not in the source listing.
    """
    return [
        name
        for name in sorted(os.listdir(VOLUME_PATH))
        if not os.path.isdir(os.path.join(VOLUME_PATH, name)) and name not in expected
    ]


def main() -> None:
    os.makedirs(VOLUME_PATH, exist_ok=True)

    # Fetch the BLS directory listing page
    req = urllib.request.Request(BLS_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as response:
        html_content = response.read().decode("utf-8")

    file_links = discover_file_links(html_content)
    print(f"Found {len(file_links)} files on BLS page")

    # An empty listing means the fetch or the parse broke, not that BLS retired the
    # entire dataset. Continuing would report every landed file as unpublished (or, with
    # RETAIN_UNPUBLISHED_FILES = False, delete all of them) and mask the real failure.
    if not file_links:
        raise RuntimeError(
            f"No file links discovered at {BLS_URL}. Refusing to proceed: an empty "
            "source listing would flag every file in the landing zone as retired. "
            "Check whether the page layout or the URL has changed."
        )

    downloaded = 0
    failed: List[str] = []
    for filename, path in file_links:
        file_url = f"{BLS_BASE}{path}"
        dest_path = os.path.join(VOLUME_PATH, filename)
        print(f"Downloading: {filename}")
        try:
            size = download_file(file_url, dest_path)
            print(f"  -> {size:,} bytes")
            downloaded += 1
        except Exception as e:
            print(f"  -> ERROR: {e}")
            failed.append(filename)

    # Reconcile against the listing we successfully fetched above, so this stays correct
    # even if some individual downloads failed.
    expected = {filename for filename, _ in file_links}
    unpublished = find_unpublished(expected)
    removed = 0

    for name in unpublished:
        if RETAIN_UNPUBLISHED_FILES:
            print(f"  RETAINED (no longer published by BLS): {name}")
        else:
            os.remove(os.path.join(VOLUME_PATH, name))
            removed += 1
            print(f"  DELETED (no longer published by BLS): {name}")

    if unpublished and RETAIN_UNPUBLISHED_FILES:
        print(
            f"\nWARNING: {len(unpublished)} file(s) in {VOLUME_PATH} are no longer "
            "published upstream and were RETAINED, not deleted. The landing zone is "
            "therefore a superset of the current source folder. Whether it should "
            "mirror the source or archive its history is an open business decision -- "
            "see 'Removed source files' in this module's docstring and in PROCESS.md."
        )

    print(
        f"\nDone: {downloaded} downloaded, {len(failed)} failed, "
        f"{len(unpublished)} no longer published ({removed} deleted)"
    )

    # Fail the task rather than let the pipeline build Bronze from a partial landing
    # zone. Without this the job reports success and the downstream tables quietly
    # reflect whatever stale copy survived from the previous run.
    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(file_links)} BLS files failed to download: "
            f"{', '.join(failed)}. Not continuing to the pipeline with a partial "
            "landing zone."
        )


if __name__ == "__main__":
    main()
