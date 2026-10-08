# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
Step 1: Download all files from the BLS productivity time-series folder.
Source: https://download.bls.gov/pub/time.series/pr/
Target: /Volumes/dev/bronze/raw/bls/

BLS requires a User-Agent header with contact info to avoid 403 Forbidden.
Always re-downloads to ensure data freshness — BLS files are small (<5MB total).
Idempotent: running multiple times produces the same result (latest data).
"""
import urllib.request
import re
import os

USER_AGENT = "RearcQuest/1.0 (contact: bcodernet@gmail.com)"
BLS_BASE = "https://download.bls.gov"
BLS_DIR = "/pub/time.series/pr/"
BLS_URL = f"{BLS_BASE}{BLS_DIR}"
VOLUME_PATH = "/Volumes/dev/bronze/raw/bls"


from typing import List, Tuple

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


def download_file(url: str, dest_path: str) -> None:
    """
    Download a single file with proper User-Agent header.
    Args:
        url (str): URL to download.
        dest_path (str): Path to save file.
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as response:
        with open(dest_path, "wb") as f:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                f.write(chunk)


def main() -> None:
    os.makedirs(VOLUME_PATH, exist_ok=True)

    # Fetch the BLS directory listing page
    req = urllib.request.Request(BLS_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req) as response:
        html_content = response.read().decode("utf-8")

    file_links = discover_file_links(html_content)
    print(f"Found {len(file_links)} files on BLS page")

    downloaded = 0
    failed = 0
    for filename, path in file_links:
        file_url = f"{BLS_BASE}{path}"
        dest_path = os.path.join(VOLUME_PATH, filename)
        print(f"Downloading: {filename}")
        try:
            download_file(file_url, dest_path)
            size = os.path.getsize(dest_path)
            print(f"  -> {size:,} bytes")
            downloaded += 1
        except Exception as e:
            print(f"  -> ERROR: {e}")
            failed += 1

    print(f"\nDone: {downloaded} downloaded, {failed} failed")


if __name__ == "__main__":
    main()