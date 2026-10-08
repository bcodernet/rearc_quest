# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
Step 1: Download the DataUSA population API response as JSON.
Source API: https://honolulu-api.datausa.io/tesseract/data.jsonrecords
  ?cube=acs_yg_total_population_1
  &drilldowns=Year%2CNation
  &locale=en
  &measures=Population
Target: /Volumes/{catalog}/{bronze_schema}/{volume_name}/population/population.json

The target is built from job parameters the bundle supplies, so deploying to a different
target writes to that target's catalog instead of always writing to `dev`.

The response body is read in full before the destination is opened, so a failed transfer
leaves the previous copy intact rather than truncating it.
"""
import os
import urllib.request

POPULATION_API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)
USER_AGENT = "RearcQuest/1.0 (contact: bcodernet@gmail.com)"
FILENAME = "population.json"


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

VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/population"


def main() -> None:
    """
    Download population data from DataUSA API and save as JSON.
    Returns:
        None
    """
    os.makedirs(VOLUME_PATH, exist_ok=True)
    dest_path = os.path.join(VOLUME_PATH, FILENAME)

    print(f"Downloading population data from DataUSA API -> {dest_path}")
    req = urllib.request.Request(
        POPULATION_API_URL,
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(req) as response:
        data = response.read()

    with open(dest_path, "wb") as f:
        f.write(data)

    print(f"Saved {len(data):,} bytes to {dest_path}")


if __name__ == "__main__":
    main()
