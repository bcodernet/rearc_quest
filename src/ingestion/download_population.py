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
Target: /Volumes/dev/bronze/raw/population/population.json
"""
import urllib.request
import os

POPULATION_API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)
VOLUME_PATH = "/Volumes/dev/bronze/raw/population"
FILENAME = "population.json"


def main() -> None:
    """
    Download population data from DataUSA API and save as JSON.
    Returns:
        None
    """
    os.makedirs(VOLUME_PATH, exist_ok=True)
    dest_path = os.path.join(VOLUME_PATH, FILENAME)

    print("Downloading population data from DataUSA API...")
    req = urllib.request.Request(
        POPULATION_API_URL,
        headers={"User-Agent": "RearcQuest/1.0 (contact: bcodernet@gmail.com)"},
    )
    with urllib.request.urlopen(req) as response:
        data = response.read()

    with open(dest_path, "wb") as f:
        f.write(data)

    print(f"Saved {len(data)} bytes to {dest_path}")


if __name__ == "__main__":
    main()