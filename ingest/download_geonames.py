#!/usr/bin/env python3
"""GeoNames lexicon: download cities + country codes for the geoparse UDF.

geonamescache (Python lib) ships with built-in data, but for fallback NER
matching on social-post text we want a richer cities500 dump (~10 MB).
This is a one-off bootstrap; lives under data/lexicons/geonames/.
"""

import io
import os
import sys
import zipfile
from pathlib import Path

import requests

CITIES_URL = "https://download.geonames.org/export/dump/cities500.zip"


def main():
    out_dir = Path("data/lexicons/geonames")
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "cities500.txt"
    if target.exists() and target.stat().st_size > 1_000_000:
        print(f"GeoNames cities500 already present: {target.stat().st_size / 1e6:.1f} MB")
        return
    print(f"GeoNames: downloading {CITIES_URL}")
    r = requests.get(CITIES_URL, timeout=300)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(out_dir)
    print(f"GeoNames done: {target.stat().st_size / 1e6:.1f} MB at {target}")


if __name__ == "__main__":
    main()
