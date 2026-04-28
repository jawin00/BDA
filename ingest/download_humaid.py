#!/usr/bin/env python3
"""HumAID / CrisisLex labeled disaster tweet datasets.

These are pre-labeled disaster social-media datasets — perfect substitute for
live Bluesky/Twitter, since we are batch-only. We fetch the HumAID tarball
directly from the public Crisis NLP mirror.

If the primary mirror is unreachable we fall back to a sample CSV bundled in
data/samples/humaid_sample.csv (small, kept in repo) so the pipeline is never
blocked by upstream availability.
"""

import argparse
import io
import os
import sys
import tarfile
from pathlib import Path

import requests

HUMAID_URL = "https://crisisnlp.qcri.org/data/humaid/HumAID_data_events_set1_47K.tar.gz"
HUMAID_FALLBACK_SAMPLE = Path("data/samples/humaid_sample.csv")


def download(url: str, dest: Path) -> bool:
    try:
        with requests.get(url, stream=True, timeout=300) as r:
            if r.status_code != 200:
                print(f"  HTTP {r.status_code} from {url}", file=sys.stderr)
                return False
            total = int(r.headers.get("content-length", 0))
            written = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)
                        if total:
                            pct = 100 * written / total
                            print(f"\r  {written / 1e6:.1f} / {total / 1e6:.1f} MB ({pct:.0f}%)",
                                  end="", flush=True)
            print()
        return True
    except requests.RequestException as exc:
        print(f"  download failed: {exc}", file=sys.stderr)
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/social")
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    archive = out_dir / "humaid.tar.gz"
    if not archive.exists() or archive.stat().st_size < 1000:
        print(f"HumAID: downloading from {HUMAID_URL}")
        ok = download(HUMAID_URL, archive)
        if not ok:
            print("HumAID primary mirror unreachable.")
            if HUMAID_FALLBACK_SAMPLE.exists():
                target = out_dir / "humaid_sample.csv"
                target.write_bytes(HUMAID_FALLBACK_SAMPLE.read_bytes())
                print(f"  copied bundled sample: {target}")
                return
            print("  no fallback sample present; skipping social source.", file=sys.stderr)
            return

    print(f"HumAID: extracting {archive}")
    with tarfile.open(archive) as tf:
        members = [m for m in tf.getmembers() if m.name.endswith((".tsv", ".csv"))]
        if args.small:
            members = members[:10]
        for m in members:
            tf.extract(m, out_dir)

    csvs = list(out_dir.rglob("*.tsv")) + list(out_dir.rglob("*.csv"))
    total = sum(p.stat().st_size for p in csvs)
    print(f"HumAID done: {len(csvs)} files, {total / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
