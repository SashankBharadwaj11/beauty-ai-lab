"""Classify the three test PDFs and export measurements as JSON.

Usage: python scripts/run_page_triage.py artifacts/page_check/*.pdf
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import pymupdf

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from bcia.parsing import classify_page


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("artifacts/page_check/results.json"))
    args = parser.parse_args()
    out = []
    for file in args.files:
        with pymupdf.open(file) as doc:
            for i, page in enumerate(doc, 1):
                out.append({"file": str(file), "page_number": i, **classify_page(page)})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
