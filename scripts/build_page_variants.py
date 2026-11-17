"""Create native, raster-scanned, and deliberately corrupted invisible-OCR PDFs.

Usage: python scripts/build_page_variants.py path/to/BCIA.pdf --page 1
All output is LOCAL SYNTHETIC TEST ARTIFACTS in artifacts/page_check/.
Requires PyMuPDF. Never alter the input PDF.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import pymupdf

BAD_LAYER = (
    "qzxvrr zzqmmn xzqvvq qzpkkx vvvzzn qxzrqt zzxkqp xqqmzz "
    "vvqzrx xzzqpp qqzvvm ppxzqt xkkzvv qzzpqm xxzrqp qvzzxm "
) * 2


def build_variants(source: Path, page_one_based: int, out: Path) -> dict[str, Path]:
    out.mkdir(parents=True, exist_ok=True)
    with pymupdf.open(source) as original:
        assert 1 <= page_one_based <= len(original), "Page number out of range"
        target = original[page_one_based - 1]
        pix = target.get_pixmap(dpi=150, alpha=False)

        with pymupdf.open() as native:
            native.insert_pdf(original, from_page=page_one_based - 1, to_page=page_one_based - 1)
            native_path = out / "native.pdf"
            native.save(native_path)

        with pymupdf.open() as scanned:
            page = scanned.new_page(width=target.rect.width, height=target.rect.height)
            page.insert_image(page.rect, stream=pix.tobytes("png"))
            scan_path = out / "scanned.pdf"
            scanned.save(scan_path)

        with pymupdf.open() as corrupted:
            page = corrupted.new_page(width=target.rect.width, height=target.rect.height)
            page.insert_image(page.rect, stream=pix.tobytes("png"))
            # Add an invisible, deliberately corrupted text overlay without
            # changing the visible page image. render_mode=3 = ignored text.
            words = BAD_LAYER.split()
            for idx in range(0, len(words), 8):
                y = 70 + 17 * (idx // 8)
                if y > page.rect.height - 30:
                    break
                page.insert_text((40, y), " ".join(words[idx:idx+8]), fontsize=10, render_mode=3)
            bad_path = out / "bad_ocr_layer.pdf"
            corrupted.save(bad_path)

    return {"native": native_path, "scanned": scan_path, "bad_ocr_layer": bad_path}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--out", type=Path, default=Path("artifacts/page_check"))
    args = parser.parse_args()
    for label, path in build_variants(args.pdf, args.page, args.out).items():
        print(f"{label}: {path}")
