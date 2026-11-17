"""D-010: reproducible unit fixtures; not measured against the user's Day 3 PDF."""

from pathlib import Path
import sys

import pymupdf

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from bcia.parsing import classify_page


GOOD_TEXT = (
    "The research team reviewed the synthetic hydration improvement results for "
    "each product formulation and recorded the approved policy wording. "
    "This document describes the research methodology, the permitted language "
    "for marketing, and the approval process for the regional review team. "
    "Before publication, the reviewer must confirm that every claim is supported "
    "by the current research report and that the source version remains active."
)
BAD_TEXT = (
    "qzxvrr zzqmmn xzqvvq qzpkkx vvvzzn qxzrqt zzxkqp xqqmzz "
    "vvqzrx xzzqpp qqzvvm ppxzqt xkkzvv qzzpqm xxzrqp qvzzxm "
) * 2


def make_native_doc():
    pdf = pymupdf.open()
    page = pdf.new_page(width=612, height=792)
    assert page.insert_textbox(
        pymupdf.Rect(48, 50, 565, 500), GOOD_TEXT, fontsize=12
    ) >= 0
    return pdf


def make_scanned_doc(native_page):
    pix = native_page.get_pixmap(dpi=150, alpha=False)
    doc = pymupdf.open()
    page = doc.new_page(width=native_page.rect.width, height=native_page.rect.height)
    page.insert_image(page.rect, stream=pix.tobytes("png"))
    return doc


def add_corrupt_hidden_layer(page):
    # Multiple short rows to fit on the page and remain extractable.
    words = BAD_TEXT.split()
    for i in range(0, len(words), 8):
        line = " ".join(words[i:i+8])
        page.insert_text((48, 90 + (i // 8) * 20), line, fontsize=11, render_mode=3)


def test_native():
    with make_native_doc() as doc:
        m = classify_page(doc[0])
        assert m["verdict"] == "native", m
        assert m["character_count"] >= 100
        assert m["image_coverage"] < 0.1
        assert m["has_invisible_text"] is False
        assert m["real_word_ratio"] >= 0.65


def test_scanned_image_only():
    with make_native_doc() as source:
        with make_scanned_doc(source[0]) as doc:
            m = classify_page(doc[0])
            assert m["verdict"] == "ocr_required", m
            assert m["character_count"] <= 15
            assert m["image_coverage"] >= 0.7
            assert not m["has_invisible_text"]


def test_scanned_with_corrupt_invisible_ocr_layer():
    with make_native_doc() as source:
        with make_scanned_doc(source[0]) as doc:
            add_corrupt_hidden_layer(doc[0])
            m = classify_page(doc[0])
            assert m["verdict"] == "reocr_required", m
            assert m["character_count"] >= 100
            assert m["image_coverage"] >= 0.7
            assert m["has_invisible_text"]
            assert m["invisible_text_ratio"] >= 0.9
            assert m["real_word_ratio"] < 0.20
