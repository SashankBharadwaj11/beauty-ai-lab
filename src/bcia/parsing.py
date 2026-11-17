"""BCIA page preflight (D-010): routing hints, NOT proof that OCR is accurate.

Dependencies: pymupdf, cmudict. Never promote a route decision to verified truth.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
import re

import pymupdf


@dataclass(frozen=True)
class PageCheckThresholds:
    # Initial pilot thresholds, not production-calibrated or PyMuPDF defaults.
    full_page_image_coverage: float = 0.70
    native_min_chars: int = 100
    scan_max_chars: int = 15
    min_real_word_ratio: float = 0.65
    minimum_lexical_tokens: int = 8
    hidden_text_fraction: float = 0.10


@lru_cache(maxsize=1)
def _english_words() -> frozenset[str]:
    try:
        import cmudict
    except ImportError as exc:
        raise ImportError(
            "Missing lexical dictionary. Install 'cmudict' to compute real_word_ratio."
        ) from exc
    return frozenset(word.lower() for word in cmudict.words())


def real_word_ratio(text: str) -> tuple[float, int]:
    """Fraction of alphabetic words (length >=3) in the CMU pronunciation dictionary.

    Excludes IDs, numbers, abbreviations and short words; this is only a weak
    signal of OCR plausibility, not measured accuracy or language-model confidence.
    """
    tokens = re.findall(r"[A-Za-z]{3,}", text)
    if not tokens:
        return 0.0, 0
    vocabulary = _english_words()
    matches = sum(token.lower() in vocabulary for token in tokens)
    return round(matches / len(tokens), 4), len(tokens)


def _covered_area(rectangles: list[pymupdf.Rect], bounds: pymupdf.Rect) -> float:
    """Exact union of axis-aligned image bounding boxes clipped to page."""
    rects = []
    for rectangle in rectangles:
        r = pymupdf.Rect(rectangle) & bounds
        if not r.is_empty and r.width > 0 and r.height > 0:
            rects.append(r)
    if not rects:
        return 0.0
    xs = sorted({value for r in rects for value in (r.x0, r.x1)})
    union = 0.0
    for left, right in zip(xs[:-1], xs[1:]):
        if right <= left:
            continue
        ys = sorted((r.y0, r.y1) for r in rects if r.x0 < right and r.x1 > left)
        if not ys:
            continue
        covered_y = 0.0
        start, end = ys[0]
        for y0, y1 in ys[1:]:
            if y0 <= end:
                end = max(end, y1)
            else:
                covered_y += end - start
                start, end = y0, y1
        covered_y += end - start
        union += (right - left) * covered_y
    return union


def classify_page(
    page: pymupdf.Page,
    *,
    thresholds: PageCheckThresholds | None = None,
) -> dict:
    """Return a routing verdict and raw measurable signals for one PDF page.

    Verdicts:
      * native           : native-text candidate; still needs content validation
      * ocr_required     : likely image scan with very little existing text
      * reocr_required   : full-page image plus hidden/unreliable OCR text
      * review_required  : uncertain; do not index without another check
    """
    t = thresholds or PageCheckThresholds()
    text = page.get_text("text", sort=False)
    char_count = sum(not c.isspace() for c in text)
    ratio, lexical_token_count = real_word_ratio(text)

    # get_image_info reports DISPLAYED images, not merely images referenced in
    # the PDF resource dictionary. Multiple tiles are combined by union area.
    image_rects = [pymupdf.Rect(img["bbox"]) for img in page.get_image_info()]
    area = page.rect.get_area()
    image_coverage = _covered_area(image_rects, page.rect) / max(area, 1.0)

    spans = page.get_texttrace()
    traced_chars = sum(len(s["chars"]) for s in spans)
    hidden_chars = sum(
        len(s["chars"])
        for s in spans
        if s.get("type") == 3 or s.get("opacity", 1.0) <= 0.05
    )
    hidden_ratio = hidden_chars / max(traced_chars, 1)
    # Extra independent signal from the page's painting log.
    has_ignored_text = any(kind == "ignore-text" for kind, _ in page.get_bboxlog())
    has_invisible_text = hidden_chars > 0 or has_ignored_text

    likely_page_scan = image_coverage >= t.full_page_image_coverage
    poor_lexical_quality = (
        lexical_token_count >= t.minimum_lexical_tokens
        and ratio < t.min_real_word_ratio
    )

    if likely_page_scan and (
        hidden_ratio >= t.hidden_text_fraction
        or (has_invisible_text and poor_lexical_quality)
    ):
        verdict = "reocr_required"
        reason = "full_page_image_with_suspicious_hidden_text"
    elif likely_page_scan and char_count <= t.scan_max_chars:
        verdict = "ocr_required"
        reason = "full_page_image_with_insufficient_text"
    elif likely_page_scan and poor_lexical_quality:
        verdict = "reocr_required"
        reason = "full_page_image_with_low_lexical_quality"
    elif (
        char_count >= t.native_min_chars
        and lexical_token_count >= t.minimum_lexical_tokens
        and ratio >= t.min_real_word_ratio
        and hidden_ratio < t.hidden_text_fraction
        and not likely_page_scan
    ):
        verdict = "native"
        reason = "sufficient_visible_text_and_lexical_plausibility"
    else:
        verdict = "review_required"
        reason = "insufficient_or_ambiguous_evidence"

    return {
        "verdict": verdict,
        "reason": reason,
        "character_count": char_count,
        "image_coverage": round(image_coverage, 4),
        "has_invisible_text": has_invisible_text,
        "invisible_character_count": hidden_chars,
        "invisible_text_ratio": round(hidden_ratio, 4),
        "real_word_ratio": ratio,
        "lexical_token_count": lexical_token_count,
        "thresholds": asdict(t),
    }
