"""Deterministic answer checks that run AFTER the LLM writes an answer.

Why this exists: a prompt can ask the model to use only approved claims, but a
prompt is not a guarantee. This module is the guarantee. Any claim the answer
puts in quotation marks must exist in the approved claims register, and no
quoted claim may contain a banned medical verb. Explanations outside quotes
("you cannot say it cures dryness") are allowed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

BANNED_VERBS = ("cure", "cures", "cured", "treat", "treats", "treated",
                "prevent", "prevents", "prevented", "reverse", "reverses", "reversed")

_QUOTED = re.compile(r'"([^"]+)"|“([^”]+)”')
_BANNED = re.compile(r"\b(" + "|".join(BANNED_VERBS) + r")\b", re.IGNORECASE)


@dataclass(frozen=True)
class Violation:
    rule: str      # "not_in_register" or "banned_verb"
    text: str      # the offending quoted claim


def quoted_claims(answer: str) -> list[str]:
    """Return every phrase the answer puts in straight or curly double quotes."""
    return [a or b for a, b in _QUOTED.findall(answer)]


def check_claims(answer: str, approved_phrases: set[str]) -> list[Violation]:
    """Check every quoted claim in `answer` against the approved register.

    approved_phrases: the currently approved claim texts (from DOC001 v2 / DOC014).
    Matching is case-insensitive and ignores surrounding whitespace.
    Returns an empty list when the answer is safe to show.
    """
    approved = {p.strip().lower() for p in approved_phrases}
    violations: list[Violation] = []
    for claim in quoted_claims(answer):
        normalized = claim.strip().lower()
        if _BANNED.search(normalized):
            violations.append(Violation("banned_verb", claim))
        elif normalized not in approved:
            violations.append(Violation("not_in_register", claim))
    return violations
