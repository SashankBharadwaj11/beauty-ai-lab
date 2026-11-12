"""Walking-skeleton router: decides which path a question needs.

BASELINE ONLY. Keyword rules are deliberately naive so we get a first score on
the evaluation set. Later this is replaced by an LLM classifier returning the
same RouteDecision, and the two are compared on routing accuracy.
"""
from __future__ import annotations

from dataclasses import dataclass

NUMBER_WORDS = ("roas", "net sales", "sales", "spend", "revenue", "ctr", "how many",
                "how much", "units", "inventory", "perform", "trend")
DOCUMENT_WORDS = ("claim", "wording", "approved", "policy", "guideline", "say about",
                  "allowed", "can we say", "research", "study", "survey", "faq")
NEEDS_MARKET = ("promotion", "promo")
MARKETS = ("new york", "boston", "charlotte", "atlanta", "los angeles", "chicago",
           "northeast", "south", "west", "midwest", "m01", "m02", "m03", "m04", "m05", "m06")


@dataclass(frozen=True)
class RouteDecision:
    route: str     # structured | unstructured | both | clarify
    reason: str


def route(question: str) -> RouteDecision:
    q = question.lower()
    if any(w in q for w in NEEDS_MARKET) and not any(m in q for m in MARKETS):
        return RouteDecision("clarify", "promotion rules differ by market and no market was given")
    numbers = any(w in q for w in NUMBER_WORDS)
    documents = any(w in q for w in DOCUMENT_WORDS)
    if numbers and documents:
        return RouteDecision("both", "asks for metrics and document content")
    if numbers:
        return RouteDecision("structured", "asks for a metric")
    if documents:
        return RouteDecision("unstructured", "asks for document content")
    return RouteDecision("clarify", "could not tell what the question needs")
