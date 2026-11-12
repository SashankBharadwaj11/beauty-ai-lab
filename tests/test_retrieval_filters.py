from datetime import date

from bcia.retrieval import is_visible

DOC001_V1 = {"doc": "DOC001_v1", "access_group": "marketing",
             "effective_from": date(2026, 1, 1), "effective_to": date(2026, 7, 14)}
DOC001_V2 = {"doc": "DOC001_v2", "access_group": "marketing",
             "effective_from": date(2026, 7, 15), "effective_to": None}
DOC001_V3 = {"doc": "DOC001_v3", "access_group": "marketing",
             "effective_from": date(2026, 12, 1), "effective_to": None}
DOC021 = {"doc": "DOC021_v1", "access_group": "restricted_finance",
          "effective_from": date(2026, 1, 1), "effective_to": None}

MARKETING = {"marketing"}
FINANCE = {"restricted_finance"}
TODAY = date(2026, 11, 12)


def test_current_version_visible_today():
    assert is_visible(DOC001_V2, MARKETING, TODAY)


def test_superseded_version_hidden_today_but_visible_historically():   # A1 and the historical case
    assert not is_visible(DOC001_V1, MARKETING, TODAY)
    assert is_visible(DOC001_V1, MARKETING, date(2026, 3, 1))


def test_future_version_hidden_until_effective():                     # DOC001 v3, Q058
    assert not is_visible(DOC001_V3, MARKETING, TODAY)
    assert is_visible(DOC001_V3, MARKETING, date(2026, 12, 1))


def test_restricted_document_paired_test():                           # acceptance A2
    assert not is_visible(DOC021, MARKETING, TODAY)
    assert is_visible(DOC021, FINANCE, TODAY)
