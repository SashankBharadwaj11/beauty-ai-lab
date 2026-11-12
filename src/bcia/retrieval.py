"""Which document chunks a user may see, as of a date.

This is the security and versioning rule, written once as plain Python so it
can be unit-tested without Databricks. On Day 4 the same rule is translated
into filters applied INSIDE the search query, so stale, future or restricted
chunks never come back from the index at all.
"""
from __future__ import annotations

from datetime import date


def is_visible(chunk: dict, user_groups: set[str], as_of: date) -> bool:
    """True if this chunk may be used to answer a question asked on `as_of`.

    chunk needs: access_group (str), effective_from (date), effective_to (date | None).
    - Access: the chunk's access_group must be one of the user's groups.
    - Time: effective_from <= as_of, and effective_to is empty or >= as_of.
      This one rule handles superseded versions (ended before as_of) and
      future-dated versions (start after as_of) the same way.
    """
    if chunk["access_group"] not in user_groups:
        return False
    if chunk["effective_from"] > as_of:
        return False
    effective_to = chunk.get("effective_to")
    return effective_to is None or effective_to >= as_of


def visible_chunks(chunks: list[dict], user_groups: set[str], as_of: date) -> list[dict]:
    return [c for c in chunks if is_visible(c, user_groups, as_of)]
