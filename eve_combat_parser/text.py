from __future__ import annotations

import re
from datetime import datetime

from .constants import TS_FMT

TAG_RE = re.compile(r"<[^>]*>")
TS_PREFIX_RE = re.compile(
    r"^\[\s*(?P<ts>\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s*\]\s+\((?P<chan>[^)]+)\)\s+(?P<body>.*)$"
)


def clean_line(raw: str) -> str:
    """Strip EVE HTML-ish tags and normalize whitespace."""
    s = TAG_RE.sub("", raw)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


_ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")


def normalize_key(s: str) -> str:
    """Normalize strings used as dictionary keys.

    We use this for pilot names, corps, and alliance tickers.
    Some logs can contain non-breaking spaces or invisible zero-width
    characters, which can cause key mismatches when we try to backfill
    missing corp/alliance fields.
    """

    if not s:
        return ""
    # Remove common invisible characters.
    s = _ZERO_WIDTH_RE.sub("", s)
    # Normalize non-breaking spaces.
    s = s.replace("\u00A0", " ")
    # Collapse whitespace and trim.
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, TS_FMT)
