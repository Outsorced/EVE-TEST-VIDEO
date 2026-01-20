from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Tuple

from .constants import TS_FMT


@dataclass(frozen=True)
class FightWindow:
    """A contiguous combat window, separated by inactivity gaps."""

    start: datetime
    end: datetime

    def label(self) -> str:
        return f"{self.start:%Y%m%d_%H%M%S}-{self.end:%Y%m%d_%H%M%S}"


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.strptime((ts or "").strip(), TS_FMT)
    except Exception:
        return None


def split_rows_into_fights(
    combat_rows: Iterable[dict[str, Any]],
    *,
    gap_minutes: int = 15,
) -> List[FightWindow]:
    """Split rows into fights based on inactivity gaps.

    We consider *combat rows* (damage, ewar, repairs). If the time gap between
    consecutive combat events exceeds `gap_minutes`, we start a new fight.
    """

    times: List[datetime] = []
    for r in combat_rows:
        t = _parse_ts(r.get("timestamp", ""))
        if t:
            times.append(t)

    if not times:
        return []

    times.sort()
    gap = timedelta(minutes=int(gap_minutes))

    start = times[0]
    prev = times[0]
    out: List[FightWindow] = []
    for t in times[1:]:
        if t - prev > gap:
            out.append(FightWindow(start=start, end=prev))
            start = t
        prev = t

    out.append(FightWindow(start=start, end=prev))
    return out


def filter_rows_by_window(
    rows: Iterable[dict[str, Any]],
    window: FightWindow,
) -> List[dict[str, Any]]:
    """Return rows with timestamps within [window.start, window.end]."""

    out: List[dict[str, Any]] = []
    for r in rows:
        t = _parse_ts(r.get("timestamp", ""))
        if not t:
            continue
        if window.start <= t <= window.end:
            out.append(r)
    return out
