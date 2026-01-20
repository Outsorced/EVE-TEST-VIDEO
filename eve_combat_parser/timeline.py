from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .models import ShipStateEvent


Timeline = Dict[str, List[ShipStateEvent]]


def restrict_timeline(timeline: Timeline, start: datetime, end: datetime) -> Timeline:
    """Return a copy of timeline containing only events within [start, end].

    This is used for fight-splitting: pilots often swap ships between fights,
    so ship backfills must not cross fight boundaries.
    """

    out: Timeline = {}
    for pilot, events in timeline.items():
        sub = [e for e in events if start <= e.t <= end]
        if sub:
            out[pilot] = sub
    return out


def add_ship_event(
    timeline: Timeline,
    pilot: str,
    t: datetime,
    ship: Optional[str],
    alliance: str = "",
    corp: str = "",
) -> None:
    pilot = pilot.strip()
    if not pilot:
        return
    timeline.setdefault(pilot, []).append(ShipStateEvent(t=t, ship=ship, alliance=alliance, corp=corp))


def finalize_timeline(timeline: Timeline) -> None:
    for events in timeline.values():
        events.sort(key=lambda e: e.t)


def _prev_disembark_index(events: List[ShipStateEvent], start_i: int) -> int:
    for j in range(start_i, -1, -1):
        if events[j].ship is None:
            return j
    return -1


def _next_known_ship_after(events: List[ShipStateEvent], start_i: int) -> Optional[ShipStateEvent]:
    for j in range(start_i + 1, len(events)):
        if events[j].ship is None:
            return None
        if events[j].ship:
            return events[j]
    return None


def resolve_ship_with_backfill(events: List[ShipStateEvent], t: datetime) -> Tuple[str, str, str]:
    """Resolve a pilot's ship at time t.

    Uses "disembark" as a boundary: if we encounter a disembark event, we do not
    backfill from earlier ships, but *do* allow backfilling from the next known
    ship after disembark when t is before that next event.
    """

    if not events:
        return "", "", ""

    idx = -1
    for i, ev in enumerate(events):
        if ev.t <= t:
            idx = i
        else:
            break

    if idx < 0:
        # Best-effort backfill from the *first* observed ship state.
        #
        # Some log formats only mention the "other" party (e.g. "<amount> from X")
        # so the listener's ship/corp/alliance might only be learned later in the
        # fight (via reps / warp scramble attempt / etc.). The user expects us to
        # fill those earlier rows once we know the listener's state somewhere in
        # the selected log set.
        first = events[0]
        if first.ship:
            return first.ship, first.alliance, first.corp
        return "", "", ""

    ev = events[idx]
    if ev.ship:
        return ev.ship, ev.alliance, ev.corp

    d_idx = _prev_disembark_index(events, idx)
    if d_idx != -1:
        nxt = _next_known_ship_after(events, d_idx)
        if nxt and t <= nxt.t:
            return (nxt.ship or ""), nxt.alliance, nxt.corp
    return "", "", ""


def lookup_ship(timeline: Timeline, pilot: str, t: datetime) -> Tuple[str, str, str]:
    events = timeline.get(pilot.strip())
    if not events:
        return "", "", ""
    return resolve_ship_with_backfill(events, t)
