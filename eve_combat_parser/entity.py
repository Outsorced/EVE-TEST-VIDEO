from __future__ import annotations

import re
from typing import Tuple


# Damage entity format:
#   Name[CORP](ShipType)
DAMAGE_ENTITY_RE = re.compile(r"^(?P<pilot>.*?)\[(?P<corp>[^\]]+)\]\((?P<ship>[^)]+)\)$")


def parse_damage_entity(entity: str) -> Tuple[str, str, str]:
    """Parse damage-style entity.

    Returns (pilot, corp, ship). If pattern doesn't match, returns (entity, "", "").
    """

    entity = entity.strip()
    m = DAMAGE_ENTITY_RE.match(entity)
    if not m:
        return entity, "", ""
    return m.group("pilot").strip(), m.group("corp").strip(), m.group("ship").strip()


def parse_rep_party(segment: str) -> Tuple[str, str, str, str]:
    """Parse rep-style party.

    Expected:
      "Pilot [ALL] [CORP] Ship"  or  "Pilot [CORP] Ship"

    Returns (pilot, ship_type, alliance, corp).
    """

    segment = segment.strip()
    # Many log lines render entities ending with a trailing dash, e.g.
    #   "Sleipnir [ECHO.] [INOU] [Turix] -"
    # or (HTML-stripped) "... [Pilot] -".
    # If we don't strip it here, ship-first "Format B" detection won't trigger,
    # and ship names can leak into the pilot column.
    segment = re.sub(r"[\s\-\u2013\u2014]+$", "", segment).strip()

    # "Rep-style" entities can appear in multiple formats depending on the
    # player's log/UI settings.
    #
    # Format A (pilot-first, common):
    #   Pilot [ALL] [CORP] Ship
    #   Pilot [CORP] Ship
    #
    # Format B (ship-first, seen in some logs):
    #   Ship [ALL] [Pilot]
    #   Ship [ALL] [CORP] [Pilot]
    #
    # The older beta parser assumed Format A only, which could shift ship names
    # into the pilot column for reps/ewar lines.

    tickers = [t.strip() for t in re.findall(r"\[([^\]]+)\]", segment)]

    # Default (assume Format A)
    alliance = ""
    corp = ""
    if len(tickers) >= 2:
        alliance = tickers[0]
        corp = tickers[1]
    elif len(tickers) == 1:
        corp = tickers[0]

    pilot = segment.split("[", 1)[0].strip() if "[" in segment else ""
    ship = segment.rsplit("]", 1)[1].strip() if "]" in segment else ""

    # Detect Format B: ends with a ']' (no trailing ship token) and we have
    # at least one bracket token. In this case the leading token is the ship,
    # and the last bracket token is the pilot.
    if segment.endswith("]") and ship == "" and tickers:
        ship_guess = pilot
        pilot_guess = tickers[-1]
        # If we have three tickers, interpret as [ALL][CORP][PILOT]
        if len(tickers) >= 3:
            alliance = tickers[0]
            corp = tickers[1]
        elif len(tickers) == 2:
            alliance = tickers[0]
            corp = ""  # unknown in this format; may be filled later
        elif len(tickers) == 1:
            alliance = ""
            corp = ""
        pilot = pilot_guess
        ship = ship_guess

    # Fallback:
    # Historically we tried to infer "<pilot> <ship>" by splitting the last token
    # as ship type. This is unsafe because pilots can have spaces in their names
    # (e.g. "International Shoe"), which would incorrectly become
    # pilot="International", ship="Shoe".
    #
    # New rule:
    #   - If the original segment has no bracket structure at all, treat it as a
    #     pilot name only (ship unknown).
    #   - If it does have brackets, we can still attempt the old heuristic as a
    #     last resort.
    if not pilot or not ship:
        no_brackets = re.sub(r"\[[^\]]+\]", "", segment).strip()
        no_brackets = re.sub(r"\s+", " ", no_brackets)

        has_any_brackets = ("[" in segment) or ("]" in segment)

        if not has_any_brackets:
            # Pure text; assume it's a pilot name.
            if not pilot:
                pilot = no_brackets
            if not ship:
                ship = ""
        else:
            if " " in no_brackets:
                parts = no_brackets.split(" ")
                if not pilot:
                    pilot = " ".join(parts[:-1]).strip()
                if not ship:
                    ship = parts[-1].strip()
            else:
                if not pilot:
                    pilot = no_brackets
                if not ship:
                    ship = ""

    # Guardrail: some logs occasionally repeat the ship type in the "pilot" bracket,
    # e.g. "to Loki [ECHO.] [Loki] -". In these cases we treat the pilot as unknown
    # to avoid polluting the roster/Pilot_list with ship names.
    if pilot and ship and pilot.strip().lower() == ship.strip().lower():
        pilot = ""

    return pilot, ship, alliance, corp


def parse_entity_any(entity_text: str) -> Tuple[str, str, str, str]:
    """Safety parse.

    - Try rep-style first.
    - If that yields neither corp nor ship, fall back to damage-style.

    Returns (pilot, ship, alliance, corp).
    """

    p, ship, all_, corp = parse_rep_party(entity_text)
    if (not corp) and (not ship):
        p2, corp2, ship2 = parse_damage_entity(entity_text)
        return p2, ship2, "", corp2
    return p, ship, all_, corp
