from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set, Tuple


def _norm_name(s: str) -> str:
    return (s or "").strip().lower()


_ROMAN_SUFFIXES = (" i", " ii", " iii", " iv", " v", " vi", " vii", " viii", " ix", " x")

# Keywords that strongly indicate an item/drone/ammo string.
# We bucket these into *_drones outputs so they don't pollute pilot lists.
# Keywords that strongly indicate a drone/fighter.
_DRONE_KEYWORDS = (
    "drone",
    "sentry",
    "fighter",
)

# Common drone family name prefixes that appear without the word "drone".
# (e.g. "Vespa EC-600")
_DRONE_PREFIXES = (
    "warrior",
    "hobgoblin",
    "hammerhead",
    "ogre",
    "infiltrator",
    "praetor",
    "vespa",
    "hornet",
    "garde",
    "curator",
    "warden",
    "bouncer",
    "gecko",
)

# Keywords that strongly indicate ammunition/charges.
_CHARGE_KEYWORDS = (
    "missile",
    "torpedo",
    "rocket",
    "bomb",
    "charge",
    "crystal",
    "shell",
    "slug",
    "projectile",
    "warhead",
    "ammo",
)


def looks_like_drone(name: str, item_names_lower: Set[str]) -> bool:
    """Best-effort check for drones/fighters accidentally parsed as pilots."""

    n = _norm_name(name)
    if not n:
        return False

    # If we have SDE names, require membership for most cases to reduce false positives.
    in_sde = (not item_names_lower) or (n in item_names_lower)

    # Strong signals.
    if any(kw in n for kw in _DRONE_KEYWORDS) and in_sde:
        return True

    # Family prefixes (covers "Vespa EC-600" etc.)
    if any(n.startswith(pref + " ") or n == pref for pref in _DRONE_PREFIXES) and in_sde:
        return True

    # Electronic warfare drones use patterns like "EC-600".
    if " ec-" in n and in_sde:
        return True

    # Many drones have roman numerals suffixes.
    if n.endswith(_ROMAN_SUFFIXES) and in_sde:
        return True

    return False


def looks_like_charge(name: str, item_names_lower: Set[str]) -> bool:
    """Best-effort check for ammo/charges accidentally parsed as pilots."""

    n = _norm_name(name)
    if not n:
        return False

    in_sde = (not item_names_lower) or (n in item_names_lower)

    # Very strong signals: keyword + (in SDE, or obvious suffix token).
    if any(kw in n for kw in _CHARGE_KEYWORDS):
        if in_sde:
            return True

        # Without SDE, only accept if it clearly ends with an ammo token.
        tail = n.split()[-1] if n.split() else ""
        if tail in {
            "missile",
            "torpedo",
            "rocket",
            "bomb",
            "charge",
            "crystal",
            "shell",
            "slug",
            "projectile",
            "warhead",
            "ammo",
        }:
            return True

    return False


def looks_like_drone_or_item(name: str, item_names_lower: Set[str]) -> bool:
    """Back-compat helper."""

    return looks_like_drone(name, item_names_lower) or looks_like_charge(name, item_names_lower)


def _looks_like_npc(
    pilot: str,
    ship_type: str,
    corp: str,
    alliance: str,
    item_names_lower: Set[str],
    known_players: Set[str],
) -> bool:
    """Heuristic NPC detector.

    Rules (priority order):
      1) If corp/alliance ticker is present -> PLAYER (not NPC)
      2) If pilot name has been seen elsewhere with a corp/alliance ticker -> PLAYER
      3) Otherwise (no ticker) -> NPC

    Additionally, if ship_type is blank and the pilot name matches an SDE invTypes typeName,
    we treat that pilot name as the NPC ship type for readability.

    This matches the user's assumption that all player entities include corp ticker in logs.
    """

    if corp or alliance:
        return False

    p = _norm_name(pilot)
    if p and p in known_players:
        return False

    return True


def classify_other_party(
    row: Dict[str, Any],
    other_prefix: str,
    item_names_lower: Set[str],
    known_players: Set[str],
) -> Tuple[str, Dict[str, Any]]:
    """Classify the other party as one of: 'players', 'npc', 'drones'.

    Returns (bucket_name, normalized_row).
    """

    pilot = str(row.get(f"{other_prefix}_pilot", "") or "")
    ship_type = str(row.get(f"{other_prefix}_ship_type", "") or "")
    corp = str(row.get(f"{other_prefix}_corp", "") or "")
    alliance = str(row.get(f"{other_prefix}_alliance", "") or "")

    # 0) Drones -> separate bucket
    if looks_like_drone(pilot, item_names_lower):
        if not ship_type and pilot:
            new_row = dict(row)
            new_row[f"{other_prefix}_ship_type"] = pilot
            return "drones", new_row
        return "drones", row

    # 0b) Charges/ammo -> separate bucket
    if looks_like_charge(pilot, item_names_lower):
        if not ship_type and pilot:
            new_row = dict(row)
            new_row[f"{other_prefix}_ship_type"] = pilot
            return "charges", new_row
        return "charges", row

    # 1) NPC vs Player
    is_npc = _looks_like_npc(pilot, ship_type, corp, alliance, item_names_lower, known_players)
    if not is_npc:
        return "players", row

    # normalize: if NPC appears only as a pilot name, and it matches an SDE typeName,
    # treat that as ship_type too for readability.
    if not ship_type and pilot and _norm_name(pilot) in item_names_lower:
        new_row = dict(row)
        new_row[f"{other_prefix}_ship_type"] = pilot
        return "npc", new_row

    return "npc", row


def split_rows_players_npc_drones_charges(
    rows: Iterable[Dict[str, Any]],
    other_prefix: str,
    item_names_lower: Set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split rows into (players_rows, npc_rows, drones_rows, charges_rows) based on the other party."""

    rows_list = list(rows)

    # Pass 1: learn which pilot names definitely represent players (corp/alliance ticker present somewhere).
    known_players: Set[str] = set()
    for r in rows_list:
        pilot = str(r.get(f"{other_prefix}_pilot", "") or "")
        corp = str(r.get(f"{other_prefix}_corp", "") or "")
        alliance = str(r.get(f"{other_prefix}_alliance", "") or "")
        if corp or alliance:
            p = _norm_name(pilot)
            if p:
                known_players.add(p)

    players: List[Dict[str, Any]] = []
    npc: List[Dict[str, Any]] = []
    drones: List[Dict[str, Any]] = []
    charges: List[Dict[str, Any]] = []

    for r in rows_list:
        bucket, norm = classify_other_party(
            r,
            other_prefix=other_prefix,
            item_names_lower=item_names_lower,
            known_players=known_players,
        )
        if bucket == "players":
            players.append(norm)
        elif bucket == "npc":
            npc.append(norm)
        elif bucket == "drones":
            drones.append(norm)
        else:
            charges.append(norm)

    return players, npc, drones, charges


def build_known_players(rows: Iterable[Dict[str, Any]]) -> Set[str]:
    """Return a set of pilot names (normalized) that are definitely players.

    A pilot is considered a "known player" if they appear anywhere with a
    corp or alliance ticker present.

    We use this to avoid misclassifying players as NPCs when their ticker is
    missing on some lines.
    """

    known: Set[str] = set()
    for r in rows:
        for side in ("source", "target"):
            pilot = str(r.get(f"{side}_pilot", "") or "")
            corp = str(r.get(f"{side}_corp", "") or "")
            alliance = str(r.get(f"{side}_alliance", "") or "")
            if corp or alliance:
                p = _norm_name(pilot)
                if p:
                    known.add(p)
    return known


def classify_party_kind(
    *,
    pilot: str,
    ship_type: str,
    corp: str,
    alliance: str,
    item_names_lower: Set[str],
    known_players: Set[str],
) -> str:
    """Classify a single entity into: player | npc | drone | charge.

    This is used for adding explicit entity-kind columns to outputs, and for
    avoiding drones/charges polluting player rosters/DBs.
    """

    if looks_like_drone(pilot, item_names_lower):
        return "drone"
    if looks_like_charge(pilot, item_names_lower):
        return "charge"

    is_npc = _looks_like_npc(pilot, ship_type, corp, alliance, item_names_lower, known_players)
    return "npc" if is_npc else "player"


# Backwards-compatible helper (older code expects 3-way split).
def split_rows_players_npc_drones(
    rows: Iterable[Dict[str, Any]],
    other_prefix: str,
    item_names_lower: Set[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    p, n, d, c = split_rows_players_npc_drones_charges(rows, other_prefix, item_names_lower)
    # Charges are treated like "drones/items" in older mode; append them to drones.
    return p, n, d + c
