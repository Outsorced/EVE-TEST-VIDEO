from __future__ import annotations

"""Ship metadata enrichment.

Goal: add ship class (EVE group name) and tech level (T1/T2/T3) for ship types
found in log rows.

Design constraints (based on user request):
- Primary data source is fuzzwork SDE CSV "invTypes-nodescription.csv" so the user
  can update the dataset themselves.
- Redundancy: if the local dataset is missing a type, optionally look up ESI.
- Deterministic: if ESI is disabled/unavailable, we still run, leaving UNKNOWN.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .sde import (
    load_invgroups_map,
    load_invmetagroups_map,
    load_invmetatypes_map,
    load_invtypes_index,
)
from .esi import esi_get_type_id, esi_get_type_info, esi_get_group_info


MONTH_ABBR_LOWER = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "may",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "oct",
    11: "nov",
    12: "dec",
}


def _tech_from_meta_group(meta_group_name: str) -> str:
    mg = (meta_group_name or "").strip().lower()
    if mg in ("tech ii", "tech 2"):
        return "T2"
    if mg in ("tech iii", "tech 3"):
        return "T3"
    return "T1"


_DRONE_NAME_MARKERS = {
    # Common drone families (best-effort). Used only when ESI group name isn't available.
    "acolyte",
    "hobgoblin",
    "hornet",
    "warrior",
    "hammerhead",
    "vespa",
    "valkyrie",
    "ogre",
    "gecko",
    "infiltrator",
    "praetor",
    "berserker",
    # Sentries
    "garde",
    "curator",
    "bouncer",
    "warden",
}


def _guess_is_drone_or_fighter(ship_name: str, ship_class: str) -> Tuple[bool, bool]:
    """Return (is_drone, is_fighter) best-effort.

    Prefer ship_class (EVE group name) when available. Fallback to name heuristics.
    Fighters are treated separately from drones.
    """

    c = (ship_class or "").strip().lower()
    if "fighter" in c:
        return False, True
    if "drone" in c:
        return True, False

    n = (ship_name or "").strip().lower()
    # Fallback: some fighters include the word fighter in the name.
    if "fighter" in n:
        return False, True
    # Name marker fallback for drones.
    for m in _DRONE_NAME_MARKERS:
        if n.startswith(m + " ") or n.startswith(m + "ii") or n == m or m in n:
            return True, False
    return False, False


def _derive_hull_rarity(meta_group_name: str) -> str:
    """Return a game-style hull rarity label.

    Uses SDE meta group names when possible (most accurate / stable):
    - Tech II / Tech III
    - Faction / Navy / Pirate / Storyline / Officer ...

    If unknown, returns Standard.
    """

    mg = (meta_group_name or "").strip()
    if not mg:
        return "Standard"

    # Normalize common SDE labels
    low = mg.lower()
    if low in ("tech ii", "tech 2"):
        return "Tech II"
    if low in ("tech iii", "tech 3"):
        return "Tech III"
    if low == "faction":
        return "Faction"
    if low == "navy":
        return "Navy"
    if low == "pirate":
        return "Pirate"
    if low == "storyline":
        return "Storyline"
    if low == "officer":
        return "Officer"
    if low == "deadspace":
        return "Deadspace"
    if low == "tournament":
        return "Tournament"

    # Otherwise keep the SDE name (it's already game-style)
    return mg


# Best-effort hull rarity/category.
# This is intentionally heuristic and stable. It classifies common families:
# - Tech (T2/T3)
# - Navy
# - Pirate
# - Faction
# - Standard (default)
#
# Note: Some hulls blur categories; if you want full control, we can later
# add an overrides CSV in the sde dir.

def _fallback_rarity_from_name(ship_name: str) -> str:
    """Name-based fallback when meta group is missing."""
    n = (ship_name or "").strip().lower()
    if "navy issue" in n or "fleet issue" in n or "state issue" in n or "imperial navy" in n:
        return "Navy"
    if "issue" in n and "navy" not in n:
        return "Faction"
    return "Standard"


@dataclass
class ShipMetaResolver:
    sde_dir: str
    cache: Dict[str, Any]
    enable_esi: bool = True
    esi_sleep_s: float = 0.2

    def __post_init__(self) -> None:
        self._inv_index = load_invtypes_index(self.sde_dir)
        self._group_name_by_id = load_invgroups_map(self.sde_dir)
        self._meta_group_by_type = load_invmetatypes_map(self.sde_dir)
        self._meta_group_name_by_id = load_invmetagroups_map(self.sde_dir)


    def resolve_extended(self, ship_name: str) -> Tuple[str, str, str]:
        # Return (ship_class, ship_tech, hull_rarity) for a ship type name.

        ship_name = (ship_name or "").strip()
        if not ship_name:
            return "", "", ""

        key = ship_name.lower()
        name_cache = self.cache.setdefault("ship_meta_by_name", {})
        if key in name_cache:
            v = name_cache[key] or {}
            return (
                (v.get("ship_class") or ""),
                (v.get("ship_tech") or ""),
                (v.get("hull_rarity") or ""),
            )

        type_id: Optional[int] = None
        group_id: Optional[int] = None

        local = self._inv_index.get(key)
        if local:
            type_id = local.get("type_id")
            group_id = local.get("group_id")

        # Redundancy: resolve type_id via ESI if missing
        if (type_id is None) and self.enable_esi:
            try:
                type_id = esi_get_type_id(ship_name, self.cache, self.esi_sleep_s)
            except Exception:
                type_id = None

        # If we have a type_id but no group_id, ask ESI for type info
        if (type_id is not None) and (group_id is None) and self.enable_esi:
            try:
                info = esi_get_type_info(int(type_id), self.cache, self.esi_sleep_s)
                gid = info.get("group_id")
                if isinstance(gid, int):
                    group_id = gid
            except Exception:
                group_id = None

        # Resolve group name (ship class)
        group_name = ""
        if group_id is not None:
            group_name = (self._group_name_by_id.get(int(group_id)) or "").strip()
        if not group_name and group_id is not None and self.enable_esi:
            try:
                ginfo = esi_get_group_info(int(group_id), self.cache, self.esi_sleep_s)
                group_name = (ginfo.get("name") or "").strip()
            except Exception:
                group_name = ""

        ship_class = group_name or "UNKNOWN"

        # Resolve meta group name (rarity) using SDE meta tables
        meta_group_name = ""
        if type_id is not None:
            mgid = self._meta_group_by_type.get(int(type_id))
            if mgid is not None:
                meta_group_name = (self._meta_group_name_by_id.get(int(mgid)) or "").strip()

        hull_rarity = _derive_hull_rarity(meta_group_name)
        if hull_rarity == "Standard" and not meta_group_name:
            hull_rarity = _fallback_rarity_from_name(ship_name)

        ship_tech = _tech_from_meta_group(meta_group_name)

        name_cache[key] = {
            "ship_class": ship_class,
            "ship_tech": ship_tech,
            "hull_rarity": hull_rarity,
        }
        return ship_class, ship_tech, hull_rarity

    def kind(self, ship_name: str) -> str:
        """Classify a type name as 'ship', 'drone', 'fighter', or 'unknown'."""
        ship_name = (ship_name or "").strip()
        if not ship_name:
            return "unknown"
        try:
            ship_class, _tech, _rarity = self.resolve_extended(ship_name)
        except Exception:
            ship_class = ""
        is_drone, is_fighter = _guess_is_drone_or_fighter(ship_name, ship_class)
        if is_fighter:
            return "fighter"
        if is_drone:
            return "drone"
        if (ship_class or "").strip() and (ship_class or "").strip().upper() != "UNKNOWN":
            return "ship"
        return "unknown"

    def resolve(self, ship_name: str) -> Tuple[str, str]:
        # Backwards-compatible convenience.
        c, t, _r = self.resolve_extended(ship_name)
        return c, t

    def annotate_row(self, row: Dict[str, Any]) -> None:
        for side in ("source", "target"):
            ship = (row.get(f"{side}_ship_type") or "").strip()
            if not ship:
                row[f"{side}_ship_class"] = ""
                row[f"{side}_ship_tech"] = ""
                row[f"{side}_hull_rarity"] = ""
                continue
            cls, tech, rarity = self.resolve_extended(ship)
            row[f"{side}_ship_class"] = cls
            row[f"{side}_ship_tech"] = tech
            row[f"{side}_hull_rarity"] = rarity

    