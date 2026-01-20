from __future__ import annotations

"""Module metadata enrichment.

Adds fields for the `module` column in combat outputs:
- module_tech_level: T1/T2/T3 (derived from SDE meta group name)
- module_meta_level: numeric meta level (from dgmTypeAttributes)
- module_meta_group: SDE meta group name (e.g., Tech II, Faction, Deadspace)

Data sources (user-updatable, fuzzwork dump):
- invTypes-nodescription.csv (typeName -> typeID)
- invMetaTypes.csv + invMetaGroups.csv (typeID -> meta group name)
- dgmTypeAttributes.csv (typeID -> meta level)

Redundancy: if module name isn't found locally, we may use ESI name->typeID
lookup (cached) when enabled.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .sde import (
    load_invmetagroups_map,
    load_invmetatypes_map,
    load_invtypes_index,
    load_meta_level_by_type_id,
)
from .esi import esi_get_type_id, esi_get_type_info


def _tech_from_meta_group(meta_group_name: str) -> str:
    mg = (meta_group_name or "").strip().lower()
    if mg in ("tech ii", "tech 2"):
        return "T2"
    if mg in ("tech iii", "tech 3"):
        return "T3"
    return "T1"


@dataclass
class ModuleMetaResolver:
    sde_dir: str
    cache: Dict[str, Any]
    enable_esi: bool = True
    esi_sleep_s: float = 0.2

    def __post_init__(self) -> None:
        self._inv_index = load_invtypes_index(self.sde_dir)
        self._meta_group_by_type = load_invmetatypes_map(self.sde_dir)
        self._meta_group_name_by_id = load_invmetagroups_map(self.sde_dir)
        self._meta_level_by_type = load_meta_level_by_type_id(self.sde_dir)

    def resolve(self, module_name: str) -> Tuple[str, str, str]:
        """Return (module_tech_level, module_meta_level_str, module_meta_group)."""

        module_name = (module_name or "").strip()
        if not module_name:
            return "", "", ""

        key = module_name.lower()
        name_cache = self.cache.setdefault("module_meta_by_name", {})
        if key in name_cache:
            v = name_cache[key] or {}
            return (
                v.get("module_tech_level") or "",
                v.get("module_meta_level") or "",
                v.get("module_meta_group") or "",
            )

        type_id: Optional[int] = None
        local = self._inv_index.get(key)
        if local:
            type_id = local.get("type_id")

        if (type_id is None) and self.enable_esi:
            try:
                type_id = esi_get_type_id(module_name, self.cache, self.esi_sleep_s)
            except Exception:
                type_id = None

        # If ESI returned a type_id, try to backfill in meta level cache by loading type info.
        # (Meta level comes from SDE; if user hasn't updated SDE, we still may not have it.)
        module_tech = ""
        meta_level = ""

        meta_group_name = ""
        if type_id is not None:
            mgid = self._meta_group_by_type.get(int(type_id))
            if mgid is not None:
                meta_group_name = (self._meta_group_name_by_id.get(int(mgid)) or "").strip()
            module_tech = _tech_from_meta_group(meta_group_name)

            ml = self._meta_level_by_type.get(int(type_id))
            if ml is not None:
                meta_level = str(int(ml))

        # If tech is empty but we have a type_id and ESI is enabled, at least ensure we know it's a module.
        if not module_tech and type_id is not None and self.enable_esi:
            try:
                _ = esi_get_type_info(int(type_id), self.cache, self.esi_sleep_s)
            except Exception:
                pass
            module_tech = module_tech or "T1"

        name_cache[key] = {
            "module_tech_level": module_tech,
            "module_meta_level": meta_level,
            "module_meta_group": meta_group_name,
        }
        return module_tech, meta_level, meta_group_name

    def annotate_row(self, row: Dict[str, Any]) -> None:
        mod = (row.get("module") or "").strip()
        if not mod:
            row["module_tech_level"] = ""
            row["module_meta_level"] = ""
            row["module_meta_group"] = ""
            return
        tech, ml, mg = self.resolve(mod)
        row["module_tech_level"] = tech
        row["module_meta_level"] = ml
        row["module_meta_group"] = mg
