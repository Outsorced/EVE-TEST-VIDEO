from __future__ import annotations

import csv
import os
import urllib.request
from typing import Dict, List, Set

from .constants import (
    FUZZWORK_BASE,
    INV_TYPES_CSV,
    INV_GROUPS_CSV,
    INV_META_TYPES_CSV,
    INV_META_GROUPS_CSV,
    DGM_TYPE_ATTRIBUTES_CSV,
)
from .prompts import Prompter


def required_sde_paths(sde_dir: str) -> List[str]:
    # invTypes: name -> (typeID, groupID)
    # invGroups: groupID -> groupName
    # invMetaTypes: typeID -> metaGroupID (Tech II, Faction, etc.)
    # invMetaGroups: metaGroupID -> metaGroupName
    return [
        os.path.join(sde_dir, INV_TYPES_CSV),
        os.path.join(sde_dir, INV_GROUPS_CSV),
        os.path.join(sde_dir, INV_META_TYPES_CSV),
        os.path.join(sde_dir, INV_META_GROUPS_CSV),
        os.path.join(sde_dir, DGM_TYPE_ATTRIBUTES_CSV),
    ]


def download_file(url: str, dest: str) -> None:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    print(f"Downloading: {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved to:   {dest}")


def ensure_sde_present(sde_dir: str, prompter: Prompter) -> None:
    missing = [p for p in required_sde_paths(sde_dir) if not os.path.exists(p)]
    if not missing:
        return

    print("Missing SDE CSV file(s):")
    for p in missing:
        print(f" - {p}")

    if not prompter.confirm("Download missing SDE file(s) now?", default=True):
        raise SystemExit("Cannot continue without SDE CSV (needed to avoid ESI lookups on items).")

    for fn in (INV_TYPES_CSV, INV_GROUPS_CSV, INV_META_TYPES_CSV, INV_META_GROUPS_CSV):
        url = f"{FUZZWORK_BASE}/{fn}"
        download_file(url, os.path.join(sde_dir, fn))

    # Optional-but-required for module meta level enrichment.
    fn = DGM_TYPE_ATTRIBUTES_CSV
    url = f"{FUZZWORK_BASE}/{fn}"
    download_file(url, os.path.join(sde_dir, fn))


def _detect_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        sample = f.read(64 * 1024)
    if "<html" in sample.lower() or "<!doctype" in sample.lower():
        raise SystemExit(
            "SDE download does not look like CSV (looks like HTML).\n"
            f"Delete '{path}' and re-run."
        )

    candidates = [",", ";", "\t", "|"]
    lines = [ln for ln in sample.splitlines() if ln.strip()]
    sniff_text = "\n".join(lines[:3]) if lines else sample
    try:
        dialect = csv.Sniffer().sniff(sniff_text, delimiters=candidates)
        return dialect.delimiter
    except Exception:
        # Fallback: pick the delimiter that produces the most consistent split count.
        best = ","
        best_score = -1
        sample_lines = lines[:5]
        for d in candidates:
            counts = [len(ln.split(d)) for ln in sample_lines if ln]
            if not counts:
                continue
            freq: Dict[int, int] = {}
            for c in counts:
                freq[c] = freq.get(c, 0) + 1
            common_count = max(freq, key=lambda k: freq[k])
            score = freq[common_count] * common_count
            if score > best_score:
                best_score = score
                best = d
        return best


def load_item_name_set(sde_dir: str) -> Set[str]:
    """Load invTypes-nodescription.csv and return set(typeName.lower()).

    Supports headerless fuzzwork file: col0=typeID, col2=typeName.
    """

    invtypes_path = os.path.join(sde_dir, INV_TYPES_CSV)
    delim = _detect_delimiter(invtypes_path)

    with open(invtypes_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        first = next(reader, None)
        second = next(reader, None)

    if not first:
        raise SystemExit(f"SDE file is empty: {invtypes_path}")

    norm = [c.strip().lstrip("\ufeff").lower() for c in first]
    header_words = {"typename", "type_name", "type name"}
    has_header = any(w in norm for w in header_words)

    first_is_int = False
    try:
        int((first[0] or "").strip().strip('"'))
        first_is_int = True
    except Exception:
        first_is_int = False

    headerless = (not has_header) and first_is_int

    type_name_idx = 2 if headerless else None
    skip_header = False

    if not headerless:
        possible = {"typename", "type_name", "type name"}
        for i, col in enumerate(norm):
            if col in possible:
                type_name_idx = i
                skip_header = True
                break
        if type_name_idx is None:
            print("\n--- SDE DEBUG ---")
            print(f"File: {invtypes_path}")
            print(f"Detected delimiter: {repr(delim)}")
            print(f"First row fields ({len(first)}): {first}")
            print(f"Second row fields ({len(second) if second else 0}): {second}")
            print("")
            raise SystemExit("SDE appears headered but no typeName column found (see SDE DEBUG).")

    names: Set[str] = set()
    with open(invtypes_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        if skip_header:
            next(reader, None)
        for row in reader:
            if not row or type_name_idx is None or type_name_idx >= len(row):
                continue
            n = (row[type_name_idx] or "").strip().strip('"')
            if n:
                names.add(n.lower())

    mode = "headerless(typeName@col2)" if headerless else "headered"
    print(f"Loaded {len(names):,} item names from SDE ({INV_TYPES_CSV}), delimiter={repr(delim)}, mode={mode}.")
    return names


def load_invtypes_index(sde_dir: str) -> Dict[str, Dict[str, int]]:
    """Load invTypes-nodescription.csv into a name index.

    Returns a dict: typeName.lower() -> {"type_id": int, "group_id": int}

    Supports both fuzzwork's headerless format and headered CSV.
    """

    invtypes_path = os.path.join(sde_dir, INV_TYPES_CSV)
    delim = _detect_delimiter(invtypes_path)

    # Peek header/first row
    with open(invtypes_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        first = next(reader, None)

    if not first:
        raise SystemExit(f"SDE file is empty: {invtypes_path}")

    norm = [c.strip().lstrip("\ufeff").lower() for c in first]

    # Header detection
    has_type_name_header = any(c in ("typename", "type_name", "type name") for c in norm)
    has_type_id_header = any(c in ("typeid", "type_id", "type id") for c in norm)
    has_group_id_header = any(c in ("groupid", "group_id", "group id") for c in norm)

    first_is_int = False
    try:
        int((first[0] or "").strip().strip('"'))
        first_is_int = True
    except Exception:
        first_is_int = False

    headerless = (not has_type_name_header) and first_is_int

    # Column indices
    if headerless:
        type_id_idx = 0
        group_id_idx = 1
        type_name_idx = 2
        skip_header = False
    else:
        type_id_idx = norm.index("typeid") if "typeid" in norm else (norm.index("type_id") if "type_id" in norm else None)
        group_id_idx = norm.index("groupid") if "groupid" in norm else (norm.index("group_id") if "group_id" in norm else None)
        type_name_idx = None
        for i, col in enumerate(norm):
            if col in ("typename", "type_name", "type name"):
                type_name_idx = i
                break
        if type_id_idx is None or group_id_idx is None or type_name_idx is None:
            raise SystemExit("invTypes CSV is headered but required columns were not found.")
        skip_header = True

    out: Dict[str, Dict[str, int]] = {}
    with open(invtypes_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        if skip_header:
            next(reader, None)
        for row in reader:
            if not row:
                continue
            try:
                type_id = int((row[type_id_idx] or "").strip().strip('"'))
                group_id = int((row[group_id_idx] or "").strip().strip('"'))
                name = (row[type_name_idx] or "").strip().strip('"')
            except Exception:
                continue
            if not name:
                continue
            k = name.lower()
            if k not in out:
                out[k] = {"type_id": type_id, "group_id": group_id}

    print(f"Loaded {len(out):,} invTypes name index from SDE ({INV_TYPES_CSV}).")
    return out


def load_meta_level_by_type_id(sde_dir: str) -> Dict[int, int]:
    """Load meta level attributes for typeIDs.

    Uses fuzzwork's dgmTypeAttributes.csv.

    We look for attributeID values commonly used for meta level:
    - 633  (metaLevelOld)
    - 1692 (metaLevel)

    Returns: typeID -> meta_level (int)
    """

    path = os.path.join(sde_dir, DGM_TYPE_ATTRIBUTES_CSV)
    delim = _detect_delimiter(path)

    # Detect header
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter=delim)
        first = next(r, None)

    if not first:
        return {}

    norm = [c.strip().lstrip("\ufeff").lower() for c in first]
    headered = "typeid" in norm or "attributeid" in norm

    # Column indices
    if headered:
        type_idx = norm.index("typeid") if "typeid" in norm else norm.index("type_id")
        attr_idx = norm.index("attributeid") if "attributeid" in norm else norm.index("attribute_id")
        # fuzzwork provides valueInt/valueFloat
        vi_idx = norm.index("valueint") if "valueint" in norm else None
        vf_idx = norm.index("valuefloat") if "valuefloat" in norm else None
        skip = True
    else:
        # Common fuzzwork ordering: typeID, attributeID, valueInt, valueFloat
        type_idx, attr_idx = 0, 1
        vi_idx = 2 if len(first) > 2 else None
        vf_idx = 3 if len(first) > 3 else None
        skip = False

    meta_by_type: Dict[int, int] = {}
    # Prefer metaLevel (1692) over metaLevelOld (633) when both exist.
    pref_attr = {1692: 2, 633: 1}
    pref_seen: Dict[int, int] = {}

    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        r = csv.reader(f, delimiter=delim)
        if skip:
            next(r, None)
        for row in r:
            if not row or len(row) <= max(type_idx, attr_idx):
                continue
            try:
                tid = int((row[type_idx] or "").strip().strip('"'))
                aid = int((row[attr_idx] or "").strip().strip('"'))
            except Exception:
                continue

            if aid not in (633, 1692):
                continue

            val: Optional[int] = None
            if vi_idx is not None and vi_idx < len(row):
                v = (row[vi_idx] or "").strip().strip('"')
                if v:
                    try:
                        val = int(float(v))
                    except Exception:
                        val = None
            if val is None and vf_idx is not None and vf_idx < len(row):
                v = (row[vf_idx] or "").strip().strip('"')
                if v:
                    try:
                        val = int(float(v))
                    except Exception:
                        val = None

            if val is None:
                continue

            pr = pref_attr.get(aid, 0)
            if tid not in pref_seen or pr > pref_seen[tid]:
                pref_seen[tid] = pr
                meta_by_type[tid] = val

    return meta_by_type


def load_invgroups_map(sde_dir: str) -> Dict[int, str]:
    """Load invGroups.csv -> {groupID: groupName}."""
    path = os.path.join(sde_dir, INV_GROUPS_CSV)
    delim = _detect_delimiter(path)
    out: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        header = next(reader, None)
        if not header:
            return out
        norm = [c.strip().lstrip("\ufeff").lower() for c in header]
        # Fuzzwork usually headered: groupID, categoryID, groupName, ...
        if "groupid" in norm:
            gid_i = norm.index("groupid")
        elif "group_id" in norm:
            gid_i = norm.index("group_id")
        else:
            gid_i = 0

        if "groupname" in norm:
            name_i = norm.index("groupname")
        elif "group_name" in norm:
            name_i = norm.index("group_name")
        else:
            name_i = 2

        for row in reader:
            if not row or gid_i >= len(row) or name_i >= len(row):
                continue
            try:
                gid = int((row[gid_i] or "").strip().strip('"'))
            except Exception:
                continue
            name = (row[name_i] or "").strip().strip('"')
            if name:
                out[gid] = name
    return out


def load_invmetatypes_map(sde_dir: str) -> Dict[int, int]:
    """Load invMetaTypes.csv -> {typeID: metaGroupID}."""
    path = os.path.join(sde_dir, INV_META_TYPES_CSV)
    delim = _detect_delimiter(path)
    out: Dict[int, int] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        header = next(reader, None)
        if not header:
            return out
        norm = [c.strip().lstrip("\ufeff").lower() for c in header]
        type_i = norm.index("typeid") if "typeid" in norm else (norm.index("type_id") if "type_id" in norm else 0)
        meta_i = norm.index("metagroupid") if "metagroupid" in norm else (norm.index("metaGroupID".lower()) if "metagroupid" in norm else 1)
        # If header variants differ, fallback
        if "metagroupid" not in norm and "meta_group_id" in norm:
            meta_i = norm.index("meta_group_id")

        for row in reader:
            if not row or type_i >= len(row) or meta_i >= len(row):
                continue
            try:
                tid = int((row[type_i] or "").strip().strip('"'))
                mid = int((row[meta_i] or "").strip().strip('"'))
            except Exception:
                continue
            out[tid] = mid
    return out


def load_invmetagroups_map(sde_dir: str) -> Dict[int, str]:
    """Load invMetaGroups.csv -> {metaGroupID: metaGroupName}."""
    path = os.path.join(sde_dir, INV_META_GROUPS_CSV)
    delim = _detect_delimiter(path)
    out: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delim)
        header = next(reader, None)
        if not header:
            return out
        norm = [c.strip().lstrip("\ufeff").lower() for c in header]
        id_i = norm.index("metagroupid") if "metagroupid" in norm else (norm.index("meta_group_id") if "meta_group_id" in norm else 0)
        name_i = norm.index("metagroupname") if "metagroupname" in norm else (norm.index("meta_group_name") if "meta_group_name" in norm else 1)
        for row in reader:
            if not row or id_i >= len(row) or name_i >= len(row):
                continue
            try:
                mid = int((row[id_i] or "").strip().strip('"'))
            except Exception:
                continue
            name = (row[name_i] or "").strip().strip('"')
            if name:
                out[mid] = name
    return out
