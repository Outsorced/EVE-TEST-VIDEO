from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def write_csv(
    path: str | Path,
    rows: List[Dict[str, Any]],
    headers: List[str],
    *,
    metadata: Optional[Dict[str, Any]] = None,
    metadata_headers: Optional[Iterable[str]] = None,
) -> None:
    path = Path(path)
    if not rows:
        print(f"No entries found for {path.name}, CSV not created.")
        return
    meta_headers = list(metadata_headers or [])
    if metadata is not None and not meta_headers:
        meta_headers = list(metadata.keys())
    headers_out = list(headers)
    for h in meta_headers:
        if h not in headers_out:
            headers_out.append(h)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers_out, extrasaction="ignore")
        w.writeheader()
        if metadata:
            for r in rows:
                rr = dict(r)
                for k, v in metadata.items():
                    rr[k] = v
                w.writerow(rr)
        else:
            w.writerows(rows)
    print(f"Exported {len(rows)} rows -> {path}")
