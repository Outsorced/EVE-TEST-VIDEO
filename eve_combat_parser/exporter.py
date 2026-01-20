from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List


def write_csv(path: str | Path, rows: List[Dict[str, Any]], headers: List[str]) -> None:
    path = Path(path)
    if not rows:
        print(f"No entries found for {path.name}, CSV not created.")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"Exported {len(rows)} rows -> {path}")
