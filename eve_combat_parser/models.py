from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class AffiliationRecord:
    alliance: str
    first_seen: datetime
    last_seen: datetime


@dataclass
class ShipStateEvent:
    t: datetime
    ship: Optional[str]  # None == disembark boundary
    alliance: str = ""
    corp: str = ""
