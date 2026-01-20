import csv
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta
from types import SimpleNamespace

from eve_combat_parser import exporter
from eve_combat_parser.constants import METADATA_HEADERS
from eve_combat_parser.cli import _write_fight_summary


class TestMetadataColumns(unittest.TestCase):
    def test_metadata_in_exporter_csv(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            out_csv = root / "export.csv"

            metadata = {
                "schema_version": "1",
                "parser_version": "test",
                "run_id": "001",
            }

            headers = ["timestamp", "amount"]
            rows = [{"timestamp": "2026.01.15 23:55:00", "amount": 1}]

            exporter.write_csv(
                out_csv,
                rows,
                headers,
                metadata=metadata,
                metadata_headers=METADATA_HEADERS,
            )

            with out_csv.open("r", encoding="utf-8", newline="") as f:
                hdr = next(csv.reader(f))

            self.assertEqual(hdr[:2], headers)
            self.assertEqual(hdr[-3:], METADATA_HEADERS)

    def test_metadata_in_summary_csv(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)

            metadata = {
                "schema_version": "1",
                "parser_version": "test",
                "run_id": "001",
            }

            start = datetime(2026, 1, 15, 23, 55, 0)
            end = start + timedelta(seconds=10)
            win = SimpleNamespace(start=start, end=end)

            _write_fight_summary(
                root,
                1,
                win,
                [],
                counts={"damage_done_players.csv": 0},
                item_names_lower=set(),
                ship_meta=None,
                metadata=metadata,
            )

            fight_summary = root / "summary" / "fight_summary.csv"
            self.assertTrue(fight_summary.exists(), "fight_summary.csv was not created")

            with fight_summary.open("r", encoding="utf-8", newline="") as f:
                hdr = next(csv.reader(f))

            self.assertEqual(hdr[-3:], METADATA_HEADERS)


if __name__ == "__main__":
    unittest.main()
