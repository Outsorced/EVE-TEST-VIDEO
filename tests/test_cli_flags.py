import tempfile
import unittest
from pathlib import Path
from unittest import mock

from eve_combat_parser import cli


def _write_min_sde(sde_dir: Path) -> None:
    sde_dir.mkdir(parents=True, exist_ok=True)
    (sde_dir / "invTypes-nodescription.csv").write_text(
        "typeID,groupID,typeName\n"
        "1,25,Rifter\n"
        "2,100,Light Missile\n",
        encoding="utf-8",
    )
    (sde_dir / "invGroups.csv").write_text(
        "groupID,categoryID,groupName\n"
        "25,6,Frigate\n"
        "100,8,Missile\n",
        encoding="utf-8",
    )
    (sde_dir / "invMetaTypes.csv").write_text(
        "typeID,metaGroupID\n"
        "1,1\n"
        "2,1\n",
        encoding="utf-8",
    )
    (sde_dir / "invMetaGroups.csv").write_text(
        "metaGroupID,metaGroupName\n"
        "1,Tech I\n",
        encoding="utf-8",
    )
    (sde_dir / "dgmTypeAttributes.csv").write_text(
        "typeID,attributeID,valueInt,valueFloat\n"
        "1,633,0,0\n"
        "2,633,0,0\n",
        encoding="utf-8",
    )


def _write_min_log(log_root: Path) -> None:
    log_root.mkdir(parents=True, exist_ok=True)
    (log_root / "sample.txt").write_text(
        "Listener: Attacker\n"
        "[ 2026.01.15 23:55:34 ] (combat) 123 to TargetPilot[TGT](Rifter) - Light Missile - Hits\n",
        encoding="utf-8",
    )


class TestCliFlags(unittest.TestCase):
    def test_offline_missing_sde_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            logs = root / "logs"
            _write_min_log(logs)
            sde = root / "sde"
            out = root / "output"

            with mock.patch("builtins.input", side_effect=AssertionError("prompted")):
                with self.assertRaises(SystemExit):
                    cli.main(
                        [
                            "--log-folder",
                            str(logs),
                            "--output-folder",
                            str(out),
                            "--sde-dir",
                            str(sde),
                            "--offline",
                            "--no-open",
                        ]
                    )

    def test_yes_suppresses_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            logs = root / "logs"
            _write_min_log(logs)
            sde = root / "sde"
            _write_min_sde(sde)
            out = root / "output"

            with mock.patch("builtins.input", side_effect=AssertionError("prompted")):
                rc = cli.main(
                    [
                        "--log-folder",
                        str(logs),
                        "--output-folder",
                        str(out),
                        "--sde-dir",
                        str(sde),
                        "--yes",
                        "--no-esi",
                        "--no-open",
                    ]
                )
            self.assertEqual(rc, 0)

    def test_no_open_skips_open_folder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            logs = root / "logs"
            _write_min_log(logs)
            sde = root / "sde"
            _write_min_sde(sde)
            out = root / "output"

            with mock.patch.object(cli, "_open_folder", side_effect=AssertionError("opened")):
                rc = cli.main(
                    [
                        "--log-folder",
                        str(logs),
                        "--output-folder",
                        str(out),
                        "--sde-dir",
                        str(sde),
                        "--yes",
                        "--no-esi",
                        "--no-open",
                    ]
                )
            self.assertEqual(rc, 0)
