import unittest
import json
import tempfile
from pathlib import Path
from unittest import mock

import cluster


class LayoutTest(unittest.TestCase):
    def test_two_shards(self):
        rows = cluster.hosts({"business_shards": 2})
        self.assertEqual(len(rows), 12)
        self.assertEqual(sorted(r["server"] for r in rows), list(range(2, 14)))
        self.assertEqual([r["server"] for r in rows if r["org"] == 1], [2, 6, 7])
        self.assertEqual([r["rpc"] for r in rows if r["org"] == 1], [12309, 12301, 12305])

    def test_all_sizes(self):
        for count in [2, 4, 8, 16, 32]:
            rows = cluster.hosts({"business_shards": count})
            self.assertEqual(len(rows), 4 * (count + 1))
            endpoints = [(r["ip"], r[k]) for r in rows for k in ["p2p", "rpc", "sync", "engine", "runtime"]]
            self.assertEqual(len(set(endpoints)), len(endpoints))

    def test_safety(self):
        for path in ["/", "/data1", "/root", "/root/du_sharding/../other"]:
            with self.assertRaises(ValueError):
                cluster.checked_path(path)
        self.assertEqual(cluster.checked_path("/root/du_sharding/runs/a"), Path("/root/du_sharding/runs/a"))

    def test_frozen_run_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config.used.json").write_text(json.dumps({"tx_count": 100000}))
            with mock.patch.object(cluster, "run_root", return_value=root):
                cluster.require_run_config({"tx_count": 100000})
                with self.assertRaises(ValueError):
                    cluster.require_run_config({"tx_count": 500000})

    def test_preserve_run_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "out").mkdir()
            (root / "out/run.log").write_text("existing measurement\n")
            with mock.patch.object(cluster, "run_root", return_value=root):
                with self.assertRaises(ValueError):
                    cluster.benchmark({})
            self.assertEqual((root / "out/run.log").read_text(), "existing measurement\n")


if __name__ == "__main__":
    unittest.main()
