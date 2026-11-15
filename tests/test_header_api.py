"""Header and dictionary reads must not decode or require the record section."""
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import qvd


class HeaderApiTests(unittest.TestCase):
    def test_real_file_metadata_and_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "данные.qvd"
            qvd.write_arrow(pa.record_batch({"pk": [1, 2, 2], "Имя": ["А", None, "Б"]}), str(path))
            header = qvd.read_qvd_header(str(path))
            self.assertEqual(header["num_records"], 3)
            self.assertEqual([item["name"] for item in header["fields"]], ["pk", "Имя"])
            self.assertEqual(set(qvd.read_qvd_symbols(str(path), "pk")), {1, 2})
            with self.assertRaises(ValueError):
                qvd.read_qvd_symbols(str(path), "missing")
            with self.assertRaises(ValueError):
                qvd.read_qvd_header(str(path), max_header_bytes=10)

    def test_header_read_with_absent_binary_section(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.qvd"
            qvd.write_arrow(pa.record_batch({"pk": [1, 2]}), str(path))
            raw = path.read_bytes()
            path.write_bytes(raw[:raw.index(b"\0") + 1])
            self.assertEqual(qvd.read_qvd_header(str(path))["num_records"], 2)
            with self.assertRaises(ValueError):
                qvd.read_qvd_symbols(str(path), "pk")

    def test_symbols_do_not_require_row_indices(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "symbols.qvd"
            qvd.write_arrow(pa.record_batch({"pk": [1, 2, 2]}), str(path))
            header = qvd.read_qvd_header(str(path))
            raw = path.read_bytes()
            binary_start = raw.index(b"\0") + 1
            path.write_bytes(raw[:binary_start + header["offset"]])
            self.assertEqual(set(qvd.read_qvd_symbols(str(path), "pk")), {1, 2})


if __name__ == "__main__":
    unittest.main()
