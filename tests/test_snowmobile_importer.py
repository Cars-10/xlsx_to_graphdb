import io
import json
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import snowmobile_importer as smi


class TestSpreadsheetParser(unittest.TestCase):
    def _make_excel_with_header_dup(self):
        df = pd.DataFrame(
            [
                ["Number", "Name", "Type", "Source"],
                [123, "Engine", "WTPart", "Windchill"],
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df.to_excel(writer, sheet_name="Parts", index=False, header=False)
            return f.name

    def test_parse_parts(self):
        path = self._make_excel_with_header_dup()
        parser = smi.SpreadsheetParser(path)
        parts = parser.parse_parts()
        self.assertIn("123", parts)
        self.assertEqual(parts["123"]["name"], "Engine")


class FakeHTTPResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestGraphDBClient(unittest.TestCase):
    @patch("snowmobile_importer.urlopen")
    def test_verify_connection(self, mock_urlopen):
        body = json.dumps([{ "id": "Snowmobile" }]).encode()
        mock_urlopen.return_value = FakeHTTPResponse(body)
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        self.assertTrue(client.verify_connection())


class TestImportPipeline(unittest.TestCase):
    def _make_excel(self):
        df = pd.DataFrame(
            [
                ["Number", "Name"],
                [111, "Frame"],
                [222, "Ski"],
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
            return f.name

    def _make_bom(self):
        df = pd.DataFrame({
            "Number": [111],
            "Component Id": [222],
        })
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df.to_csv(f.name, index=False)
            return f.name

    def test_import_dry_run_counts(self):
        excel = self._make_excel()
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        total, chunks = smi.import_data(excel_path=excel, bom_csv_path=None, client=client, batch_size=50, dry_run=True)
        self.assertEqual(total, 6)  # 2 parts * 3 triples each
        self.assertEqual(chunks, 1)

    def test_import_with_bom(self):
        excel = self._make_excel()
        bom = self._make_bom()
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        total, _ = smi.import_data(excel_path=excel, bom_csv_path=bom, client=client, batch_size=10, dry_run=True)
        self.assertEqual(total, 7)  # 6 part triples + 1 relationship triple

    def _make_bom_by_name(self):
        df = pd.DataFrame({
            "Parent Name": ["Frame"],
            "Child Name": ["Ski"],
        })
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df.to_csv(f.name, index=False)
            return f.name

    def test_import_bom_by_name(self):
        excel = self._make_excel()
        bom = self._make_bom_by_name()
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        total, _ = smi.import_data(excel_path=excel, bom_csv_path=bom, client=client, batch_size=10, dry_run=True, bom_by_name=True)
        self.assertEqual(total, 7)

    def test_generate_bom_by_name_and_import(self):
        # Build number-based BOM
        df = pd.DataFrame([
            ["Number", "Name"],
            [111, "Frame"],
            [222, "Ski"],
        ])
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
            excel = f.name
        df_bom = pd.DataFrame({
            "Number": [111],
            "Component Id": [222],
        })
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df_bom.to_csv(f.name, index=False)
            bom_num = f.name
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as out:
            out_name = out.name
        count = smi.generate_bom_by_name_file(excel, bom_num, out_name)
        self.assertGreater(count, 0)
        # Now import using the generated name-based BOM
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        total, _ = smi.import_data(excel_path=excel, bom_csv_path=out_name, client=client, batch_size=10, dry_run=True, bom_by_name=True)
        self.assertEqual(total, 7)

    def test_emit_bom_name_candidates(self):
        df_parts = pd.DataFrame([
            ["Number", "Name"],
            [1, "A"],
            [2, "B"],
        ])
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df_parts.to_excel(writer, sheet_name="P", index=False, header=False)
            excel = f.name
        df_bom = pd.DataFrame({"Number": [1], "Component Id": [2]})
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df_bom.to_csv(f.name, index=False)
            bom_num = f.name
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as out:
            out_name = out.name
        rows = smi.emit_bom_name_candidates(excel, bom_num, out_name)
        self.assertEqual(rows, 1)


class TestErrorHandling(unittest.TestCase):
    def test_missing_excel(self):
        client = smi.GraphDBClient("http://127.0.0.1:7200", "starwars")
        with self.assertRaises(Exception):
            smi.import_data(excel_path="/no/such/file.xlsx", bom_csv_path=None, client=client, dry_run=True)

    def test_ambiguous_names_strict(self):
        df = pd.DataFrame([
            ["Number", "Name"],
            [101, "Bracket"],
            [102, "Bracket"],
            [200, "Frame"],
        ])
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
            excel = f.name
        df_bom = pd.DataFrame({
            "Parent Name": ["Bracket"],
            "Child Name": ["Frame"],
        })
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df_bom.to_csv(f.name, index=False)
            bom = f.name
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        with self.assertRaises(RuntimeError):
            smi.import_data(excel_path=excel, bom_csv_path=bom, client=client, dry_run=True, bom_by_name=True, strict_names=True)

    def test_parse_bom_by_name_reversed_columns(self):
        # Recreate a simple Excel like in TestImportPipeline
        df = pd.DataFrame(
            [
                ["Number", "Name"],
                [111, "Frame"],
                [222, "Ski"],
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            with pd.ExcelWriter(f.name) as writer:
                df.to_excel(writer, sheet_name="Sheet1", index=False, header=False)
            excel = f.name
        df = pd.DataFrame({
            "Child Name": ["Ski"],
            "Parent Name": ["Frame"],
        })
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            df.to_csv(f.name, index=False)
            bom = f.name
        client = smi.GraphDBClient("http://127.0.0.1:7200", "Snowmobile")
        total, _ = smi.import_data(excel_path=excel, bom_csv_path=bom, client=client, batch_size=10, dry_run=True, bom_by_name=True)
        self.assertEqual(total, 7)


if __name__ == "__main__":
    unittest.main()
