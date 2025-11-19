import unittest
from unittest.mock import MagicMock, patch

import neo4j_importer as nji


class TestNeo4jImporter(unittest.TestCase):
    @patch("neo4j_importer.SpreadsheetParser")
    def test_import_to_neo4j_calls_driver(self, mock_parser_cls):
        mock_parser = MagicMock()
        mock_parser.parse_parts.return_value = {
            "111": {"name": "Frame"},
            "222": {"name": "Ski"},
        }
        mock_parser.build_cross_index.return_value = ({"111": "Frame", "222": "Ski"}, {})
        mock_parser.parse_bom_csv.return_value = [("111", "222")]
        mock_parser_cls.return_value = mock_parser

        fake_session = MagicMock()
        fake_driver = MagicMock()
        fake_driver.session.return_value.__enter__.return_value = fake_session

        with patch("neo4j_importer.Neo4jClient.__init__", return_value=None):
            with patch.object(nji.Neo4jClient, "driver", fake_driver):
                client = nji.Neo4jClient.__new__(nji.Neo4jClient)
                client.driver = fake_driver
                with patch.object(nji, "Neo4jClient", return_value=client):
                    rc = nji.main([
                        "--excel", "Snowmobile.xlsx",
                        "--bom", "bom.csv",
                        "--uri", "bolt://localhost:7687"
                    ])
                    self.assertEqual(rc, 0)
                    self.assertTrue(fake_session.run.called)


if __name__ == "__main__":
    unittest.main()