import unittest

from bobtools.datascan import DictScanner


class dictScannerTests(unittest.TestCase):
    def test_basic_schema(self):
        data = [{"a": i} for i in range(10)]
        ds = DictScanner()
        ds.scan_all(data)
        self.assertEqual(ds.n_scanned, len(data))
        self.assertEqual(ds.to_schema(), {"a": type(1)})

    def test_nested_schema(self):
        data = [{"a": {"d": 1, "e": 2}, "b": "3", "c": "4"}]
        ds = DictScanner()
        ds.scan_all(data)
        expected = {"a": {"d": type(1), "e": type(2)}, "b": type("3"), "c": type("4")}
        self.assertEqual(ds.to_schema(), expected)

    def test_nested_schema_with_int_keys(self):
        data = [{"a": {"d": 1, "e": 2}, 2: "3", "c": "4"}]
        ds = DictScanner()
        ds.scan_all(data)
        expected = {"a": {"d": type(1), "e": type(2)}, 2: type("3"), "c": type("4")}
        self.assertEqual(ds.to_schema(), expected)

    def test_nested_prototype_with_int_keys(self):
        data = [{"a": {"d": 1, "e": 2}, 2: "3", "c": "4"}]
        ds = DictScanner()
        ds.scan_all(data)
        expected = {"a": {"d": 1, "e": 2}, 2: "3", "c": "4"}
        self.assertEqual(ds.to_prototype(), expected)

    def test_nested_list(self):
        data = [{"a": [{"b": 1}, {"c": "here"}]}]
        ds = DictScanner()
        ds.scan_all(data)

        expected_prototype = {"a": [{"b": 1, "c": "here"}]}
        expected_schema = {"a": [{"b": type(1), "c": type("here")}]}

        self.assertEqual(ds.to_prototype(), expected_prototype)
        self.assertEqual(ds.to_schema(), expected_schema)

    def test_double_nested_list(self):
        data = [{"a": [{"b": [1, 2]}, {"c": "here"}]}]
        ds = DictScanner()
        ds.scan_all(data)

        expected_prototype = {"a": [{"b": [2], "c": "here"}]}
        expected_schema = {"a": [{"b": [type(1)], "c": type("here")}]}

        self.assertEqual(ds.to_prototype(), expected_prototype)
        self.assertEqual(ds.to_schema(), expected_schema)

    def test_optional_branch(self):
        data = [{"a": {"b": 2}}, {"a": None}]
        ds = DictScanner()
        ds.scan_all(data)

        expected_prototype = {"a": {"b": 2}}
        expected_schema = {"a": {"b": type(2)}}

        self.assertEqual(ds.to_prototype(), expected_prototype)
        self.assertEqual(ds.to_schema(), expected_schema)

    def test_initialization_with_data(self):
        data = [{"a": {"d": 1, "e": 2}, "b": "3", "c": "4"}]
        ds = DictScanner(data)
        expected = {"a": {"d": type(1), "e": type(2)}, "b": type("3"), "c": type("4")}
        self.assertEqual(ds.to_schema(), expected)


if __name__ == "__main__":
    unittest.main()
