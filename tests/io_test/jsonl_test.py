import os
import unittest
import unittest.mock


class JSONLTest(unittest.TestCase):
    def test_instantiation(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile"
        if os.path.exists(filename):
            os.remove(filename)
        jsonl = JSONL(filename)
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(jsonl.new, True)
        os.remove(filename)

    def test_closing(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile"
        if os.path.exists(filename):
            os.remove(filename)
        jsonl = JSONL(filename)
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(jsonl.closed(), False)
        jsonl.close()
        self.assertEqual(jsonl.closed(), True)
        os.remove(filename)

    def test_print_on_exit(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile"
        if os.path.exists(filename):
            os.remove(filename)
        JSONL.__repr__ = unittest.mock.Mock(return_value="repr_test")
        jsonl = JSONL(filename, print_on_exit=True)
        with jsonl:
            pass
        self.assertTrue(jsonl.closed())

    def test_write_and_read(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile"
        if os.path.exists(filename):
            os.remove(filename)

        data = ["string", 1, {"a": "b"}, [1, 2], 1.2]

        jsonl_writer = JSONL(filename)
        jsonl_writer.extend(data)
        jsonl_writer.close()
        jsonl_reader = JSONL(filename)
        read_data = jsonl_reader.read()

        self.assertEqual(data, read_data)

        os.remove(filename)

    def test_write_and_read_gzip(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile.gzip"
        if os.path.exists(filename):
            os.remove(filename)

        data = ["string", 1, {"a": "b"}, [1, 2], 1.2]

        jsonl_writer = JSONL(filename)
        jsonl_writer.extend(data)
        jsonl_writer.close()
        jsonl_reader = JSONL(filename)
        read_data = jsonl_reader.read()

        self.assertEqual(data, read_data)

        os.remove(filename)

    def test_write_and_read_gzip_double_extension(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile.jsonl.gzip"
        if os.path.exists(filename):
            os.remove(filename)

        data = ["string", 1, {"a": "b"}, [1, 2], 1.2]

        jsonl_writer = JSONL(filename)
        jsonl_writer.extend(data)
        jsonl_writer.close()
        jsonl_reader = JSONL(filename)
        read_data = jsonl_reader.read()

        self.assertEqual(data, read_data)

        os.remove(filename)

    def test_append_and_readline(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile.jsonl"
        if os.path.exists(filename):
            os.remove(filename)

        data = {"hey": "whay about this", "1": 1, "float": 0.1}

        jsonl_writer = JSONL(filename)
        jsonl_writer.append(data)

        self.assertEqual(jsonl_writer.readline(), data)
        os.remove(filename)

    def test_append_and_readline_gzip(self):
        from bobtools.io import JSONL

        filename = "test_tmpfile.jsonl.gzip"
        if os.path.exists(filename):
            os.remove(filename)

        data = {"hey": "whay about this", "1": 1, "float": 0.1}

        jsonl_writer = JSONL(filename)
        jsonl_writer.append(data)

        self.assertEqual(jsonl_writer.readline(), data)
        os.remove(filename)


if __name__ == "__main__":
    unittest.main()
