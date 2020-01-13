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
        with jsonl as f:
            pass 
        self.assertTrue(jsonl.closed())

    def test_write_and_read(self):
        from bobtools.io import JSONL
        filename = "test_tmpfile"
        if os.path.exists(filename):
            os.remove(filename)

        data = ["string", 1, {"a":"b"}, [1,2], 1.2]

        jsonl_writer = JSONL(filename)
        jsonl_writer.extend(data)
        jsonl_writer.close()
        jsonl_reader = JSONL(filename)
        read_data = jsonl_reader.read()

        self.assertEqual(data, read_data)

        os.remove(filename)


if __name__ == "__main__":
    unittest.main()