import json
import logging
import os
import gzip
from typing import Any, Generator, Iterable, Union

OPENERS = {
    "gzip": {"object": gzip.GzipFile, "read_mode": "rb+", "append_mode": "ab+"},
    "default": {"object": open, "read_mode": "r+", "append_mode": "a+"},
}


class JSONL:

    written = 0
    n_read = 0
    extension = ""
    new = False
    zipped = False
    _fileobj = None
    print_on_exit = False
    skip_unserializable_objects = False

    line_end = "\n"

    def __init__(
        self, filename, skip_unserializable_objects=False, print_on_exit=False
    ):
        # absorbed attributes
        self.print_on_exit = print_on_exit
        self.filename = filename
        # inferred attributes
        self.extension = filename.rsplit(".", 1) if "." in filename else "JSONL"
        self.opener = OPENERS.get(self.extension, OPENERS.get("default"))
        self.new = not self.exists()
        self.open()

    def open(self, for_read: bool = False):
        """(Re)-opens file """
        mode = (
            self.opener.get("read_mode") if for_read else self.opener.get("append_mode")
        )
        self._fileobj = self.opener["object"](self.filename, mode=mode)

    def __repr__(self):
        is_open = not self.closed()
        report = f"--- JSONL FILE : {self.filename} ---\n"
        f"Newly created : {self.new}"
        f"extension     : {self.extension}"
        f"zipped        : {self.zipped}"
        f"currently_open: {is_open}"
        f"lines written : {self.written}"
        f"lines read    : {self.n_read}"

        return report

    def exists(self):
        return os.path.exists(self.filename)

    def closed(self):
        if not self._fileobj:
            return False
        return self._fileobj.closed

    def __enter__(self):
        return self

    def __exit__(self, type=None, value=None, traceback=None):
        self._fileobj.close()
        if self.print_on_exit:
            print(self)

    def close(self):
        self._fileobj.close()

    @staticmethod
    def serialize_to_line(object: Union[dict, str, int, float, list, tuple]) -> str:
        """Serialize an object to a JSON string, ignoring errors if so configured"""
        try:
            return json.dumps(object)
        except Exception as e:
            if self.skip_unserializable_objects:
                return
            raise e

    @staticmethod
    def deserialize_line(
        line: Union[str, bytes]
    ) -> Union[dict, str, int, float, list, tuple]:
        """Deserialize to a object from a JSON (byte-)string, ignoring errors if so configured"""
        try:
            if type(line) == str:
                return json.loads(line)
            elif type(line) == bytes:
                return json.loads(line.decode())
        except Exception as e:
            if self.skip_unserializable_objects:
                return
            raise e

    def _assure_apendmode(self):
        """If file is not opened for appending, open file for appending."""
        if self.closed() or self._fileobj.mode != self.opener["append_mode"]:
            self.open(for_read=False)

    def _assure_readmode(self):
        """If file is not open for reading, open file for reading"""
        if self.closed() or self._fileobj.mode != self.opener["read_mode"]:
            self.open(for_read=True)

    def append(self, object: Union[dict, str, int, float, list, tuple]) -> None:
        """Add an object to the file"""
        self._assure_apendmode()
        line = self.serialize_to_line(object) + self.line_end
        if not line:
            return
        self.written += 1
        self._fileobj.write(line)

    def extend(
        self, object_iterable: Iterable[Union[dict, str, int, float, list, tuple]]
    ) -> None:
        """Appends each object in an iterable to the file (wraps JSONL.append)"""
        self._assure_apendmode()
        for obj in object_iterable:
            self.append(obj)

    def read_line(self) -> Union[dict, str, int, float, list, tuple]:
        if self.closed():
            logger.critical(
                "Unable to read, file was closed (possibly because end was reached)"
            )
            return
        self._assure_readmode()
        line = self._fileobj.read_line()
        self.n_read += 1
        return self.deserialize_line(line)

    def read(self) -> list:
        """Read all lines in a file and returns the contained objects as a list.
        
        Note:
            - re-opens file if file is closed
            - closes file after reading (as the end of the buffer is reached)
        """
        self._assure_readmode()
        contents = []
        for line in self._fileobj:
            obj = self.deserialize_line(line)
            contents.append(obj)
            self.n_read += 1
            if len(contents) == 10000:
                logging.warning(
                    "Read 10000 lines into memory, consider using JSONL.stream "
                    "to avoid running out of memory."
                )
        self.close()
        return contents

    def stream(
        self,
    ) -> Generator[Union[dict, str, int, float, list, tuple], None, None]:
        """Yield objects from file one-by-one
        
        Note:
            - re-opens file if file is closed
            - closes file after reading (as the end of the buffer is reached)
        """
        self._assure_readmode()
        for line in self._fileobj:
            obj = self.deserialize_line(line)
            self.n_read += 1
            yield obj
        self.close()
