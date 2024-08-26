import io
import unittest
import contextlib

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse


@contextlib.contextmanager
def avoid_closing(fd):
    close = fd.close
    fd.close = lambda: None
    yield fd
    fd.close = close


class TestAvroEncoder(unittest.TestCase):
    def test_encode_simple_string(self):
        """Test encoding a simple string schema with a valid string value."""
        schema = parse('{"type": "string"}')
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        with avoid_closing(bytes_writer):
            with DataFileWriter(bytes_writer, writer, schema) as dfw:
                dfw.append("Hello, Avro!")
            bytes_writer.seek(0)
            reader = DatumReader(schema)
            with DataFileReader(bytes_writer, reader) as dfr:
                for record in dfr:
                    self.assertEqual(record, "Hello, Avro!")

    def test_encode_simple_int(self):
        """Test encoding a simple int schema with a valid int value."""
        schema = parse('{"type": "int"}')
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        with avoid_closing(bytes_writer):
            with DataFileWriter(bytes_writer, writer, schema) as dfw:
                dfw.append(42)
            bytes_writer.seek(0)
            reader = DatumReader(schema)
            with DataFileReader(bytes_writer, reader) as dfr:
                for record in dfr:
                    self.assertEqual(record, 42)

    def test_encode_with_null(self):
        """Test encoding a schema with a null type."""
        schema = parse('["null", "string"]')
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        with avoid_closing(bytes_writer):
            with DataFileWriter(bytes_writer, writer, schema) as dfw:
                dfw.append(None)
            bytes_writer.seek(0)
            reader = DatumReader(schema)
            with DataFileReader(bytes_writer, reader) as dfr:
                for record in dfr:
                    self.assertIsNone(record)
