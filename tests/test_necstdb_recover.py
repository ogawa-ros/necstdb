import pathlib

import pytest

import necstdb

import xarray


@pytest.fixture
def db_path(tmp_path_factory) -> pathlib.Path:
    """Path to temporary database directory."""
    return tmp_path_factory.mktemp("test_db")


class TestReadDatabase:

    EXAMPLE_DATA_PATH = pathlib.Path(".") / "tests" / "example_data"

    def test_read_db_with_invalid_format_specifier(self):
        db = necstdb.opendb(self.EXAMPLE_DATA_PATH)
        _ = db.open_table("data4").read(astype="raw")
        with pytest.raises(ValueError):
            _ = db.open_table("data4").read(astype="tuple")
        with pytest.raises(ValueError):
            _ = db.open_table("data4").read(astype="dict")
        with pytest.raises(ValueError):
            _ = db.open_table("data4").read(astype="df")
        with pytest.raises(ValueError):
            _ = db.open_table("data4").read(astype="array")
        actual = db.open_table("data4").recovered.read(astype="raw")
        print(actual)
        actual = db.open_table("data4").recovered.read(astype="tuple")
        print(actual)
        actual = db.open_table("data4").recovered.read(astype="dict")
        print(actual)
        actual = db.open_table("data4").recovered.read(astype="df")
        print(actual)
        actual = db.open_table("data4").recovered.read(astype="array")
        print(actual)

    def test_ignore_trailing_pad_bytes(self, db_path):
        header = {
            "data": [
                {"key": "data", "format": "5s", "size": 5},
                {"key": "bool", "format": "?", "size": 1},
            ]
        }

        db = necstdb.opendb(db_path, mode="w")
        db.create_table("string_length_missepecified", header)
        table = db.open_table("string_length_missepecified", mode="ab")

        data = b"abc"
        _ = table.append(data, True)
        table.close()  # Close table to flush the data

        table = db.open_table("string_length_missepecified").recovered
        assert table.read(astype="raw")[:5] == data + b"\x00\x00"  # Won't be recovered
        assert table.read(astype="tuple")[0][0] == data
        assert table.read(astype="dict")[0]["data"] == data
        assert table.read(astype="df")["data"].values[0] == data
        assert table.read(astype="sa")["data"][0] == data
