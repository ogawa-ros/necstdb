import pathlib

import pytest

import necstdb


EXAMPLE_DATA_PATH = pathlib.Path(".") / "tests" / "example_data"


class TestReadDatabase:
    def test_read_db(self):
        db = necstdb.opendb(EXAMPLE_DATA_PATH)
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
