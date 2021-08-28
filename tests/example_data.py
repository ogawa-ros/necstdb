import pathlib
import json
import time
import sys

import necstdb

db_path = pathlib.Path(".") / "tests" / "example_data"

if db_path.exists():
    sys.exit(1)

db = necstdb.opendb(db_path, mode="w")

data_info = {
    "data": [
        {"key": "test_float64", "format": "d", "size": 8},
        {"key": "test_bool", "format": "i", "size": 4},
        {"key": "test_int64", "format": "q", "size": 8},
        {"key": "test_string", "format": "6s", "size": 6},
    ],
    "memo": "generated by example_data",
    "necstdb_version": necstdb.__version__,
}


def data(length):
    for i in range(length):
        yield [time.time(), True, i, b"string"]


db.create_table("data4", data_info.copy(), endian="")
table = db.open_table("data4", mode="ab")

for dat in data(11):
    table.append(*dat)

table.close()

with (db_path / "data4.header").open("w") as f:
    json.dump(data_info, f)