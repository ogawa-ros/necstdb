from necstdb import utils
import xarray

TEST_FORMATS = ["d", "?", "3f", "3s", "b"]

EXPECTED_INDICES = [0, 8, 9, 21, 24, 25]
EXPECTED_SIZES = [8, 1, 12, 3, 1]


def test_get_struct_indices():
    actual = utils.get_struct_indices(TEST_FORMATS, "<")
    assert actual == EXPECTED_INDICES


def test_get_struct_sizes():
    actual = utils.get_struct_sizes(TEST_FORMATS, "<")
    assert actual == EXPECTED_SIZES


TEST_DATA = [True, (2.0, 3.0), [4, 5], "six"]
EXPECTED_FLATTENED = [True, 2.0, 3.0, 4, 5, "six"]


def test_flatten_data():
    actual = utils.flatten_data(TEST_DATA)
    assert actual == EXPECTED_FLATTENED
