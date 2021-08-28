from typing import List
import struct
import re

import numpy


def get_struct_indices(fmt: str) -> List[int]:
    """Calculate first indices of fields in byte string.

    Notes
    -----
    Padding at the end is not supported.
    """
    indices = [struct.calcsize(fmt)]
    fmt = re.findall(r"[!=@<>]|[0-9]*[a-zA-Z?]", fmt)
    for i in range(len(fmt)):
        if i == 0:
            tmp_fmt = fmt
        else:
            tmp_fmt = fmt[:-i]
        if not tmp_fmt[-1] in "!=@<>":
            idx = struct.calcsize("".join(tmp_fmt)) - struct.calcsize(tmp_fmt[-1])
            indices.insert(0, idx)
    return indices


def get_struct_sizes(fmt: str) -> List[int]:
    indices = numpy.array(get_struct_indices(fmt))
    return (indices[1:] - indices[:-1]).tolist()
