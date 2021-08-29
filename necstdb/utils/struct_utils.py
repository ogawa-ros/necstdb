from typing import List
import struct

import numpy


def get_struct_indices(fmt: List[str], endian: str = "") -> List[int]:
    """Calculate first indices of fields in byte string.

    Notes
    -----
    Padding at the end is not supported.
    """
    format_str = endian + "".join(fmt)
    indices = [struct.calcsize(format_str)]
    for i in range(len(fmt)):
        if i == 0:
            tmp_fmt = fmt
        else:
            tmp_fmt = fmt[:-i]
        idx = struct.calcsize(endian + "".join(tmp_fmt)) - struct.calcsize(tmp_fmt[-1])
        indices.insert(0, idx)
    return indices


def get_struct_sizes(fmt: List[str], endian: str = "") -> List[int]:
    indices = numpy.array(get_struct_indices(fmt, endian))
    return (indices[1:] - indices[:-1]).tolist()
