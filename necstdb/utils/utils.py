from typing import List
import struct
import re


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
