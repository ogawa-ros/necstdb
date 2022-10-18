from __future__ import annotations

from typing import TYPE_CHECKING

import numpy

if TYPE_CHECKING:
    from .necstdb import table


def recover(t: table) -> table:
    fmt = "".join([d["format"] for d in t.header["data"]])

    if (t.endian == "<") and ("i" in fmt):
        t.endian = ""
        t.open(t._name, t._mode)
        for dat in t.header["data"]:
            dat["format"] = dat["format"].replace("i", "?")

    if "s" in fmt:
        modified_header_data = []
        for dat in t.header["data"]:
            if "s" not in dat["format"]:
                modified_header_data.append(dat)
            else:
                dat_numpy = t.read(astype="sa")
                this_field = dat_numpy[dat["key"]]
                lengths = numpy.unique([len(d) for d in this_field])
                if len(lengths) > 1:  # Not uniform length
                    modified_header_data.append(dat)
                else:
                    (length,) = lengths
                    specified = int(dat["format"].rstrip("s"))
                    if length == specified:
                        modified_header_data.append(dat)
                    else:
                        diff = specified - length
                        modified_header_data.append(
                            {
                                "key": dat["key"],
                                "format": f"{length}s",
                                "size": length,
                            }
                        )
                        modified_header_data.append(
                            {
                                "key": f"_{dat['key']}_pad",
                                "format": f"{diff}x",
                                "size": diff,
                            }
                        )
        t.header["data"] = modified_header_data

    return t
