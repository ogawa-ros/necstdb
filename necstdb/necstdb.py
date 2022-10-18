"""NECSTDB, a database for NECST.
NECST, an abbreviation of *NEw Control System for Telescope*, is a flexible controlling
system for radio telescopes. Its efficient data storage format is provided here.
The database contains tables, which keep individual topic of data with some metadata
attached to them, e.g. spectral data from one spectrometer board (array of data +
timestamp), various kinds of weather data (temperature + humidity + wind speed + wind
direction + ... + timestamp), etc.
"""

import json
import mmap
import os
import pathlib
import re
import struct
import tarfile
from typing import Any, Dict, List, Tuple, Union

import numpy
import pandas

from . import utils
from .recover import recover


def duplicate_rename(path: pathlib.Path, _i: int = 0) -> pathlib.Path:
    """Return nonexistent path for new file.

    Parameters
    ----------
    path
        File path you want to create as *new* file.

    """
    _path = path if _i == 0 else path.parent / f"{path.stem}({_i}){path.suffix}"
    if not _path.exists():
        return _path
    return duplicate_rename(path, _i + 1)


class necstdb:
    """Database for NECST.

    Parameters
    ----------
    path: PathLike
        Path to the database directory, the direct parent of *.data and *.header files.
    mode: str
        Mode in which the database is opened, either "r" or "w".

    """

    def __init__(self, path: os.PathLike, mode: str) -> None:
        self.opendb(path, mode)

    def opendb(self, path: os.PathLike, mode: str) -> None:
        """Catch the database directory."""
        self.path = pathlib.Path(path)

        if not self.path.exists():
            if mode.find("w") != -1:
                self.path.mkdir(parents=True)
            elif mode.find("r") != -1:
                raise Exception("This directory doesn't exist!!")

    def list_tables(self) -> List[str]:
        """List all tables within the database."""
        data = [path.stem for path in self.path.glob("*.data")]
        header = [path.stem for path in self.path.glob("*.header")]
        return sorted(set(data) & set(header))

    def create_table(
        self, name: str, config: Dict[str, Any], endian: str = "<"
    ) -> None:
        """Create a pair of data and header files, then write header content."""
        if name in self.list_tables():
            return
        self.endian = endian

        format_list = [dat["format"] for dat in config["data"]]

        # Validate sizes
        struct_sizes = utils.get_struct_sizes(format_list, endian)
        for dat, size in zip(config["data"], struct_sizes):
            dat["size"] = size
        config["struct_indices"] = True

        data_path = self.path / (name + ".data")
        header_path = self.path / (name + ".header")

        data_path.touch()
        with header_path.open("w") as f:
            json.dump(config, f)

    def open_table(self, name: str, mode: str = "rb") -> "table":
        """Topic-wise data table."""
        if hasattr(self, "endian"):
            return table(self.path, name, mode, self.endian)
        return table(self.path, name, mode)

    def save_file(self, name: str, data: Union[str, bytes], info: str = "") -> None:
        """Save a file in database.

        Parameter
        ---------
        name
            File name.
        data
            Contents to be written in the file.
        info
            Optional metadata for the file, will be added in first line of the file.

        Examples
        --------
        >>> db.savefile(
        ...     "example.txt",
        ...     "Some contents here.",
        ...     "Original path: /path/to/example.txt",
        ... )

        """
        filepath = duplicate_rename(self.path / name)
        splitter = "\n=== Written by NECSTDB ===\n"
        if isinstance(data, bytes):
            data = bytes(info + splitter, encoding="utf-8") + data
            _ = filepath.write_bytes(data)
        else:
            data = info + splitter + data
            _ = filepath.write_text(str(data))

    def read_file(
        self, name: str, asbytes: bool = False
    ) -> Tuple[Union[str, bytes], Union[str, bytes]]:
        """Read a file saved in the database.

        Parameters
        ----------
        name
            File name, extension inclusive.
        asbytes
            If True, the data will be returned as bytes.

        Examples
        --------
        >>> db.read_file("example.txt")
        "Some contents here.", "Original path: /path/to/example.txt"

        """
        if asbytes:
            info, data = (
                (self.path / name)
                .read_bytes()
                .split(b"\n=== Written by NECSTDB ===\n", 1)
            )
        else:
            info, data = (
                (self.path / name)
                .read_text()
                .split("\n=== Written by NECSTDB ===\n", 1)
            )
        return data, info

    def checkout(self, saveto: os.PathLike, compression: str = None) -> None:
        """Archive the database.

        Parameters
        ----------
        saveto: PathLike
            Path to the tar file to be created.
        compression: str
            Compression format/program to be used. One of ["gz", "bz2", "xz"].

        """
        mode = "w:"
        if compression is not None:
            mode += compression

        tar = tarfile.open(saveto, mode=mode)
        tar.add(self.path)
        tar.close()

    def get_info(self) -> pandas.DataFrame:
        """Get metadata of all tables in the database."""
        names = self.list_tables()

        dictlist = []
        for name in names:
            table = self.open_table(name)
            dic = {
                "table name": name,
                "file size [byte]": table.stat.st_size,
                "#records": table.nrecords,
                "record size [byte]": table.record_size,
                "format": table.format,
            }
            dictlist.append(dic)
            table.close()

        df = pandas.DataFrame(
            dictlist,
            columns=[
                "table name",
                "file size [byte]",
                "#records",
                "record size [byte]",
                "format",
            ],
        ).set_index("table name")

        return df


class table:
    """Data table for single topic.

    Parameters
    ----------
    dbpath: pathlib.Path
        Path to database directory, the direct parent of *.data and *.header files.
    name: str
        Name of table.
    mode: str
        Mode in which the database is opened (e.g. ["rb", "wb", ...]).
    endian: str
        One of ["=", "<", ">"].

    Notes
    -----
    Endian specifications ["@", "!"] are not supported, since numpy doesn't recognize
    them. Though ["="] is supported, the use of it is deprecated since the behavior may
    vary between architectures this program runs on.

    """

    dbpath = ""
    data_file = None
    header = {}
    record_size = 0
    format = ""
    stat = None
    nrecords = 0
    endian = ""

    def __init__(
        self, dbpath: pathlib.Path, name: str, mode: str, endian: str = "<"
    ) -> None:
        self.dbpath = dbpath
        self.endian = endian
        self.open(name, mode)

        self._name = name
        self._mode = mode

    def open(self, table_name: str, mode: str) -> None:
        """Open a data table of specified topic."""
        data_path = self.dbpath / (table_name + ".data")
        header_path = self.dbpath / (table_name + ".header")

        if not (data_path.exists() and header_path.exists()):
            raise Exception(f"Table '{table_name}' does not exist.")

        self.data_file = data_path.open(mode)
        with header_path.open("r") as header_file:
            self.header = json.load(header_file)

        format_list = [dat["format"] for dat in self.header["data"]]
        self.format = self.endian + "".join(format_list)
        self.record_size = struct.calcsize(self.format)
        self.stat = data_path.stat()
        self.nrecords = self.stat.st_size // self.record_size

        if self.header.get("struct_indices", False) is False:
            # Infer sizes
            struct_sizes = utils.get_struct_sizes(format_list, self.endian)
            for dat, size in zip(self.header["data"], struct_sizes):
                dat["size"] = size
            self.header["struct_indices"] = True

    def close(self) -> None:
        """Close the data file of the table."""
        self.data_file.close()

    def append(self, *data: Any) -> None:
        """Append data to the table."""
        self.data_file.write(struct.pack(self.format, *data))

    def read(
        self, num: int = -1, start: int = 0, cols: List[str] = [], astype: str = "tuple"
    ) -> Union[tuple, dict, numpy.ndarray, pandas.DataFrame, bytes]:
        """Read the contents of the table.

        Parameters
        ----------
        num: int
            Number of records to be read.
        start: int
            Index of first record to be read.
        cols: list of str
            Names of the fields to be picked up (e.g. "timestamp").
        astype: str
            One of ["tuple", "dict", "structuredarray", "dataframe", "buffer"] or their
            aliases, ["structured_array", "array", "sa", "data_frame", "pandas", "df",
            "raw"].

        """
        mm = mmap.mmap(self.data_file.fileno(), 0, prot=mmap.PROT_READ)
        mm.seek(start * self.record_size)

        if cols == []:
            data = self._read_all_cols(mm, num)
        else:
            if isinstance(cols, str):
                raise ValueError("Column names should be given as list of str.")
            data = self._read_specified_cols(mm, num, cols)

        mm.close()
        return self._astype(data, cols, astype)

    def _read_all_cols(self, mm: mmap.mmap, num: int) -> bytes:
        """Read all columns of the data table."""
        if num == -1:
            size = num
        else:
            size = num * self.record_size
        return mm.read(size)

    def _read_specified_cols(
        self, mm: mmap.mmap, num: int, cols: List[Dict[str, str]]
    ) -> bytes:
        """Read specified columns of the data table.

        Notes
        -----
        The byte count of this function may contain bugs. For data which are not
        aligned, the count would be correct, but byte count for aligned data is much
        difficult, hence the implementation may not be perfect.
        One resolution for this problem would be to read all columns, then drop
        unnecessary columns.

        """
        commands = []
        for _col in self.header["data"]:
            elemsize = struct.calcsize(_col["format"])
            if _col["key"] in cols:
                commands.append({"cmd": "read", "size": elemsize})
                commands.append({"cmd": "seek", "size": _col["size"] - elemsize})
            else:
                commands.append({"cmd": "seek", "size": _col["size"]})

        if num == -1:
            num = (mm.size() - mm.tell()) // self.record_size

        draw = b""
        for _ in range(num):
            for _cmd in commands:
                if _cmd["cmd"] == "seek":
                    mm.seek(_cmd["size"], os.SEEK_CUR)
                else:
                    draw += mm.read(_cmd["size"])
        return draw

    def _astype(
        self, data: bytes, cols: List[Dict[str, Any]], astype: str
    ) -> Union[tuple, dict, numpy.ndarray, pandas.DataFrame, bytes]:
        """Map the astype argument to corresponding methods."""
        if cols == []:
            cols = self.header["data"]
        else:
            cols = [_col for _col in self.header["data"] if _col["key"] in cols]

        def DataFormatError(e: Union[Exception, str] = ""):
            return ValueError(
                str(e) + "\nThis may caused by wrong specification of data format."
                "Try ``db.open_table(table_name).recovered.read()`` instead of"
                "``db.open_table(table_name).read()``."
            )

        if astype in ["tuple"]:
            try:
                return self._astype_tuple(data, cols)
            except struct.error as e:
                raise DataFormatError(e)

        elif astype in ["dict"]:
            try:
                return self._astype_dict(data, cols)
            except struct.error as e:
                raise DataFormatError(e)

        elif astype in ["structuredarray", "structured_array", "array", "sa"]:
            try:
                return self._astype_structured_array(data, cols)
            except ValueError as e:
                raise DataFormatError(e)

        elif astype in ["dataframe", "data_frame", "pandas", "df"]:
            try:
                return self._astype_data_frame(data, cols)
            except struct.error as e:
                raise DataFormatError(e)

        elif astype in ["buffer", "raw"]:
            return data

        else:
            raise ValueError(f"Unknown return type {astype}.")

    def _astype_tuple(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> Tuple[Tuple[Any]]:
        """Read the data as tuple of tuple."""
        fmt = self.endian + "".join([col["format"] for col in cols])
        return tuple(struct.iter_unpack(fmt, data))

    def _astype_dict(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Read the data as list of dict."""
        offset = 0
        dictlist = []
        while offset < len(data):
            dict_ = {}

            for col in cols:
                size = struct.calcsize(col["format"])
                if "x" in col["format"]:  # Pad field
                    offset += col["size"]
                    continue
                dat = struct.unpack(col["format"], data[offset : offset + size])
                if len(dat) == 1:
                    dat = dat[0]

                dict_[col["key"]] = dat
                offset += col["size"]

            dictlist.append(dict_)

        return dictlist

    def _astype_data_frame(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> pandas.DataFrame:
        """Read the data as pandas.DataFrame."""
        data = self._astype_dict(data, cols)
        return pandas.DataFrame.from_dict(data)

    def _astype_structured_array(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> numpy.ndarray:
        """Read the data as numpy's structured array."""
        formats = [col["format"] for col in cols]

        def parse_dtype(format_character: str) -> str:
            format_character = re.sub(
                r"^([\d+s]+)$",
                lambda m: f"{m.group(1).count('s')}S{m.group(1).split('s')[0]}",
                format_character,
            )

            format_character = format_character.replace("x", "V")
            return self.endian + format_character

        np_formats = [parse_dtype(col["format"]) for col in cols]
        keys = [col["key"] for col in cols]
        offsets = utils.get_struct_indices(formats, self.endian)[:-1]

        pad = ["x" in col["format"] for col in cols]
        data_field = [k for k, p in zip(keys, pad) if not p]

        dtype = numpy.dtype({"names": keys, "formats": np_formats, "offsets": offsets})
        return numpy.frombuffer(data, dtype=dtype)[data_field]

    @property
    def recovered(self) -> "table":
        """Restore the broken data caused by bugs in logger.

        Examples
        --------
        >>> db = necstdb.opendb("path/to/db")
        >>> data = db.open_table("topic_name").recovered.read(astype="array")
        array([...])

        Notes
        -----
        The details of the bugs are:
        - bool data are dumped as bool, but the formatting character in the header was
          int32's
        When other bug is found, this property should determine what the problem is,
        based on the value of the data (e.g. timestamp contains extremely small number
        such as 1e-308)

        """
        return recover(self)


def opendb(path: os.PathLike, mode: str = "r") -> "necstdb":
    """Quick alias to open a database.

    Parameters
    ----------
    path: PathLike
        Path to the database directory, the direct parent of *.data and *.header files.
    mode: str
        Mode in which the database is opened (e.g. ["rb", "wb", ...]).

    """
    return necstdb(path, mode)
