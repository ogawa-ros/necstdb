"""NECSTDB, a database for NECST.
NECST, an abbreviation of *NEw Control System for Telescope*, is a flexible controlling
system for radio telescopes. Its efficient data storage format is provided here.
The database contains tables, which keep individual topic of data with some metadata
attached to them, e.g. spectral data from one spectrometer board (array of data +
timestamp), various kinds of weather data (temperature + humidity + wind speed + wind
direction + ... + timestamp), etc.
"""


from typing import Union, List, Tuple, Dict, Any
import os
import mmap
import struct
import pathlib
import json
import tarfile

import numpy
import pandas


class necstdb(object):
    """Database for NECST.

    Parameters
    ----------
    path: PathLike
        Path to the database directory, the direct parent of *.data and *.header files.
    mode: str
        Mode in which the database is opened (e.g. ["rb", "wb", ...]).
    """

    def __init__(self, path: os.PathLike, mode: str) -> None:
        self.opendb(path, mode)
        pass

    def opendb(self, path: os.PathLike, mode: str) -> None:
        """Catch the database directory."""
        if not isinstance(path, pathlib.Path):
            path = pathlib.Path(path)
            pass

        if not path.exists():
            if mode == "w":
                path.mkdir(parents=True)

            elif mode == "r":
                raise Exception("This directory doesn't exist!!")
        return

    def list_tables(self) -> List[str]:
        """List all tables within the database."""
        return sorted([table.stem for table in self.path.glob("*.data")])

    def create_table(self, name: str, config: Dict[str, Any]) -> None:
        """Create a pair of data and header files, then write header content."""
        if name in self.list_tables():
            return

        data_path = self.path / (name + ".data")
        header_path = self.path / (name + ".header")

        data_path.touch()
        with header_path.open("w") as f:
            json.dump(config, f)
            pass
        return

    def open_table(self, name: str, mode: str = "rb", endian: str = "<") -> "table":
        """Topic-wise data table."""
        return table(self.path, name, mode, endian)

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
            pass
        tar = tarfile.open(saveto, mode=mode)
        tar.add(self.path)
        tar.close()
        return

    def get_info(self) -> pandas.DataFrame:
        """Metadata of all tables in the database."""
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
            continue

        df = pandas.DataFrame(
            dictlist,
            columns=["table name", "file size", "#records", "record size", "format"],
        ).set_index("table name")

        return df


class table(object):
    """Data table which records single topic.

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
        pass

    def open(self, table_name: str, mode: str) -> None:
        """Open a data table of specified topic."""
        data_path = self.dbpath / (table_name + ".data")
        header_path = self.dbpath / (table_name + ".header")

        if not (data_path.exists() and header_path.exists()):
            raise Exception("table '{table_name}' does not exist".format(**locals()))

        self.data_file = data_path.open(mode)
        with header_path.open("r") as header_file:
            self.header = json.load(header_file)
            pass

        format_list = [dat["format"] for dat in self.header["data"]]
        self.format = self.endian + "".join(format_list)
        self.record_size = struct.calcsize(self.format)
        self.stat = data_path.stat()
        self.nrecords = self.stat.st_size // self.record_size
        return

    def close(self) -> None:
        """Close the data file of the table."""
        self.data_file.close()
        return

    def append(self, *data: Any) -> None:
        """Append data to the table."""
        self.data_file.write(struct.pack(self.format, *data))
        return

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
            pass

        return self._astype(data, cols, astype)

    def _read_all_cols(self, mm: mmap.mmap, num: int) -> bytes:
        """Read all columns of the data table."""
        if num == -1:
            size = num
        else:
            size = num * self.record_size
            pass
        return mm.read(size)

    def _read_specified_cols(
        self, mm: mmap.mmap, num: int, cols: List[Dict[str, str]]
    ) -> bytes:
        """Read specified columns of the data table."""
        commands = []
        for _col in self.header["data"]:
            if _col["key"] in cols:
                commands.append({"cmd": "read", "size": _col["size"]})
            else:
                commands.append({"cmd": "seek", "size": _col["size"]})
                pass
            continue

        if num == -1:
            num = (mm.size() - mm.tell()) // self.record_size

        draw = b""
        for i in range(num):
            for _cmd in commands:
                if _cmd["cmd"] == "seek":
                    mm.seek(_cmd["size"], os.SEEK_CUR)
                else:
                    draw += mm.read(_cmd["size"])
                    pass
                continue
            continue
        return draw

    def _astype(
        self, data: bytes, cols: List[Dict[str, Any]], astype: str
    ) -> Union[tuple, dict, numpy.ndarray, pandas.DataFrame, bytes]:
        """Map the astype argument to corresponding methods."""
        if cols == []:
            cols = self.header["data"]
        else:
            cols = [_col for _col in self.header["data"] if _col["key"] in cols]
            pass

        def DataFormatError(e: str = ""):
            return ValueError(
                e + "\nThis may caused by wrong specification of data format."
                "Try ``db.open_table(table_name).recovered.read()`` instead of"
                "``db.open_table(table_name).read()``."
            )

        if astype in ["tuple"]:
            return self._astype_tuple(data, cols)

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

        return

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
            fmt = self.endian + "".join([col["format"] for col in cols])
            keys = [col["key"] for col in cols]
            dict_ = {k: v for k, v in zip(keys, struct.unpack_from(fmt, data, offset))}

            size = struct.calcsize(fmt)
            offset += size

            dictlist.append(dict_)
            continue

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

        def struct2arrayprotocol(fmt):
            fmt = fmt.replace("s", "a")  # numpy type strings for bytes are ["S", "a"]
            return numpy.dtype(fmt).newbyteorder(self.endian).str

        keys = [col["key"] for col in cols]
        dtype = [struct2arrayprotocol(col["format"]) for col in cols]

        return numpy.frombuffer(data, [(k, f) for k, f in zip(keys, dtype)])

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
        self.endian = ""
        for dat in self.header["data"]:
            dat["format"] = dat["format"].replace("i", "?")
        return self


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
