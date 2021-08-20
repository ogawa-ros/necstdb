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
        
        if path.exists() == True:
            pass

        else:
            if mode == 'w':
                path.mkdir(parents=True)

            elif mode == 'r':
                raise Exception ('this directory not exist!!')
        return

    def list_tables(self) -> List[str]:
        """List all tables within the database."""
        return sorted([table.stem for table in self.path.glob('*.data')])

    def create_table(self, name: str, config: Dict[str, Any]) -> None:
        """Create a pair of data and header files, then write header content."""
        if name in self.list_tables():
            return

        pdata = self.path / (name + '.data')
        pheader = self.path / (name + '.header')

        pdata.touch()
        with pheader.open('w') as f:
            json.dump(config, f)
            pass
        return

    def open_table(self, name: str, mode: str = 'rb', endian: str = "<") -> "table":
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
        mode = 'w:'
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
                'table name': name,
                'file size': table.stat.st_size,
                '#records': table.nrecords,
                'record size': table.record_size,
                'format': table.format,
            }
            dictlist.append(dic)
            table.close()
            continue

        df = pandas.DataFrame(
            dictlist,
            columns = ['table name',
                       'file size',
                       '#records',
                       'record size',
                       'format']
        ).set_index('table name')

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

    dbpath = ''
    fdata = None
    header = {}
    record_size = 0
    format = ''
    stat = None
    nrecords = 0

    def __init__(
        self, dbpath: pathlib.Path, name: str, mode: str, endian: str = "<"
    ) -> None:
        self.dbpath = dbpath
        self.open(name, mode)
        pass

    def open(self, table_name: str, mode: str) -> None:
        """Open a data table of specified topic."""
        data_path = self.dbpath / (table_name + '.data')
        header_path = self.dbpath / (table_name + '.header')

        if not(pdata.exists() and pheader.exists()):
            raise(Exception("table '{name}' does not exist".format(**locals())))

        self.fdata = pdata.open(mode)
        with pheader.open('r') as fheader:
            self.header = json.load(fheader)
            pass

        self.record_size = sum([h['size'] for h in self.header['data']])
        self.format = ''.join([h['format'] for h in self.header['data']])
        self.stat = pdata.stat()
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
        self,
        num: int = -1,
        start: int = 0,
        cols: List[str] = [],
        astype: str = 'tuple'
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
            d = self._read_all_cols(mm, num)
        else:
            d = self._read_specified_cols(mm, num, cols)
            pass

        return self._astype(d, cols, astype)

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
        for _col in self.header['data']:
            if _col['key'] in cols:
                commands.append({'cmd': 'read', 'size': _col['size']})
            else:
                commands.append({'cmd': 'seek', 'size': _col['size']})
                pass
            continue

        if num == -1:
            num = (mm.size() - mm.tell()) // self.record_size

        draw = b''
        for i in range(num):
            for _cmd in commands:
                if _cmd['cmd'] == 'seek':
                    mm.seek(_cmd['size'], os.SEEK_CUR)
                else:
                    draw += mm.read(_cmd['size'])
                    pass
                continue
            continue
        return draw

    def _astype(
        self, data: bytes, cols: List[Dict[str, Any]], astype: str
    ) -> Union[tuple, dict, numpy.ndarray, pandas.DataFrame, bytes]:
        """Map the astype argument to corresponding methods."""
        if cols == []:
            cols = self.header['data']
        else:
            cols = [c for c in self.header['data'] if c['key'] in cols]
            pass

        if astype in ['tuple']:
            return self._astype_tuple(data, cols)

        elif astype in ['dict']:
            return self._astype_dict(data, cols)

        elif astype in ['structuredarray', 'structured_array', 'array', 'sa']:
            return self._astype_structured_array(data, cols)

        elif astype in ['dataframe', 'data_frame', 'pandas']:
            return self._astype_data_frame(data, cols)

        elif astype in ['buffer', 'raw']:
            return data

        return

    def _astype_tuple(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> Tuple[Tuple[Any]]:
        """Read the data as tuple of tuple."""
        fmt = self.endian + ''.join([col['format'] for col in cols])
        return tuple(struct.iter_unpack(fmt, data))

    def _astype_dict(
        self, data: bytes, cols: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Read the data as list of dict."""
        offset = 0
        dictlist = []
        while count < len(data):
            dict_ = {}

            for c in cols:
                d = struct.unpack(c['format'], data[count:count+c['size']])
                if len(d) == 1:
                    d = d[0]
                    pass
                dict_[c['key']] = d
                count += c['size']
                continue

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
            fmt = fmt.replace('c', 'S')
            fmt = fmt.replace('h', 'i2')
            fmt = fmt.replace('H', 'u2')
            fmt = fmt.replace('i', 'i4')
            fmt = fmt.replace('I', 'u4')
            fmt = fmt.replace('l', 'i4')
            fmt = fmt.replace('L', 'u4')
            fmt = fmt.replace('q', 'i8')
            fmt = fmt.replace('Q', 'u8')
            fmt = fmt.replace('f', 'f4')
            fmt = fmt.replace('d', 'f8')
            fmt = fmt.replace('s', 'S')
            return fmt

        keys = [c['key'] for c in cols]
        fmt = [struct2arrayprotocol(c['format']) for c in cols]

        return numpy.frombuffer(data, [(k, f) for k, f in zip(keys, fmt)])

def opendb(path: os.PathLike, mode: str = 'r') -> "necstdb":
    """Quick alias to open a database.

    Parameters
    ----------
    path: PathLike
        Path to the database directory, the direct parent of *.data and *.header files.
    mode: str
        Mode in which the database is opened (e.g. ["rb", "wb", ...]).
    """
    return necstdb(path, mode)
