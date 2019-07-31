#!/usr/bin/env python3

import json
import pathlib
import struct

class necstdb(object):
    path = ''

    def __init__(self, path):
        self.opendb(path)
        pass

    def opendb(self, path):
        if not isinstance(path, pathlib.Path):
            path = pathlib.Path(path)
            pass

        self.path = path
        path.mkdir(parents=True, exist_ok=True)
        return

    def list_tables(self):
        return [t.stem for t in self.path.glob('*.data')]

    def create_table(self, name, config):
        list = self.list_tables()
        if name in list:
            pass
        else:
            data = self.path / (name + '.data')
            data.touch()
            header = self.path / (name + '.header')
            with header.open(mode='w') as f:
                json.dump(config,f)
                pass
            return

    def open_table(self, name, mode='rb'):
        table_ = table(self.path, name, mode)
        return table_


class table(object):
    dbpath = ''
    fdata = None
    header = {}

    def __init__(self, dbpath, name, mode):
        self.dbpath = dbpath
        self.open(name, mode)
        pass

    def open(self, table, mode):
        data = self.dbpath / (table + '.data')
        header = self.dbpath / (table + '.header')

        if not(data.exists() and header.exists()):
            raise(Exception("table '{name}' does not exist".format(**locals())))

        self.fdata = open(data, mode)
        fheader = open(header, 'r')
        self.header = json.load(fheader)
        fheader.close()
        self.record_size = sum([h['size'] for h in self.header['data']])
        return

    def close(self):
        fdata.close()
        return

    def append(self, *data):
        format = ''.join([h['format'] for h in self.header['data']])
        self.fdata.write(struct.pack(format, *data))
        return

    def read(self, num=-1, start=0, cols=[], astype=None):
        self.fdata.seek(start * self.record_size)

        if cols == []:
            d = self._read_all_cols(num)
        else:
            d = self._read_specified_cols(num, cols)
            pass

        return self._astype(d, cols, astype)

    def _read_all_cols(self, num):
        if num == -1:
            size = num
        else:
            size = num * self.record_size
            pass
        return self.fdata.read(size)

    def _read_specified_cols(self, num, cols):
        sizes = []
        seeks = []
        i = 0
        draw = b''
        while i < num:
            for size, seek in zip(sizes, seeks):
                draw += self.fdata.read(size)
                self.fdata.seek(seek, 1)
                continue
            i += 1
            continue
        return draw

    def _astype(self, data, cols, astype):
        if cols == []:
            cols = self.header['data']
        else:
            cols = [c for c in self.header['data'] if c['key'] in cols]
            pass

        if astype in [None, 'tuple']:
            return self._astype_tuple(data, cols)
        if astype in ['dict']:
            return self._astype_dict(data, cols)
        elif astype in ['pandas']:
            return self._astype_pandas(data, cold)
        return

    def _astype_tuple(self, data, cols):
        format = ''.join([c['format'] for c in cols])
        return tuple(struct.iter_unpack(format, data))


def opendb(path):
    return necstdb(path)
