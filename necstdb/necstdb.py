#!/usr/bin/env python3

import os
import mmap
import struct
import pathlib
import json


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
        if name in self.list_tables():
            return
        
        pdata = self.path / (name + '.data')
        pheader = self.path / (name + '.header')
        
        pdata.touch()
        with pheader.open('w') as f:
            json.dump(config, f)
            pass
        return

    def open_table(self, name, mode='rb'):
        table_ = table(self.path, name, mode)
        return table_


class table(object):
    dbpath = ''
    fdata = None
    header = {}
    record_size = 0
    format = ''

    def __init__(self, dbpath, name, mode):
        self.dbpath = dbpath
        self.open(name, mode)
        pass

    def open(self, table, mode):
        pdata = self.dbpath / (table + '.data')
        pheader = self.dbpath / (table + '.header')

        if not(pdata.exists() and pheader.exists()):
            raise(Exception("table '{name}' does not exist".format(**locals())))

        self.fdata = pdata.open(mode)
        with pheader.open('r') as fheader:
            self.header = json.load(fheader)
            pass
        
        self.record_size = sum([h['size'] for h in self.header['data']])
        self.format = ''.join([h['format'] for h in self.header['data']])
        return

    def close(self):
        self.fdata.close()
        return

    def append(self, *data):
        self.fdata.write(struct.pack(self.format, *data))
        return

    def read(self, num=-1, start=0, cols=[], astype=None):
        mm = mmap.mmap(self.fdata.fileno(), 0, prot=mmap.PROT_READ)
        mm.seek(start * self.record_size)

        if cols == []:
            d = self._read_all_cols(mm, num)
        else:
            d = self._read_specified_cols(mm, num, cols)
            pass

        return self._astype(d, cols, astype)

    def _read_all_cols(self, mm, num):
        if num == -1:
            size = num
        else:
            size = num * self.record_size
            pass
        return mm.read(size)
    
    def _read_specified_cols(self, mm, num, cols):
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
                elif _cmd['cmd'] == 'read':
                    draw += mm.read(_cmd['size'])
                    pass
                continue
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
        
        elif astype in ['dict']:
            return self._astype_dict(data, cols)
        
        elif astype in ['pandas']:
            return self._astype_pandas(data, cold)
        
        return

    def _astype_tuple(self, data, cols):
        format = ''.join([c['format'] for c in cols])
        return tuple(struct.iter_unpack(format, data))


def opendb(path):
    return necstdb(path)
