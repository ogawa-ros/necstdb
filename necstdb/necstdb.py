#!/usr/bin/env python3

import os
import mmap
import struct
import pathlib
import json
import tarfile

import numpy
import pandas


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
        return sorted([t.stem for t in self.path.glob('*.data')])
    
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
        return table(self.path, name, mode)

    def checkout(self, saveto, compression=None):
        mode = 'w:'
        if compression is not None:
            mode += compression
            pass
        tar = tarfile.open(saveto, mode=mode)
        tar.add(self.path)
        tar.close()
        return

    def get_info(self):
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
    dbpath = ''
    fdata = None
    header = {}
    record_size = 0
    format = ''
    stat = None
    nrecords = 0
    
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
        self.stat = pdata.stat()
        self.nrecords = self.stat.st_size // self.record_size
        return
    
    def close(self):
        self.fdata.close()
        return

    def append(self, *data):
        self.fdata.write(struct.pack(self.format, *data))
        return

    def read(self, num=-1, start=0, cols=[], astype='tuple'):
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
                else:
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

    def _astype_tuple(self, data, cols):
        fmt = ''.join([c['format'] for c in cols])
        return tuple(struct.iter_unpack(fmt, data))

    def _astype_dict(self, data, cols):
        count = 0
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

    def _astype_data_frame(self, data, cols):
        d = self._astype_dict(data, cols)
        return pandas.DataFrame.from_dict(d)
    
    def _astype_structured_array(self, data, cols):
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
    

def opendb(path):
    return necstdb(path)
