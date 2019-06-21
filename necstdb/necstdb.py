#!/usr/bin/env python3

import os
import sqlite3
import pickle
import pandas

class necstdb(object):

    def __init__(self):
        self.con = None
        self.cur = None
        self.db_path = ''
        pass

    def __del__(self):
        self.con.close()
        return

    def _check_status(self):
        pass

    def open(self, db_path):
        self._check_status()
        self.db_path = db_path
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self.con.execute("CREATE table if not exists 'necst' ('topic', 'time', 'msgs')")
        self.cur = self.con.cursor()
        return
        
    def commit_data(self):
        self._check_status()
        self.con.commit()
        return

    def close(self):
        self._check_status()
        self.con.close()
        self.db_path = ''
        self.con = None
        self.cur = None
        return

    def write(self, values, auto_commit = False):
        table_name = 'necst'
        quest = "?, ?, ?"
        param = ('topic', 'time', 'msgs')
        val = []
        val.append(values['topic'])
        val.append(values['time'])
        val.append(pickle.dumps(values['msgs']))
        values = tuple(val)
        self.cur.execute("INSERT into {0} {1} values ({2})".format(table_name, param, quest), values)
        return
        
#    def writemany(self, table_name, param, values, auto_commit = False):
#        if len(values[0]) == 1:
#            quest = "?"
#        else:
#            tmp = ""
#            quest = ",".join([tmp + "?" for i in range(len(values[0]))])
#
#        if auto_commit:
#            with self.con:
#                self.cur.executemany("INSERT into {0} {1} values ({2})".format(table_name, param, quest), values)
#        else:
#            self.cur.executemany("INSERT into {0} {1} values ({2})".format(table_name, param, quest), values)
#        return

    def read(self, param="*"):
        table_name = 'necst'
        row = self.cur.execute("SELECT {0} from {1}".format(param, table_name)).fetchall()
        if not row == []:
            data = [[row[i][j] for i in range(len(row))] 
                    for j in range(len(row[0]))]
        else : data = []
        dat = []
        for i in range(len(data)):
            if type(data[i][0]) == bytes:
                dat.append([pickle.loads(data[i][j]) for j in range(len(data[i]))])
            else:
                dat.append(data[i])
        return dat

    def read_as_pandas(self):
        table_name = 'necst'
        df = pandas.read_sql("SELECT * from {}".format(table_name), self.con)
        for i in range(len(df['msgs'])):
            df.loc[i, 'msgs'] = [pickle.loads(df['msgs'][i])]
        return df

#    def read_pandas_all(self):
#        table_name = self.get_table_name()
#        datas = [self.read_as_pandas(name) for name in table_name]
#        if datas ==[]:
#            df_all = []
#        else:
#            df_all = pandas.concat(datas, axis=1)
#        return df_all

    def check_table(self):
        row = self.con.execute("SELECT * from sqlite_master")
        info = row.fetchall()
        return info

    def get_table_name(self):
        name = self.con.execute("SELECT name from sqlite_master where type='table'").fetchall()
        name_list = sorted([name[i][0] for i in range(len(name))])
        return name_list

    def insert(self, dic):
        if self.con is None:
            self.db_path = dic['path']
            if os.path.exists(self.db_path[:self.db_path.rfind('/')]): pass
            else: os.makedirs(self.db_path[:self.db_path.rfind('/')])
            self.open(self.db_path)
        else:
            if dic['path'] != self.db_path:  
                self.finalize()
                if os.path.exists(self.db_path[:self.db_path.rfind('/')]): pass
                else: os.makedirs(self.db_path[:self.db_path.rfind('/')])
                self.open(dic['path'])
            else: pass
        self.write(dic['data'])
        return

    def finalize(self):
        if self.con is None:
            pass
        else:
            self.commit_data()
            self.close()
        return
