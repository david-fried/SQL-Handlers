import pandas as pd
#import pyodbc
import pypyodbc as pyodbc
import os
import re
from SQLDatabase import SQLDatabase


class AccessDatabase(SQLDatabase):
    """
    import pypyodbc as pyodbc to avoid truncation errors for long text (character length > 255???) when using Access database
    This method inherits from SQLDatabase, which uses pyodbc (not pyodbc).
    You may need to verify that importing pypyodbc at the top of the file in this manner works the way it should.
    I'm not sure the bulk insert method works with Access...May need to test.
    """

    def __init__(self, directory: str, filename: str, password: str):
        self.directory = directory
        self.filename = filename
        self.path = os.path.join(self.directory, self.filename)
        self.password = '' if password is None else f' PWD={password}'
        self.connection_string = 'Driver={Microsoft Access Driver (*.mdb, *.accdb)}; ' + f'DBQ={self.path};' + self.password
        self.stats = os.stat(self.path)
        self.size = os.path.getsize(self.path)
        self.tstamp = os.path.getmtime(self.path)

    def get_tables(self):
        conn = self._conn()
        with conn:
            c = conn.cursor()
            tablenames = [row[2] for row in c.tables(tableType='TABLE')]
        return tablenames

    def get_table_columns(self, tablename):
        conn = self._conn()
        with conn:
            c = conn.cursor()
            columnnames= [row[3] for row in c.columns(table=tablename)]
        return columnnames

    def get_all_columns(self):
        tablenames = self.get_tables()
        conn = self._conn()
        with conn:
            c = conn.cursor()
            all_columns = {}
            for tablename in tablenames:
                all_columns[tablename] = [row[3] for row in c.columns(table=tablename)]
        return all_columns
    
    def get_views(self):
        pass
    
    def get_view_definition(self, view_name):
        pass
