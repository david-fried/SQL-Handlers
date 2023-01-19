from ConnectionString import ConnectionString
from SQLHandlers import SQLPyodbcHandler

class SQLDatabase(SQLPyodbcHandler):
    
    """This class has been used with MS SQL Server"""
    
    @classmethod
    def prod(cls, read_only=True):
        return cls(ConnectionString().prod, read_only)

    @classmethod
    def qa(cls, read_only=False):
        return cls(ConnectionString().qa, read_only)

    @classmethod
    def dev(cls, read_only=False):
        return cls(ConnectionString().dev, read_only)

    @classmethod
    def local(cls, read_only=False):
        return cls(ConnectionString().local, read_only)

    def get_tables(self):
        return self.query(
            """
            SELECT * FROM sys.tables
            WHERE SCHEMA_NAME(schema_id) = 'dbo';
            """
            )

    def get_views(self):
        return self.query(
            """
            SELECT * FROM sys.objects
            WHERE type_desc = 'VIEW';
            """
            )

    def get_view_definition(self, view_name):
        return self.query(
            f"""
            EXEC sp_helptext {view_name};
            """
            )
