import pandas as pd
import pyodbc
import sqlalchemy
import urllib
from functools import wraps


class SQLPyodbcHandler:

	"""
		Used to perform CRUD operations on SQL Database
		
		Constructor:
			connection_string (str): Connection string formatted for pyodbc.
			read_only (bool): Specify whether class instances can only perform read_only operations (i.e., querying).

		Methods:
			allow_method(func): 
				Decorator used to disable methods when self.read_only == True.
			query(self,
				sql_query: str,
				parameters: tuple,
				pandas_dataframe=True,
				**kwargs) -> pd.DataFrame:
				Purpose: Used to perform query of the data.
			execute(self, 
				sql_statement: str, 
				*parameters) -> None:
				Purpose: Perform any Create, Update, Insert, Delete operation.
			bulk_insert(self, 
				df: pd.DataFrame, 
				table_name: str, 
				remove_nulls=False,
				handle_nulls=None) -> None:
				Purpose: Perform a bulk insert using pyodbc's fast execute_many.
			iter_execute(self, 
				parameterized_sql_statement: str, 
				values, 
				error_handling ='raise') -> None:
				Purpose: Perform an update/insert operation by looping through each row of data using parameterized sql statement.
	"""

	def __init__(self, connection_string: str, read_only=False):
		"""
		connection_string (str): Formatted for pyodbc.
		read_only (bool): Defaults to False.
		"""
		self.connection_string = connection_string
		self.read_only = read_only

	def allow_method(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			if args[0].read_only:
				print('Cannot perform operation. Access is read-only.')
				return None
			return func(*args, **kwargs)
		return wrapper

	def _conn(self):
		"""Open a database connection"""
		return pyodbc.connect(self.connection_string)
	
	def query(self, sql_query_statement: str, parameters=None, pandas_dataframe=True, **kwargs) -> pd.DataFrame:
		"""
		Purpose: Useful for querying SQL database.

		Required arguments:
			sql_query_statement (str): If parameterized than must pass a tuple of values to the parameters argument.
				
		Optional arguments:
			parameters (tuple): Tuple of values.
			pandas_dataframe (bool): True (default) of False. True returns pandas dataframe and False returns a list of tuples.
			**kwargs: see pandas.read_sql documentation
		"""
		conn = self._conn()
		with conn:
			if pandas_dataframe:
				return pd.read_sql(sql_query_statement, conn, params=parameters, **kwargs)
			return conn.cursor().execute(sql_query_statement, parameters).fetchall()

	@allow_method
	def execute(self, sql_statement: str, *parameters) -> None:
		"""
		Purpose: Perform any Create, Update, Insert, Delete operation.

		Required arguments:
			sql_statement (str): If parameterized than must include *parameters.
				
		Optional arguments:
			*parameters (int, str, float, or datetime)
		"""
		conn = self._conn()
		with conn:
			conn.cursor().execute(sql_statement, parameters)

	@allow_method
	def iter_execute(self, parameterized_sql_statement: str, values, error_handling ='raise') -> None:
		"""
		Purpose: Perform an update/insert operation several times using parameterized sql statement.

		Required arguments:

			parameterized_sql_statement (str): Used to perform either update or insert operation.
				Must be parameterized (i.e., contain question marks in place of values; see pyodbc documentation)
					Example statement: 'INSERT INTO USERS (ID, Name) VALUES (?, ?);'

			values (iterable): The data to insert or update. Must be an iterable of tuples.
				Each tuple in iterable must contain number of values that equal the number 
				of parameter markers (i.e., question marks). 
					For example, if there are two parameter markers,
					each tuple must be of length equal to 2.
						[(1, 'Mary'),
						(2, 'Bob'),
						(3, 'Bill')]

			error_handling: If one statement fails raise exception ('raise') or ignore ('continue')
				values: 'raise' (default) or 'continue'
		"""
		if '?' not in parameterized_sql_statement:
			raise ValueError('Sql statement must be parameterized.')

		conn = self._conn()

		if error_handling =='raise': # raise exception and rollback all transactions (pyodbc default)

			with conn:
				c = conn.cursor()
				for row in values:
					c.execute(parameterized_sql_statement, row)

		elif error_handling == 'ignore': # commit each row of values and ignore exceptions

			with conn:
				c = conn.cursor()
				for row in values:
					try:
						c.execute(parameterized_sql_statement, row)
					except Exception as e:
						print()
						print(e)
						print(row)
						print()
					else:
						c.commit()
		else:
			raise TypeError("Invalid argument for 'error_handling'. Argument must be 'raise' or 'ignore'.")


	@allow_method
	def bulk_insert(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
		"""
		Purpose: Perform a bulk insert using pyodbc's fast execute_many.

		Required arguments:
			df (pd.DataFrame): Pandas DataFrame containing the data to insert.
			table_name (str): Name of SQL database table to insert the data.

		Optional arguments:
			remove_nulls (bool): Remove nulls before inserting. Defaults to False.
			handle_nulls (None, str): Prevent errors when inserting data. Specify '*' for all columns. This is usually the best option. Otherwise, specify a column name containing nulls.
			identity_insert_on (bool): False (default) or True.
		"""
		remove_nulls = kwargs.get('remove_nulls', False)
		handle_nulls = kwargs.get('handle_nulls', None)
		identity_insert_on = kwargs.get('identity_insert_on', False)

		if remove_nulls:
			df.dropna(inplace=True)

		if handle_nulls == '*':
			for col in df.columns:
				if df[col].dtype == 'object':
					df[col] = df[col].fillna('') #This prevents Nones being inserted in columns with strings which can result in weird encoding/decoding
			df = df.astype(object).where(pd.notnull(df), None)
			self._insert_values(df, table_name, identity_insert_on)

		elif handle_nulls is not None:
			values, nulls = self._separate_nulls(df, handle_nulls)
			self._insert_values(values, table_name, identity_insert_on)
			self._insert_values(nulls, table_name, identity_insert_on)

		else:
			self._insert_values(df, table_name, identity_insert_on)

	@allow_method
	def _insert_values(self, df: pd.DataFrame, table_name: str, identity_insert_on: bool) -> None:
		"""helper method for bulk_insert method"""
		c = list(df.columns)
		columns = ','.join(c)
		params = ','.join(['?' for i in range(len(c))])
		sql_insert_statement = f'INSERT INTO {table_name} ({columns}) VALUES ({params});'
		if identity_insert_on:
			sql_insert_statement = f'SET IDENTITY_INSERT {table_name} ON; ' + sql_insert_statement + f' SET IDENTITY_INSERT {table_name} OFF;'
		values = df.values.tolist()
		if len(values) > 0:
			conn = self._conn()
			conn.autocommit=False
			with conn:
				c = conn.cursor()
				c.fast_executemany = True
				c.executemany(sql_insert_statement, values)
				c.commit()

	def _separate_nulls(self, df: pd.DataFrame, column: str) -> (pd.DataFrame, pd.DataFrame):
		"""helper method if a column for handle_nulls parameter is specified for bulk_insert method."""
		values = df.copy()
		values = values.loc[~values[column].isna()]
		nulls = df.copy()
		nulls = nulls.loc[nulls[column].isna()]
		nulls[column] = nulls[column].astype(str)
		nulls[column] = None
		return values, nulls

class SQLAlchemyHandler(SQLPyodbcHandler):

	"""constructor takes connection_string (str) formatted for MS SQL Server and pyodbc
		note that bulk_insert is the only 'public' method in this class
		While the SQLPyodbcHandler is my goto Handler, the bulk insert in this class
		is useful in situations where you want to create a new table in a SQL database from a pandas dataframe
		or when bulk_insert method in SQLPyodbcHandler is failing because of weird characters

	"""
	def _conn(self):
		return self._sqlalchemy_conn()

	def _sqlalchemy_conn(self):
		quoted = urllib.parse.quote_plus(self.connection_string)
		x='mssql+pyodbc:///?odbc_connect={}'.format(quoted)
		engine = sqlalchemy.create_engine(x)
		return engine

	def bulk_insert(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
		super().bulk_insert(df, table_name, **kwargs)

	def _insert_values(self, df: pd.DataFrame, table_name: str, insert_identity_on: bool) -> None:
		"""helper method for bulk_insert method"""
		df.to_sql(table_name, schema='dbo', con=self._conn(), if_exists='append', index=False)

	def _separate_nulls(self, df: pd.DataFrame, column: str) -> (pd.DataFrame, pd.DataFrame):
		return super()._separate_nulls(df, column)

	def query(self):
		pass

	def execute(self):
		pass

	def iter_execute(self):
		pass
