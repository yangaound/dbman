# dbman
Pure Python I/O Interface to Database Driver

### class ``dbman.setting``:
Basic configuration for this module

##### setting.file: a yaml filename 
##### setting.ID: a string represents default database schema
##### setting.driver: a package name of underlying database driver that users want to use

### ``dbman.base_setting``(file, ID=None, driver=None):
Does basic configuration for this module.
```
>>> configuration = {
... 'foo': {
...     'driver': 'pymysql',
...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'foo'},
...     },
... 'bar': {
...     'driver': 'MySQLdb',
...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'bar'},
...     },
... }
>>> import yaml
>>> with open('dbconfig.yaml', 'w') as fp:
...     yaml.dump(configuration, fp)
...
>>> import dbman
>>> dbman.base_setting(file='dbconfig.yaml', ID='foo', driver='pymysql') 
```
   
   
### class ``dbman.Connector``([file, [ID, [driver]]] ):
This class obtains and maintains a connection to a database scheme.<br />
Keyword argument `file` should be a dictionary object or a yaml filename, `dbman.setting.file` will be used if it's<br />
omitted. If the `file` is a yaml filename, loading the content as configuration; the dictionary or yaml content<br />
which will either passed directly to the underlying DBAPI ``connect()`` method as additional keyword arguments.<br />
Keyword argument `ID` is a string represents a database schema, `dbman.setting.ID` will be used if it's omitted.<br />
Keyword argument `driver` is a package name of underlying database driver, it values can be
{'pymysql' | 'MySQLdb' | 'pymssql'}. `dbman.setting.driver` will be used if it's omitted.<br />
	
```
>>> import dbman
>>> dbman.base_setting(file='dbconfig.yaml', ID='foo', driver='pymssql')
>>> connector = dbman.Connector()              # instantialize Connector with basic configuration
>>> connector.driver                           # using underlying driver name
>>> connector._connection                      # associated connection object
>>> connector._cursor                          # associated cursor object
>>> connector.connection                       # connection object
>>> connector.cursor()                         # call cursor factory method to obtains a new cursor object
>>> from pymysql.cursors import DictCursor
>>> connector.cursor(cursorclass=DictCursor)   # obtains a new customer cursor object
>>> connector._cursor.execute('select now();') # execute sql
>>> connector._cursor.fetchall()               # fetch result
>>> connector.close()
>>> # file is a dict
>>> dbman.Connector(file={'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'foo'}) 
>>> # with statement Auto close connection/Auto commit. 
>>> with Connector() as cursor:                # with statement return cursor instead of connector
>>>	cursor.execute('select now();')
>>>	cursor.fetchall()
```

### Connector.``connect``(driver=None, **kwargs):
obtains a connection.

```
>>> from dbman import Connector
>>> Connector.connect(driver='pymysql', host='localhost', user='bob', passwd='****', port=3306, db='foo') 
```

### class ``dbman.Manipulator``(connection=None, **kwargs):
This class used for database I/O. argument `connection` should be a connection object, if `connection` is None, 
`kwargs` will be passed to `dbman.Connector` to obtains a connection, otherwise wraps the `connection` and ignores `kwargs`.

### Manipulator.todb(table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=())
Write database method.<br />
:param table: data container, a `petl.util.base.Table` or a sequence like: [header, row1, row2...]. <br />
:param table_name: the name of a table in this schema.<br />
:param mode:<br />
	execute SQL INSERT INTO Statement if mode equal to 'insert'.<br />
	execute SQL REPLACE INTO Statement if mode equal to 'replace'.<br />
	execute SQL INSERT ... ON DUPLICATE KEY UPDATE` Statement if mode equal to 'update'(only mysql).<br />
 	execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if mode equal to 'truncate'.<br />
:param duplicate_key: it must be present if the argument mode is 'update', otherwise it will be ignored.<br />
:param with_header: specify True(default) if the argument table with header, otherwise specify False.<br />
:param slice_size: the table will be sliced to many subtable with slice_size, 1 transaction for 1 subtable.<br />
:return: affectted row number

```
>>> from dbman import Manipulator
>>> 
>>> manipulator = Manipulator(ID='bar')                  # connect to another schema 'bar'
>>> table_header = ['x', 'y', 'z']
>>> table = [table_header, [1, 1, 1], [2, 1, 1]]
>>> manipulator.create_table(table, table_name='Point')  # create table named 'Point' in schema 'bar'
>>>
>>> # with header table
>>> table = [['x', 'y'], [3, 1], [4, 1]
>>> manipulator.todb(table, table_name='Point')
>>>
>>> # None header table,
>>> table = [[5, 1, 1], [6, 1, 1]]
>>> manipulator.todb(table, table_name='Point', with_header=False)
>>>
>>> # manual modify table 'Point', set field 'x' is primary key.
>>> table = [[5, 88, 88], [6, 88, 88]]
>>> manipulator.todb(table, table_name='Point', with_header=False, mode='replace')  # replace duplication
>>>
>>> # sliced big table to many sub-table with specified size, 1 sub-table 1 transaction.
>>> big_table = [[1, 88, 88], [2, 88, 88] ......]
>>> manipulator.todb(big_table, table_name='Point', with_header=False, slice_size=128)
>>>
>>> # update table
>>> table = [['x', 'y'], [1, 88,], [2, 88,]]
>>> manipulator.todb(table, table_name='Point', mode='update', duplicate_key=('x',))
>>> # check executeed sql
>>> sql = manipulator.writer.make_sql() # return a SQL String or Iterator<SQL String>
>>> sql if isinstance(sql, basestring) else [s for s in sql]   # show sql
>>> manipulator.close()
```
	
	
### Manipulator.fromdb(select_stmt, *petl_args, **petl_kwargs)
fetch and wrap all data immediately if latency is `False`

```
>>> from dbman import Manipulator 
>>> with Manipulator(ID='bar') as manipulator:
>>>     table = manipulator.fromdb("select * from Point;", )
>>> table
```
    
