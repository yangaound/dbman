# dbman
Low Level Database I/O Adapter to A Pure Python Database Driver

# QuickStart
```
>>> # make configuration file
>>> configuration = {
... 'foo': {
...     'driver': 'pymysql',
...     'config': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db':'foo'},
...     },
... 'bar': {
...     'driver': 'MySQLdb',
...     'config': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db':'bar'},
...     },
... }
>>> import yaml
>>> with open('dbconfig.yaml', 'w') as fp:
...     yaml.dump(configuration, fp)
...
>>> import dbman
>>> manipulator = dbman.Manipulator(db_config='dbconfig.yaml', db_label='foo')
>>> table = [['x', 'y', 'z'], [1, 0, 0]]
>>> manipulator.todb(table, table_name='point', mode='create')  # create a table named 'point' in the schema 'foo'
>>> manipulator.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
>>> # insert None header table
>>> manipulator.todb([[2,0,0], [3, 0, 0]], table_name='point', mode='insert', with_header=False)  
2
>>> manipulator.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
| 2 | 0 | 0 |
+---+---+---+
| 3 | 0 | 0 |
+---+---+---+

>>> manipulator.cursor().execute('ALTER TABLE `point` ADD PRIMARY KEY(`x`);')  # set field 'x' as primary key
0
>>> # replace duplicate key 'x'
>>> manipulator.todb([[2,5,0], [3, 5, 0]], table_name='point', mode='replace', with_header=False)
4
>>> manipulator.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
| 2 | 5 | 0 |
+---+---+---+
| 3 | 5 | 0 |
+---+---+---+

>>> for sql in manipulator.writer.make_sql():   # check executed sql statement
...     print sql
...
REPLACE INTO point VALUES (%s, %s, %s)
>>> table = [['x', 'y', 'z'], [1, 9, 9], [2, 9, 9], [3, 9, 9]]
>>> # updatet if the key 'x' is duplicated otherw insert
>>> manipulator.todb(table, table_name='point', mode='update', duplicate_key=('x', )) 
6
>>> for sql in manipulator.writer.make_sql():
...     print sql
...
INSERT INTO point (y, x, z) VALUES (9, 1, 9) ON DUPLICATE KEY UPDATE y=9, z=9
INSERT INTO point (y, x, z) VALUES (9, 2, 9) ON DUPLICATE KEY UPDATE y=9, z=9
INSERT INTO point (y, x, z) VALUES (9, 3, 9) ON DUPLICATE KEY UPDATE y=9, z=9
>>> # prevent sql injection
>>> manipulator.fromdb('select * from point where x=%(input)s;', {'input': 1})
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 9 | 9 |
+---+---+---+
>>> # sliced big table to many sub-table with specified size, 1 subtable 1 transaction.
>>> big_table = [[1, 88, 88], [2, 88, 88] ......]
>>> manipulator.todb(big_table, table_name='point', with_header=False, slice_size=128)
>>> manipulator.close()
```


### class ``dbman.setting``:
Basic configuration for this module

##### setting.db_config: a yaml filename or a dictionary object
##### setting.db_label: a string represents default database schema in yaml file
##### setting.driver: a package name of underlying database driver, 'pymysql' will be assumed by default.

### ``dbman.base_setting``(db_config, db_label=None, driver=None):
Does basic configuration for this module.
```
>>> import dbman
>>> dbman.base_setting(db_config='dbconfig.yaml', db_label='foo') 
```
   
   
### class ``dbman.Connector``([db_config, [db_label, [driver]]] ):
This class obtains and maintains a connection to a schema.<br>
argument `db_config` should be a yaml filename or a dictionary object, `setting.db_config` will be used if it's omitted.
if the argument `db_config` is a yaml filename, loading the content as configuration.
the dictionary or yaml content, which will either to the underlying DBAPI ``connect()`` method as additional keyword arguments.
argument `db_label` is a string represents a schema, `setting.db_label` will be used if it's omitted.
argument `driver` is a package name of underlying database driver that clients want to use, `pymysql` will be assumed if it's omitted.
:type driver: str` = {'pymysql' | 'MySQLdb' | 'pymssql'}
	
```
>>> import dbman
>>> dbman.base_setting(db_config='dbconfig.yaml', db_label='foo', driver='pymysql')
>>> connector = dbman.Connector()              # instantialize Connector with basic configuration
>>> connector.driver                           # using underlying driver name
>>> connector._connection                      # associated connection object
>>> connector._cursor                          # associated cursor object
>>> connector.connection                       # connection object
>>> connector.cursor()                         # call cursor factory method to obtains a new cursor object
>>> from pymysql.cursors import DictCursor
>>> connector.cursor(cursor=DictCursor)   # obtains a new customer cursor object
>>> connector.close()
>>> # `db_config` is a `dict`
>>> dbman.Connector(db_config={'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'foo'}) 
>>> # with statement Auto close connection/Auto commit. 
>>> with dbman.Connector() as cursor:                # with statement return cursor instead of connector
>>>	  cursor.execute('INSERT INTO point (y, x, z) VALUES (1, 10, 9);')  
```

### ``Connector.connect``(driver=setting.driver, **kwargs):
obtains a connection.
```
>>> from dbman import Connector
>>> Connector.connect(host='localhost', user='root', passwd='', port=3306, db='foo') 
```

### class ``dbman.Manipulator``(connection=None, driver=None, **kwargs):
This class inherits `dbman.Connector` and add 2 methods: `fromdb()` for read and `todb()` for write.<br />
argument `connection` should be a connection object. 
argument `driver` is a package name of underlying database driver that clients want to use, `pymysql` will be assumed if it's omitted.
if `connection` is `None`, `kwargs` will be passed to `dbman.Connector` to obtains a connection, otherwise `kwargs` will be ignored.


### Manipulator.`fromdb`(select_stmt, args=None, latency=False)
Argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
fetch and wraps all data immediately if the optional keyword argument `latency` is `True`


### Manipulator.`todb`(table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=())
:param table: data container, a `petl.util.base.Table` or a sequence like: [header, row1, row2, ...] or [row1, row2, ...].<br />
:param table_name: the name of a table in this schema.<br />
:param mode:<br />
	execute SQL INSERT INTO Statement if `mode` equal to 'insert'.<br />
	execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.<br />
	execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'(only mysql).<br />
 	execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if `mode` equal to 'truncate'.<br />
	create a table and insert data into it if `mode` equal to 'create'.
:param duplicate_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.<br />
:param with_header: specify `True` if the argument `table` with header, otherwise specify `False`.<br />
:param slice_size: the `table` will be sliced to many subtable with `slice_size`, 1 transaction for 1 subtable.<br />
:return: affectted row number
