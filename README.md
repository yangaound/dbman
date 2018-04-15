# dbman
Low Level Database I/O Adapter to A Pure Python Database Driver

# QuickStart
```
>>> # make a configuration file with yaml format
>>> configuration = {
... 'foo_label': {
...     'driver': 'MySQLdb',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db': 'foo'},
...     },
...
... 'bar_label': {
...     'driver': 'pymysql',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db': 'bar'},
...     },
... 'baz_label': {
...     'driver': 'pymssql',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'password': '', 'port': 1433, 'database': 'baz'},
...     },
... }
>>> import yaml
>>> with open('dbconfig.yaml', 'w') as fp:
...     yaml.dump(configuration, fp)
...
>>> from dbman import BasicConfig, RWProxy
>>> # does basic configuration
>>> BasicConfig.set(db_config='dbconfig.yaml', db_label='foo_label')
>>> # New a `RWProxy` with configuration file
>>> proxy = RWProxy(db_config='dbconfig.yaml', db_label='foo_label')
>>> proxy.close()
>>> # New a `RWProxy` using basic configuration 
>>> with RWProxy() as proxy:
...     pass
...
>>> proxy = RWProxy()
>>> table = [['x', 'y', 'z'], [1, 0, 0]]
>>> proxy.todb(table, table_name='point', mode='create')  # create a table named 'point' in the schema 'foo'
>>> proxy.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
>>> # insert None header table
>>> proxy.todb([[2, 0, 0], [3, 0, 0]], table_name='point', mode='insert', with_header=False)  
2
>>> proxy.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
| 2 | 0 | 0 |
+---+---+---+
| 3 | 0 | 0 |
+---+---+---+

>>> proxy.cursor().execute('ALTER TABLE `point` ADD PRIMARY KEY(`x`);')  # set field 'x' as primary key
0
>>> # replace rows with the same pk(key 'x')
>>> proxy.todb([[2, 5, 0], [3, 5, 0]], table_name='point', mode='replace', with_header=False)
4
>>> proxy.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 0 | 0 |
+---+---+---+
| 2 | 5 | 0 |
+---+---+---+
| 3 | 5 | 0 |
+---+---+---+

>>> for sql in proxy.writer.make_sql():   # check executed sql statement
...     print sql
...
REPLACE INTO point VALUES (%s, %s, %s)
>>> table = [['x', 'y', 'z'], [1, 9, 9], [2, 9, 9], [3, 9, 9]]
>>> # updatet if the key 'x' is duplicated otherwise insert
>>> proxy.todb(table, table_name='point', mode='update', duplicate_key=('x', )) 
6
>>> for sql in proxy.writer.make_sql():
...     print sql
...
INSERT INTO point (y, x, z) VALUES (9, 1, 9) ON DUPLICATE KEY UPDATE y=9, z=9
INSERT INTO point (y, x, z) VALUES (9, 2, 9) ON DUPLICATE KEY UPDATE y=9, z=9
INSERT INTO point (y, x, z) VALUES (9, 3, 9) ON DUPLICATE KEY UPDATE y=9, z=9
>>> # prevent sql injection
>>> proxy.fromdb('select * from point where x=%(input)s;', {'input': 1})
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 9 | 9 |
+---+---+---+
>>> # slice a big table into many sub-table with specified size, 1 subtable 1 transaction.
>>> big_table = [[1, 88, 88], [2, 88, 88] ......]
>>> proxy.todb(big_table, table_name='point', with_header=False, slice_size=128)
>>> proxy.close()
```


### class ``dbman.BasicConfig``:
Basic configuration for this module

##### `.db_config`: a yaml filename or a dictionary object
##### `.db_label`: a string represents default database schema
##### `.driver`: a package name of underlying database driver, 'MySQLdb' will be assumed by default.
##### ``.set``(db_config, db_label, [driver]): does basic configuration for this module.

```
>>> from dbman import BasicConfig
>>> BasicConfig.set(db_config='dbconfig.yaml', db_label='foo_label') 
```
   
   
### class ``dbman.ConnectionProxy``([db_config, [db_label, [driver]]]):
This class obtains and maintains a connection to a schema.
argument `db_config` is a yaml file path, `BasicConfig.db_config` will be used if it's omitted.
argument `db_label` is a string represents a schema, `BasicConfig.db_label` will be used if it's omitted.
argument `driver` is a package name of underlying database drivers that clients want to use, `BasicConfig.driver`
      will be used if it's omitted.
	
```
>>> from dbman import ConnectionProxy
>>> # instantialize `ConnectionProxy` with basic configuration
>>> proxy1 = ConnectionProxy()
>>> proxy1._driver                                # using underlying driver name
>>> proxy1._connection                            # binded connection object
>>> proxy1._cursor                                # associated cursor object
>>> proxy1.connection                             # a property that maintaines the associated connection object
>>> proxy1.cursor()                               # factory method that creates a cursor object
>>> # instantialize `ConnectionProxy` with basic configuration to schema 'bar'
>>> proxy2 = ConnectionProxy(db_label='bar_label')
>>> from MySQLdb.cursors import DictCursor as C1
>>> from pymysql.cursors import DictCursor as C2
>>> proxy1.cursor(cursorclass=C1)                 # obtains a new customer cursor object depends on dirver 'MySQLdb'
>>> proxy2.cursor(cursorclass=C2)                 # obtains a new customer cursor object depends on dirver 'pymysql'
>>> proxy1.close()
>>> proxy2.close()
>>> # with statement Auto close connection/Auto commit.
>>> with ConnectionProxy() as cursor:             # with statement return cursor instead of ConnectionProxy
>>>     cursor.execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
```

# obtain a new connection, `BasicConfig.driver` will be used if the argument `driver` is omitted.
```
>>> from dbman import connect
>>> connect(host='localhost', user='root', passwd='', port=3306, db='foo')
>>> connect(driver='MySQLdb', host='localhost', user='root', passwd='', port=3306, db='foo') 
>>> connect(driver='pymysql', host='localhost', user='root', passwd='', port=3306, db='bar') 
>>> connect(driver='pymssql', host='localhost', user='root', password='', port=1433, database='baz') 
```

### class ``dbman.RWProxy``([db_config, [db_label, [driver, [connection]]]]):
This class inherits `dbman.ConnectionProxy` and add 2 methods: `fromdb()` for read and `todb()` for write.
if the argument connection is no None, this proxy will bind with, otherwise `db_config`, `db_label` and `driver` 
will be passed to supper to obtain a connection.

### `RWProxy.fromdb`(select_stmt, args=None, latency=True)
Argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
fetch and wrap all data immediately if the argument `latency` is `False`


### `RWProxy.todb`(table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=())
this method return a number that describes affected row number<br/>
the argumen `table` can be a `petl.util.base.Table` 
or a sequence like: [header, row1, row2, ...] or [row1, row2, ...].<br />
the argument `table_name` is the name of a table in this schema.<br />
the argument `mode`:<br />
	execute SQL INSERT INTO Statement if `mode` equal to 'insert'.<br />
	execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.<br />
	execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'(only mysql).<br />
 	execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if `mode` equal to 'truncate'.<br />
	create a table and insert data into it if `mode` equal to 'create'.
the argument `duplicate_key` must be present if the argument `mode` is 'update', otherwise it will be ignored.<br />
the argument `with_header` should be `True` if the argument `table` with header, otherwise `False`.<br />
the argument `slice_size` used to slice `table` into many subtable with `slice_size`, 1 transaction for 1 subtable.<br />
