# dbman
Low Level Database I/O Adapter to A Pure Python Database Driver

# Demo
```
>>> # make a configuration file with yaml format
>>> configuration = {
...  'foo_label': {
...     'driver': 'pymysql',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db': 'foo'},
...     },
...  'bar_label': {
...     'driver': 'MySQLdb',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'passwd': '', 'port': 3306, 'db': 'bar'},
...     },
...  'baz_label': {
...     'driver': 'pymssql',
...     'connect_kwargs': {'host': 'localhost', 'user': 'root', 'password': '', 'port': 1433, 'database': 'baz'},
...     },
... }
>>> import os
>>> import yaml
>>> db_conf_path = os.path.join(os.path.expanduser("~"), 'dbconfig.yaml')
>>> with open(db_conf_path, 'w') as fp:
...     yaml.dump(configuration, fp)
...
>>> from dbman import RWProxy
>>> proxy = RWProxy(db_config=db_conf_path, db_label='foo_label')
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
>>> proxy.fromdb('select * from point;')
+---+---+---+
| x | y | z |
+===+===+===+
| 1 | 9 | 9 |
+---+---+---+
| 2 | 9 | 9 |
+---+---+---+
| 3 | 9 | 9 |
+---+---+---+

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

##### `.db_config`: a yaml file path
##### `.db_label`: a string represents default database schema
##### `.driver`: a package name of underlying database driver, 'pymysql' will be assumed by default.
##### ``.set``(db_config, db_label, [driver]): does basic configuration for this module.

```
>>> from dbman import BasicConfig, RWProxy
>>> BasicConfig.set(db_config=db_conf_path, db_label='foo_label') 
>>> # with statement auto close connection/auto commit.
>>> with RWProxy() as proxy:
...     proxy.cursor().execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
...
>>>
```
   	

# Obtain a new connection, `BasicConfig.driver` will be used if the argument `driver` is omitted.
```
>>> from dbman import connect
>>> connect(host='localhost', user='root', passwd='', port=3306, db='foo')
>>> connect(driver='MySQLdb', host='localhost', user='root', passwd='', port=3306, db='foo') 
>>> connect(driver='pymysql', host='localhost', user='root', passwd='', port=3306, db='bar') 
>>> connect(driver='pymssql', host='localhost', user='root', password='', port=1433, database='baz') 
```

### class ``dbman.RWProxy``([connection, [driver, [db_config, [db_label]]]]):
A connection proxy class which method `.fromdb()` for reading and `.todb()` for writing.
The argument `connection` should be an connection object this proxy bind with.
The argument `driver` is a package name of underlying database drivers that clients want to use, `BasicConfig.driver`
      will be used if it's omitted.
The argument `db_config` is a yaml file path, `BasicConfig.db_config` will be used if it's omitted.
The argument `db_label` is a string represents a schema, `BasicConfig.db_label` will be used if it's omitted.


### `RWProxy.fromdb`(select_stmt, args=None, latency=False)
Argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
fetch and wrap all data immediately if the argument `latency` is `False`


### `RWProxy.todb`(table, table_name, mode='insert', with_header=True, slice_size=128, unique_key=())
this method return a number that describes affected row number<br/>
the argumen `table` can be a `petl.util.base.Table` 
or a sequence like: [header, row1, row2, ...] or [row1, row2, ...].<br />
the argument `table_name` is the name of a table in this schema.<br />
the argument `mode`:<br />
    execute SQL INSERT INTO Statement if `mode` equal to 'insert'.<br/>
    execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.<br/>
    execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'.<br/>
    execute SQL INSERT INTO Statement before attempting to execute SQL TRUNCATE TABLE Statement
        if `mode` equal to 'truncate'.<br/>
    execute SQL INSERT INTO Statement before attempting to automatically create a database table which requires
      `SQLAlchemy <http://www.sqlalchemy.org/>` to be installed if `mode` equal to 'create'<br/>
the argument `unique_key` must be present if the argument `mode` is 'update', otherwise it will be ignored.<br />
the argument `with_header` should be `True` if the argument `table` with header, otherwise `False`.<br />
the argument `slice_size` used to slice `table` into many subtable with `slice_size`, 1 transaction for 1 subtable.<br />
