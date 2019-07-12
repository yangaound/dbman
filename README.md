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
>>> # does basic configuration for this module
>>> from dbman import BasicConfig, Proxy
>>> BasicConfig.set(db_config=db_conf_path, db_label='foo_label')
>>> proxy = Proxy()                             # use basic configuration
>>> proxy._driver                               # using underlying driver name
>>> proxy._connection                           # bound connection for proxy
>>> proxy.connection                            # property that reference to the bound connection
>>> proxy.cursor()                              # factory method that creates a cursor object
>>> proxy.close()
>>>
>>> # with statement auto commit/close.
>>> with Proxy(db_config=db_conf_path, db_label='foo_label') as proxy:
...     proxy.cursor().execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
...
>>> from dbman import Proxy
>>> proxy = Proxy(db_config=db_conf_path, db_label='foo_label')
>>> table = [
... {'y': 0, 'x': 1, 'z': 0},
... {'y': 0, 'x': 2, 'z': 0},
... {'y': 0, 'x': 3, 'z': 0},
... ]
>>> # create a table named 'point' in the schema 'foo'
>>> proxy.todb(table, table_name='point', mode='create') 
3
>>> table = proxy.fromdb('select * from point;')
>>> table
+---+---+---+
| y | x | z |
+===+===+===+
| 0 | 1 | 0 |
+---+---+---+
| 0 | 2 | 0 |
+---+---+---+
| 0 | 3 | 0 |
+---+---+---+
>>> table.dicts()
{u'y': 0, u'x': 1, u'z': 0}
{u'y': 0, u'x': 2, u'z': 0}
{u'y': 0, u'x': 3, u'z': 0}
>>> # set field 'x' as primary key
>>> proxy.cursor().execute('ALTER TABLE `point` ADD PRIMARY KEY(`x`);')  
0
>>> # replace rows with the same pk(key 'x')
>>> table = [['x', 'y', 'z'], [2, 5, 0], [3, 5, 0]]
>>> proxy.todb(table, table_name='point', mode='replace')
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

>>> for sql in proxy.writer.make_sql():   # debug sql statement
...     print sql
...
REPLACE INTO `point` (`x`, `y`, `z`) VALUES (5, 2, 0)
REPLACE INTO `point` (`x`, `y`, `z`) VALUES (5, 3, 0)
>>> table = [
... {'y': 9, 'x': 1, 'z': 9},
... {'y': 9, 'x': 2, 'z': 9},
... {'y': 9, 'x': 3, 'z': 9},
... ]
>>> # updatet if the key 'x' is duplicated otherwise insert
>>> proxy.todb(table, table_name='point', mode='update', unique_key=('x', )) 
6
>>> for sql in proxy.writer.make_sql():   # debug sql statement
...     print sql
...
INSERT INTO `point`(`y`, `x`, `z`) VALUES (9, 1, 9) ON DUPLICATE KEY UPDATE `y`=VALUES(`y`), `x`=VALUES(`x`), `z`=VALUES(`z`)
INSERT INTO `point`(`y`, `x`, `z`) VALUES (9, 2, 9) ON DUPLICATE KEY UPDATE `y`=VALUES(`y`), `x`=VALUES(`x`), `z`=VALUES(`z`)
INSERT INTO `point`(`y`, `x`, `z`) VALUES (9, 3, 9) ON DUPLICATE KEY UPDATE `y`=VALUES(`y`), `x`=VALUES(`x`), `z`=VALUES(`z`)
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

>>> # slice a big table into many sub-table with specified size, 1 subtable 1 transaction if batch_commit is True.
>>> big_table = [[1, 88, 88], [2, 88, 88] ......]
>>> proxy.todb(big_table, table_name='point', batch_size=128, batch_commit=True)
>>> proxy.close()
```


### class ``dbman.BasicConfig``:
Basic configuration for this module

##### `.db_config`: a yaml file path
##### `.db_label`: a string represents default database schema
##### `.driver`: a package name of underlying database driver, 'pymysql' will be assumed by default.
##### ``.set``(db_config, db_label, [driver]): does basic configuration for this module.

```
>>> from dbman import BasicConfig, Proxy
>>> BasicConfig.set(db_config=db_conf_path, db_label='foo_label') 
```
   	

# Obtain a new connection, `BasicConfig.driver` will be used if the argument `driver` is omitted.
```
>>> from dbman import connect
>>> connect(host='localhost', user='root', passwd='', port=3306, db='foo')
>>> connect(driver='MySQLdb', host='localhost', user='root', passwd='', port=3306, db='foo') 
>>> connect(driver='pymysql', host='localhost', user='root', passwd='', port=3306, db='bar') 
>>> connect(driver='pymssql', host='localhost', user='root', password='', port=1433, database='baz') 
```

### class ``dbman.Proxy``([connection, [driver, [db_config, [db_label]]]]):
A connection proxy class which method `.fromdb()` for reading and `.todb()` for writing.
The argument `connection` should be an connection object this proxy bind with.
The argument `driver` is a package name of underlying database drivers that clients want to use, `BasicConfig.driver`
      will be used if it's omitted.
The argument `db_config` is a yaml file path, `BasicConfig.db_config` will be used if it's omitted.
The argument `db_label` is a string represents a schema, `BasicConfig.db_label` will be used if it's omitted.


### `Proxy.fromdb`(select_stmt, args=None, latency=True)
Argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
fetch and wrap all data immediately if the argument `latency` is `False`


### `Proxy.todb`(table, table_name, mode='insert',  batch_size=128, batch_commit=False, unique_key=())
        :param table: a `petl.util.base.Table` or a sequence like this:
            [header, row1, row2, ...] or [row1, row2, ...]
        :param table_name: the name of a table in connected database
        :param mode:
            execute SQL INSERT INTO Statement if `mode` equal to 'insert'.
            execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.
            execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'.
            execute SQL INSERT INTO Statement before attempting to execute SQL TRUNCATE TABLE Statement
                if `mode` equal to 'truncate'.
            execute SQL INSERT INTO Statement before attempting to automatically create a database table which requires
              `SQLAlchemy <http://www.sqlalchemy.org/>` to be installed if `mode` equal to 'create'
        :param batch_size: the `table` will be slice to many subtable with `batch_size`, batch execute for 1 subtable.
        :param batch_commit: the `table` will be slice to many subtable with `batch_size`, 1 transaction for 1 subtable if `batch_commit` is True.
        :param unique_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.