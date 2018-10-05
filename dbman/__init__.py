# -*- coding: utf-8 -*-
"""
Created on 2016年12月24日

@author: albin
"""

import os
import numbers
import abc

import yaml
import petl


__version__ = '1.0.1'


class BasicConfig:
    # default configuration file path
    db_config = os.path.join(os.path.expanduser("~"), 'dbconfig.yaml')
    # a string represents default database schema
    db_label = None
    # default package name of underlying database driver.
    driver = 'pymysql'

    @classmethod
    def set(cls, db_config, db_label, driver=None, ):
        """Does basic configuration for this module."""
        cls.db_config = db_config
        cls.db_label = db_label
        cls.driver = driver or BasicConfig.driver

    def __init__(self):
        raise NotImplementedError('can not initialize %s' % self.__class__)


def connect(driver=None, **connect_kwargs):
    """ obtain a new connection, `BasicConfig.driver` will be used if the argument `driver` is omitted.

    E.g.,
    >>> from dbman import connect
    >>> connect(host='localhost', user='root', passwd='', port=3306, db='foo')
    >>> connect(driver='MySQLdb', host='localhost', user='root', passwd='', port=3306, db='foo')
    >>> connect(driver='pymysql', host='localhost', user='root', passwd='', port=3306, db='bar')
    >>> connect(driver='pymssql', host='localhost', user='root', password='', port=1433, database='baz')
    """

    driver = driver or BasicConfig.driver
    driver in globals() or globals().update({driver: __import__(driver)})
    connection = globals()[driver].connect(**connect_kwargs)
    return connection


def load_db_config(db_config):
    with open(db_config) as f:
        config = yaml.load(f)
    return config


class RWProxy(object):
    """a connection proxy class which method `.fromdb()` for reading and `.todb()` from writing
    E.g.,

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
    >>> from dbman import BasicConfig, RWProxy
    >>> # with statement auto close connection/auto commit.
    >>> with RWProxy(db_config='dbconfig.yaml', db_label='foo_label') as proxy:
    ...     proxy.cursor().execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
    ...
    >>> # does basic configuration for this module
    >>> BasicConfig.set(db_config='dbconfig.yaml', db_label='foo_label')
    >>> proxy1 = RWProxy()                           # use basic configuration
    >>> proxy1._driver                               # using underlying driver name
    >>> proxy1._connection                           # bound connection object
    >>> proxy1.connection                            # connection property
    >>> proxy1.cursor()                              # factory method that creates a cursor object
    >>> # new a `RWProxy` with basic configuration to schema 'bar'
    >>> proxy2 = ConnectionProxy(db_label='bar_label')
    >>> from pymysql.cursors import DictCursor as C1
    >>> from MySQLdb.cursors import DictCursor as C2
    >>> proxy1.cursor(cursorclass=C1)                # obtains a new customer cursor object depends on dirver 'pymysql'
    >>> proxy2.cursor(cursorclass=C2)                # obtains a new customer cursor object depends on dirver 'MySQLdb'
    >>> proxy1.close()
    >>> proxy2.close()
    """

    def __init__(self, connection=None, driver=None, db_config=None, db_label=None):
        """
        :param connection: a connection object this proxy will associate with.
        :param db_config: a yaml file path, `BasicConfig.db_config` will be used if it's omitted.
        :param db_label: a string represents a schema, `BasicConfig.db_label` will be used if it's omitted.
        :param driver: package name of underlying database drivers that clients want to use, `BasicConfig.driver`
            will be used if it's omitted.
        :type driver: `str` = {'pymysql' | 'MySQLdb' | 'pymssql'}
        """

        if connection:
            assert driver, "argument driver is not present"
            self._connection = connection                                     # binding connection
            self._driver = driver
        else:
            db_config = db_config or BasicConfig.db_config
            db_label = db_label or BasicConfig.db_label
            driver = driver or BasicConfig.driver
            if db_config is None:
                self._driver = driver
                self._connect_kwargs = {}
            elif isinstance(db_config, basestring):
                config = load_db_config(db_config)
                self._driver = config[db_label]['driver']
                self._connect_kwargs = config[db_label]['connect_kwargs']
            else:
                raise TypeError('Unexpected data type in argument "db_config"')
            self._connection = connect(self._driver, **self._connect_kwargs)  # binding connection
        self.writer = None                                                    # for loading data to database

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """commit if successful otherwise rollback"""
        self.connection.rollback() if exc_type else self.connection.commit()
        self.close()
        return False

    def fromdb(self, select_stmt, args=None, latency=False):
        """argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
        fetch and wrap all data immediately if the argument `latency` is `False`
        """
        temp = petl.fromdb(self.connection, select_stmt, args)
        return temp if latency else petl.wrap([row for row in temp])

    def todb(self, table, table_name, mode='insert', with_header=True, slice_size=128, unique_key=()):
        """
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
        :param unique_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.
        :param with_header: specify `True` if the argument `table` with header, otherwise `False`.
        :param slice_size: the `table` will be slice to many subtable with `slice_size`, 1 transaction for 1 subtable.
        """
        mode = mode.upper()
        if mode == 'CREATE':
            from petl.io.db_create import create_table
            create_table(table, self.connection, table_name)
            mode = 'INSERT'
        if mode == 'TRUNCATE':
            self.cursor().execute("TRUNCATE TABLE `%(table_name)s`;", {'table_name': table_name})
            mode = 'INSERT'
        kwargs = {
            'connection': self.connection,
            'table': table,
            'table_name': table_name,
            'with_header': with_header,
            'slice_size': slice_size,
        }
        if (mode == 'UPDATE') and self._driver and ('MYSQL' in self._driver.upper()):
            self.writer = _MySQLUpdating(unique_key=unique_key, **kwargs)
        elif mode == 'INSERT':
            self.writer = _InsertingWriter(**kwargs)
        elif mode == 'REPLACE':
            self.writer = _MySQLReplacing(**kwargs)
        else:
            raise AssertionError('The driver "%s" can not handle this mode "%s"' % (self._driver, mode))
        return self.writer.write()

    @property
    def connection(self):
        return self._connection

    def cursor(self, **kwargs):
        """cursor factory method"""
        return self.connection.cursor(**kwargs)

    def close(self):
        self._connection.close()


class _WriterInterface(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def make_sql(self):
        """:return collections.Iterable<unicode>, where unicode is a valid SQL Statement"""

    @abc.abstractmethod
    def write(self):
        """load data to database"""


class _InsertingWriter(_WriterInterface):
    SQL_MODE = 'INSERT INTO'

    def __init__(self, connection, table, table_name, slice_size, with_header):
        self.connection = connection
        self.table_name = table_name
        self.slice_size = slice_size
        self.with_header = with_header

        sequence = table.tuple() if isinstance(table, petl.util.base.Table) else table
        if self.with_header:
            self.header = sequence[0]
            self.content = sequence[1:]
        else:
            self.header = None
            self.content = sequence
        self.row_count = len(self.content)

    def write(self):
        cursor = self.connection.cursor()
        sql = self.make_sql().next()
        affected_row_count = 0
        for sub_table in self.slice_table():
            num = cursor.executemany(sql, sub_table)
            affected_row_count += num
            self.connection.commit()
        return affected_row_count

    def slice_table(self):
        if self.row_count <= self.slice_size:
            yield self.content
            return
        for loop_count in range(self.row_count / self.slice_size):
            yield self.content[loop_count * self.slice_size: (loop_count + 1) * self.slice_size]
        if self.row_count % self.slice_size > 0:
            left = (loop_count + 1) * self.slice_size
            yield self.content[left: left + self.row_count % self.slice_size]

    def make_sql(self):
        if self.header is not None:
            fields_sql = u', '.join(self.header)
            values_sql = u', '.join(('%s',) * len(self.header))
            sql = u"%s %s (%s) VALUES (%s)" % (self.SQL_MODE, self.table_name, fields_sql, values_sql)
        else:
            values_sql = u', '.join(('%s',) * len(self.content[0]))
            sql = u"%s %s VALUES (%s)" % (self.SQL_MODE, self.table_name, values_sql)
        yield sql


class _MySQLReplacing(_InsertingWriter):
    SQL_MODE = 'REPLACE INTO'


class _MySQLUpdating(_WriterInterface):
    def __init__(self, connection, table, table_name, slice_size, with_header, unique_key):
        assert unique_key, 'argument unique_key must be specified'
        assert with_header, 'argument table has not header'
        self.connection = connection
        self.table = table if isinstance(table, petl.util.base.Table) else petl.wrap(table)
        self.table_name = table_name
        self.slice_size = slice_size
        self.unique_key = unique_key

    def make_sql(self):
        sql_statement_fmt = u"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
        for row in self.table.dicts():
            keys = row.keys()
            keys_sql = ', '.join(keys)
            values_sql = ', '.join(map(self.obj2sql, row.values()))
            update_items = map(lambda field: u"%s=%s" % (field, self.obj2sql(row[field])),
                               (k for k in keys if k not in self.unique_key))
            update_items_sql = ', '.join(update_items)
            yield sql_statement_fmt % (self.table_name, keys_sql, values_sql, update_items_sql)

    def write(self):
        cursor = self.connection.cursor()
        affected_row_count = 0
        for i, sql in enumerate(self.make_sql()):
            num = cursor.execute(sql)
            if (i > 0) and (i % self.slice_size == 0):
                self.connection.commit()
            affected_row_count += (num or 0)
        self.connection.commit()
        return affected_row_count

    @staticmethod
    def obj2sql(obj):
        if obj is None:
            sql = '\\N'
        elif isinstance(obj, numbers.Number):
            sql = str(obj)
        elif isinstance(obj, basestring):
            sql = u"'%s'" % obj.replace("'", "''")
        else:
            sql = u"'%s'" % obj
        return sql
