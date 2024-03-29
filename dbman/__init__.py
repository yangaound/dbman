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


__version__ = '2.0.0'

default_db_conf_path = os.path.join(os.path.expanduser("~"), 'dbconfig.yaml')
default_db_label = '127.0.0.1'
default_db_driver = 'pymysql'


class BasicConfig:
    # default configuration file path
    db_config = default_db_conf_path
    # a string represents default database schema
    db_label = default_db_label
    # default package name of underlying database driver.
    driver = default_db_driver

    @classmethod
    def configure(cls, db_config=default_db_conf_path, db_label=default_db_label, driver=default_db_driver):
        """Does basic configuration for this module."""
        cls.db_config = db_config
        cls.db_label = db_label
        cls.driver = driver

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
        config = yaml.safe_load(f)
    return config


class DBProxy(object):
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
    >>> # does basic configuration for this module
    >>> from dbman import BasicConfig, DBProxy
    >>> BasicConfig.configure(db_config=db_conf_path, db_label='foo_label')
    >>> proxy = DBProxy()                           # use basic configuration
    >>> proxy._driver                               # using underlying driver name
    >>> proxy._connection                           # bound connection for proxy
    >>> proxy.connection                            # property that reference to the bound connection
    >>> proxy.cursor()                              # factory method that creates a cursor object
    >>> proxy.close()
    >>>
    >>> # with statement auto commit/close.
    >>> with DBProxy(db_config=db_conf_path, db_label='foo_label') as proxy:
    ...     proxy.cursor().execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
    ...
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

        self._cursor = None
        if connection:
            self._connection = connection                                     # binding connection
            self._driver = driver or BasicConfig.driver
        else:
            db_config = db_config or BasicConfig.db_config
            db_label = db_label or BasicConfig.db_label
            driver = driver or BasicConfig.driver
            if db_config is None:
                self._driver = driver
                self._connect_kwargs = {}
            elif isinstance(db_config, str):
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

    def fromdb(self, select_stmt, args=None, latency=True):
        """argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
        fetch and wrap all data immediately if the argument `latency` is `False`
        """
        temp = petl.fromdb(self.connection, select_stmt, args)
        return temp if latency else petl.wrap([row for row in temp])

    def todb(self, table, table_name, mode='insert', batch_size=128, unique_key=()):
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
        :param batch_size: The `table` will be sliced into many sub-tables with `batch_size`, batch execute sub-table.
        :param unique_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.
        """
        mode = mode.upper()
        if mode == 'CREATE':
            import petl
            cursor = self.connection.cursor()
            cursor.execute("SET SQL_MODE=ANSI_QUOTES")
            return petl.todb(table, cursor, table_name, create=True, commit=True)
        if mode == 'TRUNCATE':
            self.cursor().execute("TRUNCATE TABLE %s;" % table_name)
            mode = 'INSERT'
        kwargs = {
            'connection': self.connection,
            'table': table,
            'table_name': table_name,
            'batch_size': batch_size,
            'batch_commit': False,
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
        self._cursor = self.connection.cursor(**kwargs)
        return self._cursor

    def close(self):
        self._connection.close()


class WriterInterface(object):
    __metaclass__ = abc.ABCMeta

    PREFIX = 'INSERT INTO'
    POSTFIX = ''

    def __init__(self, connection, table, table_name, batch_size, batch_commit):
        self.connection = connection
        self.table_name = table_name
        self.batch_size = batch_size
        self.batch_commit = batch_commit
        if isinstance(table, petl.util.base.Table):
            self.table = table
        else:
            if len(table) > 0:
                if isinstance(table[0], dict):
                    table = petl.wrap(petl.fromdicts(table))
                else:
                    table = petl.wrap(table)
            else:
                table = petl.empty()
        self.table = table
        self.header = self.table.header()
        self.row_count = self.table.nrows()
        self._cursor = None

    def write(self):
        self._cursor = cursor = self.connection.cursor()
        sql_fmt = self._make_query_fmt()
        affected_row_count = 0
        for sub_table in self.__slice_table():
            num = cursor.executemany(sql_fmt, sub_table)
            affected_row_count += (num or 0)
            if self.batch_commit:
                self.connection.commit()
        self.connection.commit()
        return affected_row_count

    def make_sql(self):
        """:return collections.Iterable<unicode>, where unicode is a valid SQL Statement"""
        for row in self.table.dicts():
            yield "%s %s(%s) VALUES (%s) %s %s" % (
                self.PREFIX,
                self._table_name_q(),
                self._fields_q(row.keys()),
                ', '.join(map(self._to_q, row.values())),
                self.POSTFIX,
                self._items_q(),
            )

    def _make_query_fmt(self):
        return "%s %s(%s) VALUES (%s) %s %s" % (
            self.PREFIX,
            self._table_name_q(),
            self._fields_q(),
            self._values_f(),
            self.POSTFIX,
            self._items_q(),
        )

    def __slice_table(self):
        if self.row_count <= self.batch_size:
            yield self.table[1:]
            return

        for loop_count in range(self.row_count // self.batch_size + 1):
            left = loop_count * self.batch_size + 1
            right = (loop_count + 1) * self.batch_size + 1
            if left > self.row_count:
                return
            yield self.table[left:right]

    def _table_name_q(self):
        table_name = self.table_name
        if '.' in table_name:
            tu = table_name.split('.')
            ss = "`%s`.`%s`" % (tu[0], tu[1])
        else:
            ss = "`%s`" % table_name
        return ss.replace('``', '`')

    def _fields_q(self, header=None):
        return ', '.join(["`%s`" % f for f in header or self.header]).replace('%', '%%')

    def _values_f(self):
        return ', '.join(('%s',) * len(self.header))

    def _items_q(self):
        return ''

    def _to_q(self, obj):
        if obj is None:
            sql = 'NULL'
        elif isinstance(obj, numbers.Number):
            sql = str(obj)
        elif isinstance(obj, str):
            sql = "'%s'" % obj.replace("'", "''")
        elif isinstance(obj, (list, tuple)):
            return type(obj)([self._to_q(i) for i in obj])
        else:
            sql = "'%s'" % obj
        return sql


class _InsertingWriter(WriterInterface):
    """"""


class _MySQLReplacing(_InsertingWriter):
    PREFIX = 'REPLACE INTO'


class _MySQLUpdating(WriterInterface):
    POSTFIX = 'ON DUPLICATE KEY UPDATE'

    def __init__(self, connection, table, table_name, batch_size, batch_commit, unique_key):
        super(_MySQLUpdating, self).__init__(connection, table, table_name, batch_size, batch_commit)
        self.unique_key = unique_key
        assert unique_key, 'argument unique_key must be specified'

    def _items_q(self):
        return ', '.join(("`%s`=VALUES(`%s`)" % (f, f) for f in self.header if f not in self.unique_key))
