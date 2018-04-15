# -*- coding: utf-8 -*-
"""
Source:
    https://github.com/yangaound/dbman

Created on 2016年12月24日

@author: albin
"""

import numbers
import abc

import yaml
import petl


__version__ = '1.0.1'


class BasicConfig:
    # configuration file path with yaml format
    db_config = None
    # a string represents default database schema
    db_label = None
    # a package name of underlying database driver, 'pymysql' will be assumed by default.
    driver = 'pymysql'

    @staticmethod
    def set(db_config, db_label, driver=None, ):
        """
        Does basic configuration for this module.

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
        >>> import yaml
        >>> with open('dbconfig.yaml', 'w') as fp:
        ...     yaml.dump(configuration, fp)
        ...
        >>>
        >>> from dbman import BasicConfig, ConnectionProxy, RWProxy
        >>> # does basic configuration
        >>> BasicConfig.set(db_config='dbconfig.yaml', db_label='foo_label')
        >>> proxy = RWProxy()
        >>> proxy.close()
        >>> # with statement Auto close connection/Auto commit.
        >>> with ConnectionProxy() as cursor:  # with statement return cursor instead of ConnectionProxy
        ...     cursor.execute('INSERT INTO point (y, x, z) VALUES (10, 10, 10);')
        ...
        >>>
        """

        BasicConfig.db_config = db_config
        BasicConfig.db_label = db_label
        BasicConfig.driver = driver or BasicConfig.driver


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


class ConnectionProxy(object):
    """ This class obtains and maintains a connection to a schema
    E.g.,
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
    """

    def __init__(self, db_config=None, db_label=None, driver=None, ):
        """
        :param db_config: a yaml file path, `BasicConfig.db_config` will be used if it's omitted.
        :param db_label: a string represents a schema, `BasicConfig.db_label` will be used if it's omitted.
        :param driver: package name of underlying database drivers that clients want to use, `BasicConfig.driver`
            will be used if it's omitted.
        :type driver: `str` = {'pymysql' | 'MySQLdb' | 'pymssql'}
        """

        db_config = db_config or BasicConfig.db_config
        db_label = db_label or BasicConfig.db_label
        driver = driver or BasicConfig.driver

        if isinstance(db_config, basestring):
            with open(db_config) as f:
                yaml_obj = yaml.load(f)
            self._driver = yaml_obj[db_label]['driver']
            self.connect_kwargs = yaml_obj[db_label]['connect_kwargs']
        elif db_config is None:
            self._driver = driver
            self.connect_kwargs = {}
        else:
            raise TypeError("Unexpected data type in argument 'db_config'")
        self.writer = None  # dependency delegator for writing database
        self._connection = connect(self._driver, **self.connect_kwargs)  # associated connection
        self._cursor = self._connection.cursor()  # associated cursor

    def __enter__(self):
        """with statement return a cursor instead of a ``dbman.ConnectionProxy``"""
        return self.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """commit if successful otherwise rollback"""
        self.connection.rollback() if exc_type else self.connection.commit()
        self.close()
        return False

    @property
    def connection(self):
        if hasattr(self._connection, 'open') and not self._connection.open:
            self._connection = connect(self._driver, **self.connect_kwargs)
        return self._connection

    def cursor(self, **kwargs):
        """cursor factory method"""
        return self.connection.cursor(**kwargs)

    def close(self):
        self._cursor.close()
        self._connection.close()


class RWProxy(ConnectionProxy):
    """This class inherits `dbman.ConnectionProxy` and add 2 methods: `fromdb` for read and `todb` for write"""

    def __init__(self, db_config=None, db_label=None, driver=None, connection=None):
        """    
        :param connection: a connection object this proxy will associate with. if `connection` is `None`, arguments
          `db_config`, `db_label` and `driver` will be passed to supper to obtain a connection.
        """
        if connection:
            self._connection = connection
            self._driver = driver or BasicConfig.driver
        else:
            super(RWProxy, self).__init__(db_config=db_config, db_label=db_label, driver=driver)

    def __enter__(self):
        """overwrite"""
        return self

    def fromdb(self, select_stmt, args=None, latency=False):
        """argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
        fetch and wrap all data immediately if the argument `latency` is `False`
        """
        temp = petl.fromdb(self.connection, select_stmt, args)
        return temp if latency else petl.wrap([row for row in temp])

    def todb(self, table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=()):
        """
        :param table: a `petl.util.base.Table` or a sequence like: [header, row1, row2, ...] or [row1, row2, ...]
        :param table_name: the name of a table in this database
        :param mode:
            execute SQL INSERT INTO Statement if `mode` equal to 'insert'.
            execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.
            execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'.
            execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if `mode` equal to 'truncate'.
            create a table and insert data into it if `mode` equal to 'create', this operation depends `SqlAlchemy`.
        :param duplicate_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.
        :param with_header: specify `True` if the argument `table` with header, otherwise `False`.
        :param slice_size: the `table` will be slice to many subtable with `slice_size`, 1 transaction for 1 subtable.
        """
        mode = mode.upper()
        if mode == 'CREATE':
            return self._create_table(table, table_name=table_name)

        if mode == 'TRUNCATE':
            self.cursor().execute("TRUNCATE TABLE `%(table_name)s`;", {'table_name': table_name})
            mode = 'INSERT'
        self.writer = self._make_writer(connection=self.connection, table=table, table_name=table_name, mode=mode,
                                        with_header=with_header, slice_size=slice_size, duplicate_key=duplicate_key)
        return self.writer.write()

    def _make_writer(self, **kwargs):
        mode = kwargs.get('mode').upper()
        if (mode == 'UPDATE') and self._driver and ('MYSQL' in self._driver.upper()):
            writer = _UpdateDuplicateWriter(**kwargs)
        elif mode in ('INSERT', 'REPLACE'):
            writer = _InsertReplaceWriter(**kwargs)
        else:
            raise AssertionError("The driver '%s' can't handle this request" % self._driver)
        return writer

    def _create_table(self, table, table_name, **petl_kwargs):
        if self._driver and ('MYSQL' in self._driver.upper()):
            self.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        petl.todb(table, self.connection, table_name, create=True, **petl_kwargs)


class _WriterInterface(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def make_sql(self):
        """:return collections.Iterator<basestring>, where basestring is a SQL Statement"""

    @abc.abstractmethod
    def write(self):
        """load data to database"""


class _InsertReplaceWriter(_WriterInterface):

    def __init__(self, connection, table, table_name, with_header, mode, slice_size, duplicate_key):
        assert mode in ('INSERT', 'REPLACE'), "Unsupported operation mode '%s'" % mode
        self.connection = connection
        self.table_name = table_name
        self.with_header = with_header
        self.mode = mode
        self.slice_size = slice_size

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
            self.connection.commit()
            affected_row_count += (num or 0)
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
            fields = u', '.join(self.header)
            values_fmt = u', '.join(('%s',) * len(self.header))
            sql = u"%s INTO %s (%s) VALUES (%s)" % (self.mode.upper(), self.table_name, fields, values_fmt)
        else:
            values_fmt = u', '.join(('%s',) * len(self.content[0]))
            sql = u"%s INTO %s VALUES (%s)" % (self.mode.upper(), self.table_name, values_fmt)
        yield sql


class _UpdateDuplicateWriter(_WriterInterface):
    import_mode = ('UPDATE',)

    def __init__(self, connection, table, table_name, with_header, mode, slice_size, duplicate_key):
        assert mode == 'UPDATE', "Unsupported operation mode '%s'" % mode
        assert duplicate_key, 'argument duplicate_key must be specified'
        assert with_header, 'argument table has not header'
        self.connection = connection
        self.table_name = table_name
        self.with_header = with_header
        self.mode = mode
        self.slice_size = slice_size
        self.duplicate_key = duplicate_key
        self.content = table if isinstance(table, petl.util.base.Table) else petl.wrap(table)

    @staticmethod
    def obj2sql(obj):
        if isinstance(obj, numbers.Number):
            sql = str(obj)
        elif isinstance(obj, basestring):
            sql = u"'%s'" % obj.replace("'", "''")
        else:
            sql = u"'%s'" % obj
        return sql

    def make_sql(self):
        sql_statement_fmt = u"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
        for row in self.content.dicts():
            dic = dict((k, v) for k, v in row.items() if v is not None)
            keys = dic.keys()
            keys_sql = ', '.join(keys)
            values_sql = ', '.join(map(self.obj2sql, dic.values()))
            update_items = map(lambda field: u"%s=%s" % (field, self.obj2sql(dic[field])),
                               (k for k in keys if k not in self.duplicate_key))
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
