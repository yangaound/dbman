# -*- coding: utf-8 -*-
"""
Created on 2016年12月10日

@author: albin
"""

import numbers
import abc

import yaml
import petl

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = None


class setting:
    ID = None
    file = None
    driver = None
    sqlalchemy_uri = None


def base_setting(ID, file, driver=None, sqlalchemy_uri=None):
    """
    :param ID: a string represents a database schema.
    :param file: a yaml filename users configuration.
    :param driver: driver: package name of underlying database driver. `str`={'pymysql' | 'MySQLdb' | 'pymssql'}
    :param sqlalchemy_uri: user for create a sqlalchemy engine
    """
    setting.ID = ID
    setting.file = file
    setting.driver = driver or setting.driver
    setting.sqlalchemy_uri = sqlalchemy_uri or setting.sqlalchemy_uri


class Connector(object):
    """
    This class obtains and maintains a connection to a database scheme.

    :param ID: a string represents a database schema in users configuration file,
        `setting.ID` will be used if it's omitted.

    :param driver:
        package name of underlying database driver that users want to use.
    :type driver:
        `str` = {'pymysql' | 'MySQLdb' | 'pymssql'}

    :param file:
        a yaml filename or a dictionary object, `setting.filename` will be used if it's omitted.
        if the argument file is a yaml filename, loading the content as configuration.
        the dictionary or yaml content, which will either passed directly to the underlying DBAPI
        ``connect()`` method as additional keyword arguments, so the dictionary's key should be follow
        underlying API.

    :type file:
        `dict` or `basestring`


    E.g., Create and use a Connector, configuration read from file named 'db.yaml' and schema ID is 'foo'
    >>> configuration = {
    ... 'foo': {
    ...     'driver': 'pymssql',
    ...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '**', 'port': 3306, 'db':'foo'},
    ...     },
    ... 'bar': {
    ...     'driver': 'pymssql',
    ...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '**', 'port': 3306, 'db':'bar'},
    ...     },
    ... }
    >>> import yaml
    >>> with open('db.yaml', 'w') as fp:
    ...     yaml.dump(configuration, fp)
    ...
    >>> base_setting(ID='foo', file='db.yaml')     # set schema ID/configuration file path
    >>> connector = Connector()
    >>> connector.driver                           # using underlying driver name
    >>> connector._connection                      # associated connection object
    >>> connector._cursor                          # associated cursor object
    >>> connector.connection                       # connection object
    >>> connector.cursor()                         # call cursor factory method to obtains a new cursor object
    >>> from pymysql.cursors import DictCursor
    >>> connector.cursor(cursorclass=DictCursor)   # obtains a new customer cursor object
    >>> connector._cursor.execute('select now();') # execute sql
    >>> connector._cursor.fetchall()               # fetch result
    >>> connector._connection.commit()
    >>> connector.close()

    E.g., Auto close connection/Auto commit.
    >>> with Connector() as cursor:                      # Note: with statement return cursor instead of connector
    >>>    cursor.execute('insert into Point(x, y, x) values (1.0, 2.0, 3.0);')
    >>>
    >>> with Connector(ID='bar') as cursor:              # connect to another ID(schema) 'bar'
    >>>     cursor.execute('insert into Point(x, y, x) values (1.0, 2.0, 3.0);')
    >>>
    >>> with Connector(ID='bar', driver='MySQLdb') as cursor:    # using another driver
    >>>     cursor.execute('insert into Point(x, y, x) values (1.0, 2.0, 3.0);')
    >>> # get a connection object. driver is optional keyword argument.
    >>> connection = Connector.connect(driver='pymssql', host='localhost', user='bob', passwd='**', port=3306, db='foo')
    >>> # get a sqlalchemy engine
    >>> engine = make_sqlalchemy_engine(sqlalchemy_uri='mysql+mysqldb://bob:**@localhost/foo')
    """

    def __init__(self, ID=setting.ID, file=setting.file, driver=None, ):
        if isinstance(file, basestring):
            with open(file) as f:
                yaml_obj = yaml.load(f)
            self.driver = driver or yaml_obj[ID].get('driver') or setting.driver  # driver name
            self.connect_args = yaml_obj[ID]['config']
        elif isinstance(file, dict):
            self.driver = driver or setting.driver
            self.connect_args = file
        else:
            raise TypeError("Unexpected data type in argument file")
        self.writer = None                                                 # dependency delegator for writing database
        self._connection = self.connect(self.driver, **self.connect_args)  # associated connection
        self._cursor = self._connection.cursor()                           # associated cursor

    def __enter__(self):
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """commit if successful otherwise rollback"""
        self.connection.rollback() if exc_type else self.connection.commit()
        self.close()
        return False

    @staticmethod
    def connect(driver=setting.driver, **connect_kwargs):
        driver in globals() or globals().update({driver: __import__(driver)})
        return globals()[driver].connect(**connect_kwargs)

    @property
    def connection(self):
        if hasattr(self._connection, 'open') and not self._connection.open:
            self._connection = self.connect(self.driver, **self.connect_args)
        return self._connection

    def cursor(self, **kwargs):
        """cursor factory method"""
        return self.connection.cursor(**kwargs)

    def close(self):
        self._cursor.close()
        self._connection.close()


class DBManipulator(Connector):
    """
    This class inherits Connector, used for database I/O.
    :param connection: connection object, auto connect if None.

    E.g., read from database 'foo' and write to database 'bar'
    >>> with DBManipulator() as manipulator:
    >>>    # fetch all data immediately if latency is False, and return a table<? extends petl.util.base.Table>
    >>>    petl_table = manipulator.fromdb('select * from Point;', latency=False)
    >>>    petl_table   # shows petl table, see : http://petl.readthedocs.io/en/latest/ for petl detail
    >>>
    >>> manipulator = DBManipulator(ID='bar')
    >>> manipulator.create_table(petl_table, table_name='Point')
    >>>
    >>> # write with header table
    >>> table_header = ['x', 'y', 'z']
    >>> table = [table_header, [1.0, 88, 88], [2.0, 88, 88]]
    >>> manipulator.todb(table, table_name='Point', mode='insert')  # default mode is 'insert'
    >>>
    >>> # write None header table,
    >>> table = [[1.0, 88, 88], [2.0, 88, 88]]
    >>> manipulator.todb(table, table_name='Point', mode='replace', with_header=False)     # default with_header is True
    >>>
    >>> # sliced big table to many sub-table with specified size, 1 sub-table 1 transaction.
    >>> big_table = [[1.0, 88, 88], [2.0, 88, 88] .....]
    >>> manipulator.todb(big_table, table_name='Point', with_header=False, slice_size=128)  # default slice_size is 128
    >>>
    >>> # check executeed sql
    >>> sql = manipulator.writer.make_sql() # return a SQL String or Iterator<SQL String>
    >>> sql if isinstance(sql, basestring) else [s for s in sql] # show sql
    >>> manipulator.close()
    """

    def __init__(self, connection=None, **kwargs):
        if connection is None:
            super(DBManipulator, self).__init__(**kwargs)
        else:
            self._connection = connection

    def __enter__(self):
        """overwrite"""
        return self

    def fromdb(self, select_stmt, latency=False, **petl_kwargs):
        """fetch and wrap all data immediately if latency is False"""
        temp = petl.fromdb(self.connection, select_stmt, **petl_kwargs)
        return temp if (isinstance(latency, bool) and latency) else petl.wrap([row for row in temp])

    def todb(self, table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=()):
        """
        :param table: data container, a `petl.util.base.Table` or a sequence like: [header, row1, row2...].
        :param table_name: the name of a table in this schema.
        :param mode:
            execute SQL INSERT INTO Statement if mode equal to 'insert'.
            execute SQL REPLACE INTO Statement if mode equal to 'replace'.
            execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if mode equal to 'update'.
            execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if mode equal to 'truncate'.
        :param duplicate_key: it must be present if the argument mode is 'update', otherwise it will be ignored.
        :param with_header: specify True(default) if the argument table with header, otherwise specify False.
        :param slice_size: the table will be slice to many subtable with slice_size, 1 transaction for 1 subtable.
        """
        self.make_writer(table=table, table_name=table_name, mode=mode, with_header=with_header, slice_size=slice_size,
                         duplicate_key=duplicate_key)
        return self.writer.write()

    def make_writer(self, **kwargs):
        mode = kwargs.get('mode')
        if mode == 'truncate':
            self.cursor().execute("TRUNCATE TABLE %s" % kwargs['table_name'])
            kwargs.update(mode='insert')
        if mode == 'update':
            self.writer = UpdateDuplicateWriter(self.connection, **kwargs)
        else:
            self.writer = InsertReplaceWriter(self.connection, **kwargs)
        return self.writer

    def create_table(self, table, table_name, **petl_kwargs):
        if self.driver and ('MYSQL' in self.driver.upper()):
            self.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        petl.todb(table, self.connection, table_name, create=True, **petl_kwargs)
        return table.nrows()


class Writer(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection, table, table_name, with_header=True, mode='insert', slice_size=128, duplicate_key=()):
        if mode.upper() not in ('INSERT', 'REPLACE', 'UPDATE'):
            raise ValueError("Invalid mode, it's should be: {'insert' | 'replace' | 'update'}")
        self.connection = connection
        self.table = self.convert2table(table)
        self.table_name = table_name
        if with_header:
            self.header = self.table.header()
            self.content = self.table.skip(1).tuple()
            self.row_count = self.table.nrows()
        else:
            self.header = None
            self.content = self.table.tuple()
            self.row_count = self.table.nrows() + 1
        self.mode = mode
        self.slice_size = slice_size
        self.duplicate_key = duplicate_key

    @staticmethod
    def convert2table(sequence):
        if isinstance(sequence, petl.util.base.Table):
            return sequence
        if isinstance(sequence, (list, tuple)) and isinstance(sequence[0], (list, tuple)):
            return petl.wrap(sequence)
        raise TypeError('Unexpected data type in table, it should be `petl.util.base.Table` or `sequence`')

    @abc.abstractmethod
    def write(self):
        """load content to database"""

    @abc.abstractmethod
    def make_sql(self):
        """:return SQL statement or Iterator<SQL statement>"""


class InsertReplaceWriter(Writer):
    def write(self):
        cursor = self.connection.cursor()
        sql = self.make_sql()
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
            values_fmt = u', '.join(('%s', ) * len(self.header))
            sql = u"%s INTO %s (%s) VALUES (%s)" % (self.mode.upper(), self.table_name, fields, values_fmt)
        else:
            values_fmt = u', '.join(('%s', ) * len(self.content[0]))
            sql = u"%s INTO %s VALUES (%s)" % (self.mode.upper(), self.table_name, values_fmt)
        return sql


class UpdateDuplicateWriter(Writer):
    def make_sql(self):
        sql_statement_fmt = u"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
        for row in self.table.dicts():
            dic = dict((k, v) for k, v in row.items() if v is not None)
            keys = dic.keys()
            keys_sql = ', '.join(keys)
            values_sql = ', '.join(
                map(lambda v: u"{}".format(v) if isinstance(v, numbers.Number) else u"'{}'".format(v), dic.values()))
            update_keys = [k for k in keys if k not in self.duplicate_key]
            update_items_sql = ', '.join(map(
                lambda k: u"{}={}".format(k, dic[k]) if isinstance(dic[k], numbers.Number) else u"{}='{}'".format(k, dic[k]), update_keys))
            yield sql_statement_fmt % (self.table_name, keys_sql, values_sql, update_items_sql)

    def write(self):
        if not self.duplicate_key:
            raise ValueError('argument duplicate_key is not specified')
        cursor = self.connection.cursor()
        affected_row_count = 0
        for i, sql in enumerate(self.make_sql()):
            num = cursor.execute(sql)
            if (i > 0) and (i % self.slice_size == 0):
                self.connection.commit()
            affected_row_count += (num or 0)
        self.connection.commit()
        return affected_row_count


def make_sqlalchemy_engine(sqlalchemy_uri=setting.sqlalchemy_uri, **kwargs):
    return sqlalchemy.create_engine(sqlalchemy_uri, **kwargs)
