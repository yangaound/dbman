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


class setting:
    ID = None
    file = None
    driver = None


def base_setting(file, ID=None, driver=None, ):
    """
    Does basic configuration for this module object.

    E.g.,
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
    >>> dbman.base_setting(file='dbconfig.yaml', )
    """
    setting.file = file
    setting.ID = ID or setting.ID
    setting.driver = driver or setting.driver


class Connector(object):
    """
    This class obtains and maintains a connection to a database scheme.

    :param file:
        a yaml filename or a dictionary object, `setting.filename` will be used if it's omitted.
        if the argument file is a yaml filename, loading the content as configuration.
        the dictionary or yaml content, which will either passed directly to the underlying DBAPI
        ``connect()`` method as additional keyword arguments.
    :type file: `dict` or `basestring`
    :param ID: a string represents a database schema, `setting.ID` will be used if it's omitted.
    :param driver: package name of underlying database driver that users want to use.
    :type driver: str` = {'pymysql' | 'MySQLdb' | 'pymssql'}
    """
    def __init__(self, file=setting.file, ID=setting.ID, driver=None, ):
        if isinstance(file, basestring):
            with open(file) as f:
                yaml_obj = yaml.load(f)
            self.driver = driver or yaml_obj[ID].get('driver') or setting.driver  # driver name
            self.connect_args = yaml_obj[ID]['config']
        elif isinstance(file, dict):
            self.driver = driver or setting.driver
            self.connect_args = file
        else:
            raise TypeError("Unexpected data type in argument 'file'")
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


class Manipulator(Connector):
    """
    This class used for database I/O.
    :param connection: a connection object.
    :param kwargs: if connection is None, kwargs will be passed to  ``dbman.Connector`` to obtains a connection, otherwise ignores it.
    """

    def __init__(self, connection=None, **kwargs):
        if connection is None:
            super(Manipulator, self).__init__(**kwargs)
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
        :param table_name: the name of a table in this ID.
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
        if (mode == 'update') and ('MYSQL' in self.driver.upper()):
            self.writer = UpdateDuplicateWriter(self.connection, **kwargs)
        elif mode in ('insert', 'replace'):
            self.writer = InsertReplaceWriter(self.connection, **kwargs)
        else:
            raise RuntimeError("The driver '%s' can't handle this request" % self.driver)
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
            values_sql = ', '.join(map(
                lambda v: u"{}".format(v) if isinstance(v, numbers.Number) else u"'{}'".format(v), dic.values()))
            update_keys = [k for k in keys if k not in self.duplicate_key]
            update_items_sql = ', '.join(map(
                lambda k: u"{}={}".format(k, dic[k]) if isinstance(dic[k], numbers.Number) else u"{}='{}'".format(k, dic[k]), update_keys))
            yield sql_statement_fmt % (self.table_name, keys_sql, values_sql, update_items_sql)

    def write(self):
        if not self.duplicate_key:
            raise ValueError('Argument duplicate_key is not specified')
        cursor = self.connection.cursor()
        affected_row_count = 0
        for i, sql in enumerate(self.make_sql()):
            num = cursor.execute(sql)
            if (i > 0) and (i % self.slice_size == 0):
                self.connection.commit()
            affected_row_count += (num or 0)
        self.connection.commit()
        return affected_row_count

