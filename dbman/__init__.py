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
    db_label = None
    db_config = None
    driver = 'pymysql'


def base_setting(db_config, db_label=None, driver=None, ):
    """
    Does basic configuration for this module.

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
    >>> dbman.base_setting(db_config='dbconfig.yaml', db_label='foo')
    """
    setting.db_config = db_config
    setting.db_label = db_label
    setting.driver = driver or setting.driver


class Connector(object):
    """ This class obtains and maintains a connection to a schema"""

    def __init__(self, db_config=None, db_label=None, driver=None, ):
        """   
        :param db_config:
            a yaml filename or a dictionary object, `setting.db_config` will be used if it's omitted.
            if the argument `db_config` is a yaml filename, loading the content as configuration.
            the dictionary or yaml content, which will either passed to the underlying DBAPI
            ``connect()`` method as additional keyword arguments.
        :type db_config: `dict` or `basestring`
        :param db_label: a string represents a schema, `setting.db_label` will be used if it's omitted.
        :param driver: package name of underlying database driver that clients want to use, `pymysql` will be assumed if it's omitted.
        :type driver: str` = {'pymysql' | 'MySQLdb' | 'pymssql'}
        """
        db_config = db_config or setting.db_config
        db_label = db_label or setting.db_label
        if isinstance(db_config, basestring):
            with open(db_config) as f:
                yaml_obj = yaml.load(f)
            self.driver = yaml_obj[db_label].get('driver') or driver or setting.driver  # driver name
            self.connect_args = yaml_obj[db_label]['config']
        elif isinstance(db_config, dict):
            self.driver = driver or setting.driver
            self.connect_args = db_config
        else:
            raise TypeError("Unexpected data type in argument 'db_config'")
        self.writer = None                                                 # dependency delegator for writing database
        self._connection = self.connect(self.driver, **self.connect_args)  # associated connection
        self._cursor = self._connection.cursor()                           # associated cursor

    def __enter__(self):
        """with statement return cursor instead of connector"""
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """commit if successful otherwise rollback"""
        self.connection.rollback() if exc_type else self.connection.commit()
        self.close()
        return False

    @staticmethod
    def connect(driver=None, **connect_kwargs):
        driver = driver or setting.driver
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
    """This class inherits `dbman.Connector and add 2 methods: `fromdb` for read and `todb` for write"""

    def __init__(self, connection=None, driver=None, **kwargs):
        """    
        :param connection: a connection object.
        :param driver: package name of underlying database driver that users want to use, `pymysql` will be assumed if it's omitted.
        :param kwargs: if `connection` is `None`, `kwargs` will be passed to `dbman.Connector`` to obtains a connection, 
            otherwise it will be ignored.
        """
        if connection is None:
            super(Manipulator, self).__init__(driver=driver, **kwargs)
        else:
            self.driver = driver or setting.driver
            self._connection = connection

    def __enter__(self):
        """overwrite"""
        return self

    def fromdb(self, select_stmt, args=None, latency=False):
        """argument `select_stmt` and `args` will be passed to the underlying API `cursor.execute()`.
        fetch and wraps all data immediately if the optional keyword argument `latency` is `True`
        """
        temp = petl.fromdb(self.connection, select_stmt, args)
        return temp if latency else petl.wrap([row for row in temp])

    def todb(self, table, table_name, mode='insert', with_header=True, slice_size=128, duplicate_key=()):
        """
        :param table: data container, a `petl.util.base.Table` or a sequence like: [header, row1, row2, ...] or [row1, row2, ...]
        :param table_name: the name of a table in this database
        :param mode:
            execute SQL INSERT INTO Statement if `mode` equal to 'insert'.
            execute SQL REPLACE INTO Statement if `mode` equal to 'replace'.
            execute SQL INSERT ... ON DUPLICATE KEY UPDATE Statement if `mode` equal to 'update'.
            execute SQL TRUNCATE TABLE Statement and then execute SQL INSERT INTO Statement if `mode` equal to 'truncate'.
            create a table and insert data into it if `mode` equal to 'create'.
        :param duplicate_key: it must be present if the argument `mode` is 'update', otherwise it will be ignored.
        :param with_header: specify `True` if the argument `table` with header, otherwise specify `False`.
        :param slice_size: the `table` will be slice to many subtable with `slice_size`, 1 transaction for 1 subtable.
        """
        if mode == 'create':
            return self._create_table(table, table_name=table_name)
        else:
            self._make_writer(table=table, table_name=table_name, mode=mode, with_header=with_header, slice_size=slice_size, 
                    duplicate_key=duplicate_key)
            return self.writer.write()

    def _make_writer(self, **kwargs):
        kwargs.update(connection=self.connection)
        mode = kwargs.get('mode')
        if mode == 'truncate':
            self.cursor().execute("TRUNCATE TABLE `%(table_name)s`;", kwargs)
            kwargs.update(mode='insert')
        if (mode == 'update') and self.driver and ('MYSQL' in self.driver.upper()):
            self.writer = _UpdateDuplicateWriter(**kwargs)
        elif mode in ('insert', 'replace'):
            self.writer = _InsertReplaceWriter(**kwargs)
        else:
            raise RuntimeError("The driver '%s' can't handle this request" % self.driver)
        return self.writer

    def _create_table(self, table, table_name, **petl_kwargs):
        if self.driver and ('MYSQL' in self.driver.upper()):
            self.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        petl.todb(table, self.connection, table_name, create=True, **petl_kwargs)


class _Writer(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection, table_name, with_header, mode, slice_size, duplicate_key):
        self.connection = connection
        self.table_name = table_name
        self.with_header = with_header
        self.mode = mode
        self.duplicate_key = duplicate_key
        self.slice_size = slice_size

    @abc.abstractmethod
    def make_sql(self):
        """:return Iterator<SQL statement>"""
        
    @abc.abstractmethod
    def write(self):
        """load data to database"""


class _InsertReplaceWriter(_Writer):
    def __init__(self, table, **kwargs):
        super(_InsertReplaceWriter, self).__init__(**kwargs)
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
            values_fmt = u', '.join(('%s', ) * len(self.header))
            sql = u"%s INTO %s (%s) VALUES (%s)" % (self.mode.upper(), self.table_name, fields, values_fmt)
        else:
            values_fmt = u', '.join(('%s', ) * len(self.content[0]))
            sql = u"%s INTO %s VALUES (%s)" % (self.mode.upper(), self.table_name, values_fmt)
        yield sql


class _UpdateDuplicateWriter(_Writer):
    def __init__(self, table, **kwargs):
        super(_UpdateDuplicateWriter, self).__init__(**kwargs)
        if not self.duplicate_key or not self.with_header:
            raise ValueError('Argument duplicate_key is not specified or argument table with not header')
        self.content = table if isinstance(table, petl.util.base.Table) else petl.wrap(table)

    def make_sql(self):
        sql_statement_fmt = u"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
        for row in self.content.dicts():
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
        cursor = self.connection.cursor()
        affected_row_count = 0
        for i, sql in enumerate(self.make_sql()):
            num = cursor.execute(sql)
            if (i > 0) and (i % self.slice_size == 0):
                self.connection.commit()
            affected_row_count += (num or 0)
        self.connection.commit()
        return affected_row_count
