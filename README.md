


### ``dbman.base_setting``(file, ID, driver=None):
	Does basic configuration for this module. 
    

### class ``dbman.Connector``([file, [ID, [driver]]] ):
    This class obtains and maintains a connection to a database scheme.<br />
	Keyword argument `file` should be a dictionary object or a yaml filename, `basic configuration's file` will be used if it's <br />
	omitted. If the `file` is a yaml filename, loading the content as configuration of a instance. The dictionary or yaml content,<br />
	which will either passed directly to the underlying DBAPI ``connect()`` method as additional keyword arguments, so the <br />
	dictionary's key should be follow underlying API.<br />
    Keyword argument `ID` is a string represents a database schema, `basic configuration's ID` will be used if it's omitted.<br />
    Keyword argument `driver` is a package name of underlying database driver that users want to use. E.g:<br /> 
	`driver` = {'pymysql' | 'MySQLdb' | 'pymssql'}. `basic configuration's driver` will be used if it's omitted.<br />
	
```
>>>  # Creates configuration file
>>> configuration = {
... 'foo': {
...     'driver': 'pymssql',
...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'foo'},
...     },
... 'bar': {
...     'driver': 'pymssql',
...     'config': {'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'bar'},
...     },
... }
>>> import yaml
>>> with open('dbconfig.yaml', 'w') as fp:
...     yaml.dump(configuration, fp)
...
>>> import dbman
>>> dbman.base_setting(file='dbconfig.yaml', ID='foo', driver='pymssql') # does basic configuration
>>> connector = dbman.Connector()              # instantialize Connector with basic configuration
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
>>> # file is a dict
>>> dbman.Connector(file={'host': 'localhost', 'user': 'bob', 'passwd': '****', 'port': 3306, 'db':'foo'}) 
>>> # new a connection, driver argument is optional
>>> dbman.Connector.connect(driver='pymysql', host='localhost', user='bob', passwd='****', port=3306, db='foo') 
```
	
