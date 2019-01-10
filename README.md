# THE DATALAB COMMAND LINE CLIENT

``datalab`` is command-line Python client for the [NOAO Data Lab](https://datalab.noao.edu).

It provides easy access to Data Lab functionalities:

1. remote storage (VOSpace)
1. (a)synchronous data queries (TAP)
1. job management

Authentication to Data Lab is based on a username and password.


## System requirements

* A Data Lab account
* Python 2.7 or later (Python 3.6 recommended)
* fuse or OSX-FUSE (if you want to mount the remote storage as a local filesystem)


## Installation

The ``noaodatalab`` package installs the ``datalab`` command line
client, and some Data Lab Python libraries that allow you to use Data
Lab functionality locally on your computer (for instance in Ipython
etc.)

### Install via pip

The easiest way to install the ``datalab`` client is via pip:

```
pip install --upgrade noaodatalab
```

### Install from sources

You can also install the ``datalab`` client from source on
[GitHub](https://github.com/noaodatalab/datalab.git):

```
git clone https://github.com/noaodatalab/datalab.git
cd datalab
python setup.py install
```

If you want it installed in your private Python repository (because
you maintain multiple Python instances on your machine) then do:

```
python setup.py install --user
```

Finally, if you intend to mount the virtual storage as a local
filesystem, you will need to touch a file in your home directory:

```
touch ~/.netrc
```
## Documentation

### ``datalab`` command line client

To get a list of available datalab commands (tasks):

```
datalab help

Usage:

    % datalab <task> [task_options]

where <task> is one of:

                 cp - copy a file in Data Lab
             dropdb - Drop a user MyDB table
                get - get a file from Data Lab
             listdb - List the user MyDB tables
                 ln - link a file in Data Lab
              login - Login to the Data Lab
             logout - Logout of the Data Lab
                 ls - list a location in Data Lab
              mkdir - create a directory in Data Lab
                 mv - move a file in Data Lab
          mydb_copy - Rename a user MyDB table
        mydb_create - Create a user MyDB table
          mydb_drop - Drop a user MyDB table
        mydb_import - Import data into a user MyDB table
        mydb_insert - Insert data into a user MyDB table
          mydb_list - List the user MyDB tables
        mydb_rename - Rename a user MyDB table
      mydb_truncate - Truncate a user MyDB table
           profiles - List the available Query Manager profiles
                put - Put a file into Data Lab
           qresults - Get the async query results
            qstatus - Get an async query job status
              query - Query a remote data service in the Data Lab
                 rm - delete a file in Data Lab
              rmdir - delete a directory in Data Lab
             schema - Print data service schema info
             status - Report on the user status
           svc_urls - Print service URLs in use
                tag - tag a file in Data Lab
             whoami - Print the current active user
```

You can get summaries of the arguments to a task with the ``help``
option:

```
datalab login help
The 'login' task takes the following parameters:
  debug - print debug log level messages [optional]
  verbose - print verbose level log messages [optional]
  warning - print warning level log messages [optional]
  user - username of account in Data Lab [required]
  password - password for account in Data Lab [required]
  mount - mountpoint of remove VOSpace [optional]
```

The ``datalab`` command will prompt you for required arguments if you do not
provide them on the command line, e.g.:

```
datalab login
user (default: None):
password (default: None):
```

Documentation for the ``datalab`` commands can be also found in the
[``docs/``](github.com/noaodatalab/datalab/tree/master/docs)
directory:

### ``dl`` Data Lab Python module

Once the client is installed, some Data Lab Python modules can be
imported and used in your Python programs locally, e.g.

```python
ipython
In [1]: from dl import queryClient as qc
In [2]: result = qc.query(sql='SELECT ra,dec from smash_dr1.object LIMIT 10')
In [3]: print(result)
ra,dec
175.215070742307,-38.4897863179213
175.241595469141,-38.4163769993698
175.25128999751,-38.4393292753547
175.265049366394,-38.424371697545
175.265160854504,-38.4915114547051
175.277267094536,-38.431267581266
175.302055158646,-38.4674421358985
175.328056295831,-38.4350989294865
175.334968899953,-38.4547709884234
175.34222308206,-38.4433633662239
```

A very comprehensive [user
manual](https://datalab.noao.edu/docs/manual/) explains the many
features of Data Lab.
  
### To mount virtual storage as a local directory at login

You can mount the virtual storage as a local directory at login by
using the optional <i>mount</i> argument. 

```
datalab login --user=<user> --password=<password> --mount=/tmp/vospace
```

This will attempt to mount the default virtual storage (at NOAO). If
you need to mount another one, you should use the ``datalab mount`` option.

### To mount virtual storage as a local directory once logged in

```
datalab mount
vospace (default: vos:):
mount (default: /tmp/vospace):
```

### To unmount virtual storage

You can either use a regular Unix command:

```
umount /tmp/vospace
```

or unmount the space when you log out of Data Lab:

```
datalab logout --unmount=/tmp/vospace
```
