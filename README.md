# python-logging-utils

This repository contains `logutils` Python module that includes several utilities for logging.

- `DatabaseHandler`
<br>_A standard logging library handler subclass that writes to a database table._

- `make_log_table_definition`
<br>_A function that returns a default log table definition query._

- `setup_database_logger`
<br>_A function that helps setup database logger._

- `setup_file_logger`
<br>_A function that helps setup file logger._

- `setup_stream_logger`
<br>_A function that helps setup stream logger._

### Usage

All examples use `pyodbc` module to establish ODBC connection and execute queries, but any other DB API 2.0 compatible module should also work.

- To create a table in a database to store logs:

```python
import pyodbc
from logutils import make_log_table_definition

query = make_log_table_definition("table_name", "primary_key_name")

conn = pyodbc.connect("connection_string")
cur = conn.cursor()
cur.execute(query).commit()
```

- To quickly setup a logger that writes to an existing log table:

```python
import pyodbc
from logutils import setup_database_logger

conn = pyodbc.connect("connection_string")
setup_database_logger(conn, "table_name")  # use root logger if name is not provided
```
- To create and attach a logger handler that writes to an existing log table:

```python
import logging
import pyodbc
from logutils import DatabaseHandler

conn = pyodbc.connect("connection_string")

handler = DatabaseHandler(conn, "table_name")
handler.setLevel(logging.INFO)

logger = logging.getLogger()  # obtain a root logger
logger.setLevel(logging.INFO)
logger.addHandler(handler)

logger.info("System Event")
```
