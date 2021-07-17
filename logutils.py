import datetime
import io
import logging
import pathlib
import sys
from typing import Dict, Optional, Union

__all__ = [
    "DatabaseHandler",
    "make_log_table_definition",
    "setup_database_logger",
    "setup_file_logger",
    "setup_stream_logger",
]

LOG_TABLE_DEFINITION = """CREATE TABLE {table_name} (
      log_id      INT           NOT NULL IDENTITY
    , date        DATETIME      NOT NULL
    , logger      VARCHAR(100)      NULL
    , module      VARCHAR(100)  NOT NULL
    , func_name   VARCHAR(100)  NOT NULL
    , line        INT               NULL
    , level       INT           NOT NULL
    , level_name  VARCHAR(100)  NOT NULL
    , message     VARCHAR(400)      NULL
    , traceback   VARCHAR(4000)     NULL

    , CONSTRAINT {primary_key} PRIMARY KEY (log_id)
);"""

LOG_TABLE_MAP = {
    "date": "created",
    "logger": "name",
    "module": "module",
    "func_name": "funcName",
    "line": "lineno",
    "level": "levelno",
    "level_name": "levelname",
    "message": "message",
    "traceback": "exc_info",
}


class DatabaseHandler(logging.Handler):
    """A logging library handler subclass that writes logging records to a
    database table."""

    record = logging.makeLogRecord({})
    logging.Formatter().format(record)
    default_mapping: Dict[str, str] = {k.lower(): k for k in record.__dict__}
    del record

    insert_query = "INSERT INTO {table} ({cols}) VALUES ({values});"

    def __init__(
        self, connection, table: str, mapping: Optional[Dict[str, str]] = None
    ):
        """Initialize handler.

        :param connection: A DB API 2.0 compliant Connection object
        :param table: Table name
        :param mapping: Table column names and LogRecord object attributes
                        mapping (default: LOG_TABLE_MAP)
        """
        super().__init__()

        self.connection = connection
        self.table = table
        self.mapping = mapping or LOG_TABLE_MAP

        diff = set(self.mapping.values()) - set(self.default_mapping.values())
        if diff:
            raise AttributeError(
                "'%s' object has no attribute%s %s"
                % (
                    logging.LogRecord.__name__,
                    "s" if len(diff) > 1 else "",
                    ", ".join("'%s'" % x for x in sorted(diff)),
                )
            )

        self.cursor = self.connection.cursor()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.format(record)

            query = self.insert_query.format(
                table=self.table,
                cols=", ".join(self.mapping),
                values=", ".join("?" for _ in range(len(self.mapping))),
            )

            params = []
            param_type = Union[str, int, float, None, datetime.datetime]
            for attr in self.mapping.values():
                value = getattr(record, attr)
                if attr == "created":
                    param: param_type = datetime.datetime.fromtimestamp(value)
                elif attr in ("exc_info", "exc_text"):
                    if record.exc_info and any(record.exc_info):
                        param = "|".join(
                            logging.Formatter()
                            .formatException(record.exc_info)
                            .splitlines()
                        )
                    else:
                        param = None
                elif isinstance(value, str) and value.strip() == "":
                    param = None
                elif isinstance(
                    value, (str, int, float, type(None), datetime.datetime)
                ):
                    param = value
                else:
                    param = str(value)
                params.append(param)

            self.cursor.execute(query, params)
            self.cursor.commit()
        except Exception:
            import traceback

            traceback.print_exc(file=sys.stderr)

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()
        super().close()


def make_log_table_definition(table_name: str, primary_key: str) -> str:
    """Return default log table definition query.

    :param table_name: Table name
    :param primary_key: Primary key name
    """
    return LOG_TABLE_DEFINITION.format(
        table_name=table_name, primary_key=primary_key
    )


def setup_database_logger(
    connection,
    table: str,
    name: Optional[str] = None,
    attrs: Optional[Dict[str, str]] = None,
    level_logger: int = logging.DEBUG,
    level_handler: int = logging.DEBUG,
) -> None:
    """Attach logger handler that writes to a database table with level
    `level_handler` and set or update logging level `level_logger` for logger
    `name`.

    :param connection: A DB API 2.0 compliant Connection object
    :param table: Table name
    :param name: Logger name (default: root)
    :param attrs: Table column names and log record attributes mapping
                  (default: LOG_TABLE_MAP)
    :param level_logger: Logging level of the logger
    :param level_handler: Logging level of the database handler
    """
    log = logging.getLogger(name)

    if level_logger is not None:
        log.setLevel(level_logger)

    handler = DatabaseHandler(
        connection=connection,
        table=table,
        mapping=attrs,
    )
    handler.setLevel(level_handler)

    log.addHandler(handler)


def setup_file_logger(
    filename: Union[str, pathlib.Path],
    name: Optional[str] = None,
    mode: str = "a",
    encoding: Optional[str] = None,
    level_logger: int = logging.DEBUG,
    level_handler: int = logging.DEBUG,
    fmt: str = "%(asctime)s %(levelname)-8s %(message)s",
) -> None:
    """Attach logger handler that writes to a file with level `level_handler`
    and set or update logging level `level_logger` for logger `name`.

    :param filename: File name
    :param name: Logger name (default: root)
    :param mode: File mode
    :param encoding: File encoding
    :param level_logger: Logging level of the logger
    :param level_handler: Logging level of the file handler
    :param fmt: Format string for the file handler
    """
    log = logging.getLogger(name)

    if level_logger is not None:
        log.setLevel(level_logger)

    for h in log.handlers:
        if isinstance(h, logging.FileHandler) and h.baseFilename == filename:
            return

    handler = logging.FileHandler(
        filename=filename, mode=mode, encoding=encoding
    )
    handler.setLevel(level_handler)

    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    log.addHandler(handler)


def setup_stream_logger(
    name: Optional[str] = None,
    stream: Optional[io.TextIOWrapper] = None,
    level_logger: int = logging.DEBUG,
    level_handler: int = logging.DEBUG,
    fmt: str = "%(asctime)s %(levelname)-8s %(message)s",
) -> None:
    """Attach logger handler that writes to a stream with level `level_handler`
    and set or update logging level `level_logger` for logger `name`.

    :param name: Logger name (default: root)
    :param stream: Stream object (default: sys.stderr)
    :param level_logger: Logging level of the logger
    :param level_handler: Logging level of the stream handler
    :param fmt: Format string for the stream handler
    """
    log = logging.getLogger(name)

    if level_logger is not None:
        log.setLevel(level_logger)

    for h in log.handlers:
        if isinstance(h, logging.StreamHandler):
            return

    handler = logging.StreamHandler(stream=stream)
    handler.setLevel(level_handler)

    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)

    log.addHandler(handler)
