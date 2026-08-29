"""Opening a MySQL connection, with a short retry so containers can start in any order."""

import time

import pymysql
from pymysql.cursors import DictCursor

from moderation.config import settings


def connect(retries: int = 10, delay: float = 2.0) -> pymysql.connections.Connection:
    """Connect to MySQL, waiting a little while for it to come up."""
    last_error = None
    for _ in range(retries):
        try:
            return pymysql.connect(
                host=settings.mysql_host,
                port=settings.mysql_port,
                user=settings.mysql_user,
                password=settings.mysql_password,
                database=settings.mysql_database,
                cursorclass=DictCursor,
                autocommit=True,
            )
        except pymysql.err.OperationalError as exc:
            last_error = exc
            time.sleep(delay)
    raise last_error
