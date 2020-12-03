'''
define table structure in database
'''
from flask_sqlalchemy import SQLAlchemy

from .server_logger import logger

db = SQLAlchemy()


class Base(db.Model):
    __abstract__ = True
    row = 0
    timestamp = db.Column(db.BIGINT, nullable=False, primary_key=True)
    value = db.Column(db.Float, nullable=False)
    max_rows = 100000
    max_flush_cache = 1000

    @classmethod
    def limit_max_rows(cls):
        db.session.execute(db.text(
            "delete from {table} where timestamp in (select timestamp from {table} order by timestamp desc limit -1 "
            "offset {max_rows})".format(table=cls.__tablename__, max_rows=cls.max_rows)
        ))
        logger.info('remove surplus rows in table [{table}]'.format(table=cls.__tablename__))

    @classmethod
    def on_insert(cls, mapper, connection, target):
        if cls.rows % cls.max_flush_cache == 0:
            cls.limit_max_rows()
            cls.rows += 1
        else:
            cls.rows += 1
