from threading import Lock
from logs.logger import Logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class ClickHouseDB:
    _instance = None
    _lock = Lock()

    def __new__(cls, connection_string: str):
        logger = Logger('db.log')

        with cls._lock:
            if cls._instance is None:
                try:
                    cls._instance = create_engine(connection_string)
                except SQLAlchemyError as e:
                    logger.log(f"Cannot create ClickHouse engine: {e}", level="ERROR")
                    raise
            return cls._instance