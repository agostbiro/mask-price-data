from functools import partial
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
from typing import Type

from sqlalchemy import (
    create_engine,
    event,
    Column,
    Float,
    Integer,
    String,
    DateTime,
    Boolean,
    ForeignKey,
)
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.session import Session
from sqlalchemy_repr import RepresentableBase


from mp_data import repo_root


_DB_FILE_PATH = repo_root / Path("./data/db/mask_price_data.sqlite")

_engine = None

_Base = declarative_base(cls=RepresentableBase)
_NonNullCol: Type[Column] = partial(Column, nullable=False)


# Enforce foreign key constraint in SQlite
# From https://stackoverflow.com/a/15542046/2650622
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, _):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(f"sqlite:///{_DB_FILE_PATH}")
        _Base.metadata.create_all(_engine)
    return _engine


def get_session():
    return Session(bind=get_engine())


class HIT(_Base):
    """Human Intelligence Task in MTurk"""

    __tablename__ = "hits"

    id = Column(Integer, primary_key=True)
    # Human Intelligence Task ID in MTurk
    hit_id = _NonNullCol(String, index=True, unique=True)
    # The UTC time the task was created
    creation_time = _NonNullCol(DateTime)
    # Identifier of a group of tasks create at the same time
    batch_name = _NonNullCol(String)
    # The url of the product
    url_param = _NonNullCol(String)
    # The domain name of the marketplace
    domain_name = _NonNullCol(String, index=True)


class Assignment(_Base):
    """Worker assignment for a HIT"""

    __tablename__ = "assignments"

    id = Column(Integer, primary_key=True)
    hit_id = _NonNullCol(String, ForeignKey("hits.hit_id"))
    # The assignment ID in MTurk
    assignment_id = _NonNullCol(String, index=True, unique=True)
    # The time the worker started the task
    accept_time = _NonNullCol(DateTime)
    # The time the worker finished the task
    submit_time = _NonNullCol(DateTime)
    # Whether the product is available for sale in the marketplace
    in_stock = _NonNullCol(Boolean)
    # The price of the product in cents
    price = Column(Float)
    # The currency symbol
    currency = Column(String)
    # The quantity of masks in the package
    quantity = Column(Integer)

    hit = relationship("HIT", back_populates="assignments")


HIT.assignments = relationship(
    "Assignment", order_by=Assignment.id, back_populates="hit"
)
