from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime
from sqlalchemy.schema import CreateTable

Base = declarative_base()


class TableLoad(Base):
    __tablename__ = 'tableloads'

    name = Column(String, primary_key=True)
    BatchDate = Column(DateTime)
    ValidToDate = Column(DateTime)
    OffsetValidToDate = Column(DateTime)
    ValidFromDate = Column(DateTime)


def get_sqa_tableload(con):
    """
    param con: sqa database engine
    """
    return str(CreateTable(TableLoad.__table__).compile(con))
