import unittest
import os
from sqlalchemy import MetaData, Column, Integer, DateTime
from sqlalchemy import Table, create_engine
from sqlalchemy.ext.declarative import declarative_base

from ninjasql.db.sqa_dml_extractor import SqaExtractor
from tests import db

DBPATH = os.path.dirname(db.__file__)


def get_engine():
    """
    return a database connection
    """
    dbname = "ninjasql_test.db"
    url = os.path.join(DBPATH, dbname)
    engine = create_engine('sqlite:///' + url, echo=True)
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return engine


class SqaExtractorTest(unittest.TestCase):

    def setUp(self):
        engine = get_engine()
        metadata = MetaData(engine)
        self.staging_table = Table(
            'stg_table1',
            metadata,
            Column('id', Integer),
            Column('number', Integer))

        self.history_table = Table(
            'his_table1',
            metadata,
            Column('id', Integer),
            Column('number', Integer),
            Column('ROW', Integer),
            Column('UPDATED_AT', DateTime),
            Column('VALID_FROM_DATE', DateTime),
            Column('VALID_TO_DATE', DateTime))

    def _columns(self):
        return ['id', 'number']

    def _table_name(self):
        return "stg_table1"

    def test_if_error_on_wrong_class(self):
        """
        test if class only except sqa staging_table object. Raise error if
        not right class
        """
        self.assertIsNotNone(SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()))
        with self.assertRaises(TypeError):
            SqaExtractor(staging_table="ABS")

    def test_get_col_names(self):
        """
        test if all column can be extracted
        """
        col = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()).get_col_names()
        self.assertEqual(col, self._columns())

    def test_get_source_col_names(self):
        """
        test if all source columns can be extracted
        """
        col = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()).get_source_col_names()
        self.assertEqual(col, self._columns())

    def test_get_table_name(self):
        """
        test if staging_table name can be extracted
        """
        tab = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()).get_table_name()
        self.assertEqual(tab, self._table_name())

    def test_get_sql_scd2_new_insert_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        ins = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()).scd2_new_insert()

        self.assertIsInstance(ins, str)
        self.assertIsNotNone(ins)

    def test_get_sql_scd2_updated_ins_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        ins = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            con=get_engine()).scd2_updated_insert()

        self.assertIsInstance(ins, str)
        self.assertIsNotNone(ins)
        self.assertEqual(ins, "INSERT")
