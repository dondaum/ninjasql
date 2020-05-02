import unittest
from sqlalchemy import MetaData, Column, Integer, DateTime
from sqlalchemy import Table
from sqlalchemy.sql.selectable import ScalarSelect
from sqlalchemy.sql.elements import BinaryExpression

from ninjasql.db.sqa_dml_extractor import SqaExtractor
from tests.db.db_helper import get_engine


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
            Column('BATCH_RUN_AT', DateTime),
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
            con=get_engine(),
            load_strategy='database_table'))
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
            load_strategy='database_table',
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
            load_strategy='database_table',
            con=get_engine()).get_source_col_names()
        self.assertEqual(col, self._columns())

    def test_get_staging_col(self):
        """
        test if all stating colums are extracted as sqa objects
        """

        cols = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).get_staging_columns()
        for col in cols:
            self.assertIn(col.name, self._columns())
            self.assertIsInstance(col, Column)

    def test_set_metadata_columns(self):
        """
        test if all metadata colums get be extracted as a list
        """

        cols = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).set_metadata_colums()

        for col in cols:
            self.assertIsInstance(col, ScalarSelect)

    def test_equal_pk_columns(self):
        """
        test if filter or join columns get be extracted
        """

        cols = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).def_equal_pk_col()
        for col in cols:
            self.assertIn(col._orig[0].name, self._columns())
            self.assertIsInstance(col, BinaryExpression)

    def test_compare_columns(self):
        """
        test if compare columns can be extracted
        """
        cols = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).get_compare_columns()
        for col in cols:
            self.assertIn(col._orig[0].name, self._columns())
            self.assertIsInstance(col, BinaryExpression)

    def test_get_staging_table_pk_columns(self):
        """
        test if pk columns from staging table can be extracted
        """
        cols = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).get_staging_table_pk_col()
        for col in cols:
            self.assertIn(col.name, self._columns())
            self.assertIsInstance(col, Column)

    def test_get_table_name(self):
        """
        test if staging_table name can be extracted
        """
        tab = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).get_table_name()
        self.assertEqual(tab, self._table_name())

    def test_get_sql_scd2_new_insert_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        for mode in ['database_table', 'jinja']:
            ins = SqaExtractor(
                staging_table=self.staging_table,
                history_table=self.history_table,
                logical_pk=["id", "number"],
                load_strategy=mode,
                con=get_engine()).scd2_new_insert()

            self.assertIsInstance(ins, str)
            self.assertIsNotNone(ins)

    def test_get_sql_scd2_updated_ins_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        for mode in ['database_table', 'jinja']:
            ins = SqaExtractor(
                staging_table=self.staging_table,
                history_table=self.history_table,
                logical_pk=["id", "number"],
                load_strategy=mode,
                con=get_engine()).scd2_updated_insert()

            self.assertIsInstance(ins, str)
            self.assertIsNotNone(ins)
        # self.assertEqual(ins, "INSERT")

    def test_get_sql_scd2_updated_update_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        for mode in ['database_table', 'jinja']:
            upd = SqaExtractor(
                staging_table=self.staging_table,
                history_table=self.history_table,
                logical_pk=["id", "number"],
                load_strategy=mode,
                con=get_engine()).scd2_updated_update()

            self.assertIsInstance(upd, str)
            self.assertIsNotNone(upd)
        # self.assertEqual(upd, "UPDATE")

    def test_get_sql_scd2_deleted_update_cms(self):
        """
        test if sql scd2 insert command can be generated
        """
        for mode in ['database_table', 'jinja']:
            upd = SqaExtractor(
                staging_table=self.staging_table,
                history_table=self.history_table,
                logical_pk=["id", "number"],
                load_strategy=mode,
                con=get_engine()).scd2_deleted_update()

            self.assertIsInstance(upd, str)
            self.assertIsNotNone(upd)
        # self.assertEqual(upd, "UPDATE")

    def test_get_tableload_insert(self):
        """
        test if table load insert statement can be extracted
        """
        ins = SqaExtractor(
            staging_table=self.staging_table,
            history_table=self.history_table,
            logical_pk=["id", "number"],
            load_strategy='database_table',
            con=get_engine()).get_tableload_insert()

        self.assertIsInstance(ins, str)
        self.assertIsNotNone(ins)
