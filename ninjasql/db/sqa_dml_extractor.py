from sqlalchemy.sql.schema import Table
from sqlalchemy.sql import exists, and_, select, func, or_
from sqlalchemy.sql.functions import now
from sqlalchemy.sql.expression import cast, literal_column, literal
from sqlalchemy import DateTime, table, column
from datetime import datetime


class SqaExtractor(object):
    """
    :param staging_table: Sqa Table class object
    :param history_table: Sqa Table class object
    :param con: Sqlalchemy database connection
    """

    def __init__(self,
                 staging_table: Table,
                 history_table: Table,
                 logical_pk: list,
                 con
                 ):
        self._staging_table = staging_table
        self._history_table = history_table
        self._logical_pk = logical_pk
        self._con = con

        if not isinstance(self._staging_table, Table):
            raise TypeError("Must be a SQA Table class instance")

    def get_col_names(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.name for c in self._staging_table.c]

    def get_his_col_names(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.name for c in self._history_table.c]

    def get_source_col_names(self):
        """
        method returns all non technical or business related column names
        """
        return [i for i, j in zip(
            self.get_his_col_names(),
            self.get_col_names()) if i == j]

    def get_table_name(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.table.name for c in self._staging_table.c][0]

    def scd2_new_insert(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        # https://social.msdn.microsoft.com/Forums/SECURITY/en-US/d3ed03bb-d1db-4559-b35c-a0635ae639ab/alternate-queries-for-merge?forum=transactsql
        # https://kite.com/python/docs/sqlalchemy.sql.Insert
        # https://stackoverflow.com/questions/1849375/how-do-i-insert-into-t1-select-from-t2-in-sqlalchemy
        # https://stackoverflow.com/questions/18501347/postgresql-insert-into-where-not-exists-using-sqlalchemys-insert-from-select
        """
        # sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
        all_his_columns = self.get_his_col_names()
        # add INSERT from staging
        all_stg_columns = [getattr(self._staging_table.c, c) for
                           c in self.get_col_names()]
        # func.date(p_now)
        p_now = datetime.now()
        metadata = select(
            [literal_column("1").label("ROW"),
             now().label("UPDATED_AD"),
             now().label("VALID_FROM_DATE"),
             func.date(literal_column("'9999-12-31'")).label("VALID_TO_DATE")
             ])
        # dw_from_dt = select([cast(p_now, DateTime)])
        all_stg_columns.append(metadata)
        # all_stg_columns.append(dw_from_dt)

        pk_columns = self._logical_pk
        exist_stat = [getattr(self._staging_table.c, pk) for pk in pk_columns]
        # todo: Check datatype if ifnull -1 or ''
        filters = []
        for pk_col in self._logical_pk:
            filters.append(
                getattr(self._staging_table.c, pk_col) ==
                getattr(self._history_table.c, pk_col)
            )
        stmt = (self._history_table.insert().
                from_select(all_his_columns,
                select(all_stg_columns).where(
                    ~exists(exist_stat).where(and_(
                        *filters)))
                        ))
        # select().where(t2.c.y == 5)))
        # where(self._staging_table.c.id == 5).
        # values(number=20))
        return str(stmt.compile(self._con))

    def scd2_updated_insert(self):
        """
        scd2 command for all records where an update is needed
        """
        # https://stackoverflow.com/questions/23030321/sqlalchemy-update-from-select
        bus_cols = []
        for col in self.get_source_col_names():
            bus_cols.append(
                getattr(self._staging_table.c, col) ==
                getattr(self._history_table.c, col)
            )
        compare_columns = []
        for col in self.get_source_col_names():
            compare_columns.append(
                getattr(self._staging_table.c, col) !=
                getattr(self._history_table.c, col)
            )

        all_stg_columns = [getattr(self._staging_table.c, c) for
                           c in self.get_col_names()]
  
        p_now = datetime.now()

        # get join columns
        pk_cols = []
        for pk_col in self._logical_pk:
            pk_cols.append(
                getattr(self._staging_table.c, pk_col) ==
                getattr(self._history_table.c, pk_col)
            )

        metadata_col = []
        metadata_col.append(
            select([literal_column("1").label("ROW")]).as_scalar()
        )
        metadata_col.append(
            select([now().label("UPDATED_AD")]).as_scalar()
        )
        metadata_col.append(
           select([now().label("VALID_FROM_DATE")]).as_scalar()
        )
        metadata_col.append(
            select([func.date(
                literal_column("'9999-12-31'")).label("VALID_TO_DATE")]).as_scalar()
        )
        # dw_from_dt = select([cast(p_now, DateTime)])
        all_stg_columns.extend(metadata_col)

        """
        """
        table1 = table('t1', column('a'), column('b'))
        table2 = table('t2', column('a'), column('b'))
        s1 = select([table1.c.a, table2.c.b]).\
                    select_from(table1.join(table2,
                                table1.c.a==table2.c.a))
        """
        """

        join = self._history_table.join(self._staging_table,
                                        and_(*pk_cols))
        sel = (select(all_stg_columns).
               select_from(self._history_table.join(
                   self._staging_table,
                   and_(*pk_cols))).where(
                       or_(*compare_columns)))

        se = select([self._history_table]).select_from(
            self._history_table.join(self._staging_table,
            1 == 1))

        # s1 = select([table1.c.a, table2.c.b]).\
        #      select_from(table1.join(table2,
        #     table1.c.a==table2.c.a))

        
        stmt = (self._history_table.insert().
                from_select(self.get_his_col_names(), sel))

        # stmt = (self._history_table.update().
        #         values(bus_cols).where(or_(
        #            *compare_columns)
        #         ))
        return str(stmt.compile(self._con))
        # print(s1)
