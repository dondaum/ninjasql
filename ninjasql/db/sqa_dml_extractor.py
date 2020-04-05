from sqlalchemy.sql.schema import Table
from sqlalchemy.sql import exists, and_, select, func, or_
from sqlalchemy.sql.functions import now
from sqlalchemy.sql.expression import literal_column


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

    def get_col_names(self) -> list:
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.name for c in self._staging_table.c]

    def get_his_col_names(self) -> list:
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.name for c in self._history_table.c]

    def get_source_col_names(self) -> list:
        """
        method returns all non technical or business related column names
        """
        return [i for i, j in zip(
            self.get_his_col_names(),
            self.get_col_names()) if i == j]

    def get_table_name(self) -> str:
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.table.name for c in self._staging_table.c][0]

    def get_staging_columns(self):
        """
        method that returns all staging column
        """
        return [getattr(self._staging_table.c, c) for
                c in self.get_col_names()]

    def set_metadata_colums(self,
                            batch_dt: str = None,
                            from_dt: str = None,
                            to_dt: str = None) -> list:
        """
        method that create the metadata columns for the insert
        and updates as scalar selectable
        """
        to_dt = "'9999-12-31'"
        valid_dt = "'2020-01-31'"
        batch_dt = "'2020-02-01'"
        # from_dt = None
        metadata_col = []
        metadata_col.append(
            select([literal_column("1").label("ROW")]).as_scalar()
        )
        metadata_col.append(
            select([now().label("UPDATED_AT")]).as_scalar()
        )
        metadata_col.append(
            select([func.date(
                literal_column(batch_dt)).label("BATCH_RUN_AT")]).as_scalar()
        )
        metadata_col.append(
           select([func.date(
                literal_column(
                    valid_dt)).label("VALID_FROM_DATE")]).as_scalar()
        )
        metadata_col.append(
            select([func.date(
                literal_column(to_dt)).label("VALID_TO_DATE")]).as_scalar()
        )
        return metadata_col

    def def_equal_pk_col(self) -> list:
        """
        method that returns all staging column
        """
        pk_cols = []
        for pk_col in self._logical_pk:
            pk_cols.append(
                getattr(self._staging_table.c, pk_col) ==
                getattr(self._history_table.c, pk_col)
            )
        return pk_cols

    def get_compare_columns(self) -> list:
        """
        method that returns all compare columns
        """
        compare_columns = []
        for col in self.get_source_col_names():
            compare_columns.append(
                getattr(self._staging_table.c, col) !=
                getattr(self._history_table.c, col)
            )
        return compare_columns

    def get_staging_table_pk_col(self) -> list:
        """
        method that gets all pk columns from staging table
        """
        return [getattr(self._staging_table.c, pk) for pk in self._logical_pk]

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
        all_his_columns = self.get_his_col_names()
        all_stg_columns = self.get_staging_columns()
        all_stg_columns.extend(self.set_metadata_colums())
        exist_stat = self.get_staging_table_pk_col()
        filters = self.def_equal_pk_col()

        stmt = (self._history_table.insert().
                from_select(all_his_columns,
                select(all_stg_columns).where(
                    ~exists(exist_stat).where(and_(
                        *filters)))
                        ))
        return str(stmt.compile(self._con))

    def scd2_updated_insert(self):
        """
        scd2 command for all records where an update is needed due to a
        change record in a source table. Insert a new record for an existing
        but changed dataset
        """
        # compare columns
        compare_columns = self.get_compare_columns()
        # all columns
        all_stg_columns = self.get_staging_columns()
        # get join columns
        pk_cols = self.def_equal_pk_col()

        all_stg_columns.extend(self.set_metadata_colums())

        to_dt = "'9999-12-31'"

        sel = (select(all_stg_columns).
               select_from(self._history_table.join(
                   self._staging_table,
                   and_(*pk_cols))).where(
                       or_(*compare_columns)).where(
                           self._history_table.c.VALID_TO_DATE
                           == func.date(literal_column(to_dt))))

        stmt = (self._history_table.insert().
                from_select(self.get_his_col_names(), sel))
        return str(stmt.compile(self._con))

    def scd2_updated_update(self):
        """
        scd2 command for all records where an update is needed due to a change
        in the source system. Set current latest record VALID_TO_DATE to
        current batch date
        """
        # compare columns
        compare_columns = self.get_compare_columns()
        exist_stat = self.get_staging_table_pk_col()
        filters = self.def_equal_pk_col()

        to_dt = "'9999-12-31'"
        batch_dt = "'2020-02-01'"
        # from_dt = "'2020-01-31'"

        upd = (self._history_table.update()
               .values(
                    VALID_TO_DATE=func.date(literal_column(batch_dt)),
                    UPDATED_AT=now()).where(
                        exists(exist_stat).where(
                            and_(*filters)).where(
                                or_(*compare_columns)).where(
                                    and_(self._history_table.c.VALID_TO_DATE
                                         == func.date(literal_column(to_dt)),
                                         self._history_table.c.BATCH_RUN_AT
                                         < func.date(literal_column(batch_dt))
                                         ))))
        return str(upd.compile(self._con))

    def scd2_deleted_update(self):
        """
        scd2 command for all records where an update is needed due to a change
        in the source system. Set current latest record VALID_TO_DATE to
        current batch date
        """
        to_dt = "'9999-12-31'"
        batch_dt = "'2020-02-01'"
        # from_dt = "'2020-01-31'"

        no_exist = self.get_staging_table_pk_col()
        filters = self.def_equal_pk_col()

        upd = (self._history_table.update()
               .values(
                    VALID_TO_DATE=func.date(literal_column(batch_dt)),
                    UPDATED_AT=now()).where(
                        and_(~exists(no_exist).where(
                            and_(*filters)),
                             self._history_table.c.VALID_TO_DATE
                             == func.date(literal_column(to_dt)))))
        return str(upd.compile(self._con))
