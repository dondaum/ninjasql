from sqlalchemy.sql.schema import Table
from sqlalchemy.sql import exists, and_, select, func, or_, insert
from sqlalchemy.sql.functions import now
from sqlalchemy.sql.expression import literal_column
from datetime import datetime

from ninjasql.db.sqa_table_loads import TableLoad


class SqaExtractor(object):
    """
    :param staging_table: Sqa Table class object
    :param history_table: Sqa Table class object
    :param logical_pk: Logical primary key of the target table as list
    :param con: Sqlalchemy database connection
    :param load_strategy: [jinja, database_table]
    """

    def __init__(self,
                 staging_table: Table,
                 history_table: Table,
                 logical_pk: list,
                 con,
                 load_strategy: str,
                 ):
        self._staging_table = staging_table
        self._history_table = history_table
        self._logical_pk = logical_pk
        self._con = con
        self._load_strategy = load_strategy

        ALLOWED_STRATEGIES = ['jinja', 'database_table']

        if not isinstance(self._staging_table, Table):
            raise TypeError("Must be a SQA Table class instance")
        if self._load_strategy not in ALLOWED_STRATEGIES:
            stra = ' ,'.join(ALLOWED_STRATEGIES)
            raise ValueError(f"Invalid load stragey. Allowed are: '{stra}'")

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

    def get_hist_table_name(self) -> str:
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.table.name for c in self._history_table.c][0]

    def get_staging_columns(self) -> list:
        """
        method that returns all staging column
        """
        return [getattr(self._staging_table.c, c) for
                c in self.get_col_names()]

    def set_batch_date(self) -> str:
        """
        Set the correct batch date either table load
        or jinja expression
        """
        if self._load_strategy == "jinja":
            return select([func.date(
                literal_column(
                    r"{{ batch_date }}")).label("BATCH_RUN_AT")]).as_scalar()
        elif self._load_strategy == "database_table":
            table = self.get_hist_table_name()
            return select([TableLoad.BatchDate]).where(
                TableLoad.name == table).as_scalar()

    def set_validto_date(self) -> str:
        """
        Set the correct batch date either table load
        or jinja expression
        """
        if self._load_strategy == "jinja":
            return select([func.date(
                literal_column(
                    r"{{ validto_date }}")).label(
                        "VALID_TO_DATE")]).as_scalar()
        elif self._load_strategy == "database_table":
            table = self.get_hist_table_name()
            return select([TableLoad.ValidToDate]).where(
                TableLoad.name == table).as_scalar()

    def set_subquery_validto_date(self) -> str:
        """
        Set the correct validto date either table load
        or jinja expression in a subquery
        """
        if self._load_strategy == "jinja":
            return func.date(literal_column(r"{{ validto_date }}"))
        elif self._load_strategy == "database_table":
            return select([TableLoad.ValidToDate]).where(
                TableLoad.name == self.get_hist_table_name())

    def set_subquery_batch_date(self) -> str:
        """
        Set the correct batch date either table load
        or jinja expression in a subquery
        """
        if self._load_strategy == "jinja":
            return func.date(literal_column(r"{{ batch_date }}"))
        elif self._load_strategy == "database_table":
            return select([TableLoad.BatchDate]).where(
                TableLoad.name == self.get_hist_table_name())

    def set_subquery_offsetvalidto_date(self) -> str:
        """
        Set the correct offset validto date either table load
        or jinja expression in a subquery
        """
        if self._load_strategy == "jinja":
            return func.date(literal_column(r"{{ offset_validto_date }}"))
        elif self._load_strategy == "database_table":
            return select([TableLoad.OffsetValidToDate]).where(
                TableLoad.name == self.get_hist_table_name())

    def set_validfrom_date(self) -> str:
        """
        Set the correct batch date either table load
        or jinja expression
        """
        if self._load_strategy == "jinja":
            return select([func.date(
                literal_column(
                    r"{{ validfrom_date }}")).label(
                        "VALID_FROM_DATE")]).as_scalar()
        elif self._load_strategy == "database_table":
            table = self.get_hist_table_name()
            return select([TableLoad.ValidFromDate]).where(
                TableLoad.name == table).as_scalar()

    def set_metadata_colums(self,
                            batch_dt: str = None,
                            from_dt: str = None,
                            to_dt: str = None) -> list:
        """
        method that create the metadata columns for the insert
        and updates as scalar selectable
        """
        valid_to_dt = self.set_validto_date()  # "'9999-12-31'"
        valid_from_dt = self.set_validfrom_date()  # "'2020-01-31'"
        batch_dt = self.set_batch_date()  # "'2020-02-01'"

        metadata_col = []
        metadata_col.append(
            select([now().label("UPDATED_AT")]).as_scalar()
        )
        metadata_col.append(batch_dt)
        metadata_col.append(valid_from_dt)
        metadata_col.append(valid_to_dt)
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

    def get_tableload_insert(self) -> str:
        """
        method that generate the default INSERT of the persistent staging table
        for the table load table if strategy == database_table
        """
        valid_to_dt = datetime(9999, 12, 31)  # "'9999-12-31'"
        valid_from_dt = datetime(2020, 1, 31)  # "'2020-01-31'"
        batch_dt = datetime(2020, 2, 1)  # "'2020-02-01'"
        offsetvalid_to_dt = datetime(2020, 1, 30)
        ins = insert(TableLoad).values(
            name=self.get_hist_table_name(),
            BatchDate=batch_dt,
            ValidToDate=valid_to_dt,
            ValidFromDate=valid_from_dt,
            OffsetValidToDate=offsetvalid_to_dt)

        return str(ins.compile(bind=self._con,
                               compile_kwargs={"literal_binds": True}))

    def scd2_new_insert(self) -> str:
        """
        method that insert statement
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
        return str(stmt.compile(bind=self._con,
                                compile_kwargs={"literal_binds": True}))

    def scd2_updated_insert(self) -> str:
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

        sel = (select(all_stg_columns).
               select_from(self._history_table.join(
                   self._staging_table,
                   and_(*pk_cols))).where(
                       or_(*compare_columns)).where(
                           self._history_table.c.VALID_TO_DATE
                           == self.set_subquery_validto_date()))

        stmt = (self._history_table.insert().
                from_select(self.get_his_col_names(), sel))
        return str(stmt.compile(bind=self._con,
                                compile_kwargs={"literal_binds": True}))

    def scd2_updated_update(self) -> str:
        """
        scd2 command for all records where an update is needed due to a change
        in the source system. Set current latest record VALID_TO_DATE to
        current batch date
        """
        # compare columns
        compare_columns = self.get_compare_columns()
        exist_stat = self.get_staging_table_pk_col()
        filters = self.def_equal_pk_col()

        to_dt = self.set_subquery_validto_date()  # "'9999-12-31'"
        batch_dt = self.set_subquery_batch_date()  # "'2020-02-01'"
        offset_validto = self.set_subquery_offsetvalidto_date()
        # from_dt = "'2020-01-31'"

        upd = (self._history_table.update()
               .values(
                    VALID_TO_DATE=offset_validto,
                    UPDATED_AT=now()).where(
                        exists(exist_stat).where(
                            and_(*filters)).where(
                                or_(*compare_columns)).where(
                                    and_(self._history_table.c.VALID_TO_DATE
                                         == to_dt,
                                         self._history_table.c.BATCH_RUN_AT
                                         < batch_dt
                                         ))))
        return str(upd.compile(bind=self._con,
                               compile_kwargs={"literal_binds": True}))

    def scd2_deleted_update(self) -> str:
        """
        scd2 command for all records where an update is needed due to a change
        in the source system. Set current latest record VALID_TO_DATE to
        current batch date
        """
        to_dt = self.set_subquery_validto_date()  # "'9999-12-31'"
        # batch_dt = self.set_subquery_batch_date()  # "'2020-02-01'"
        offset_validto = self.set_subquery_offsetvalidto_date()

        no_exist = self.get_staging_table_pk_col()
        filters = self.def_equal_pk_col()

        upd = (self._history_table.update()
               .values(
                    VALID_TO_DATE=offset_validto,
                    UPDATED_AT=now()).where(
                        and_(~exists(no_exist).where(
                            and_(*filters)),
                             self._history_table.c.VALID_TO_DATE
                             == to_dt)))
        return str(upd.compile(bind=self._con,
                               compile_kwargs={"literal_binds": True}))
