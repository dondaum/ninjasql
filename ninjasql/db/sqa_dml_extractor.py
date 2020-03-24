from sqlalchemy.sql.schema import Table


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

    def get_table_name(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        return [c.table.name for c in self._staging_table.c][0]

    def simple_insert(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        """
        stmt = (self._staging_table.update().
                where(self._staging_table.c.id == 5).
                values(number=20))
        return str(stmt.compile(self._con))

    def scd2_insert(self):
        """
        method that returns all columns
        COLUMN class has:
        {'key': X, 'name': Y, 'table': T}
        # https://social.msdn.microsoft.com/Forums/SECURITY/en-US/d3ed03bb-d1db-4559-b35c-a0635ae639ab/alternate-queries-for-merge?forum=transactsql
        # https://kite.com/python/docs/sqlalchemy.sql.Insert
        """
        sel = select([table1.c.a, table1.c.b]).where(table1.c.c > 5)
        stmt = (self._history_table.insert().
                where(self._staging_table.c.id == 5).
                values(number=20))
        return str(stmt.compile(self._con))
