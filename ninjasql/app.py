from pathlib import Path
import os
import logging
import pandas as pd
from sqlalchemy.schema import CreateSchema
from sqlalchemy import inspect
from datetime import datetime
import traceback
from ninjasql.errors import NoColumnsError, NoTableNameGivenError
from ninjasql.settings import Config

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(name)s %(levelname)s:%(message)s]')
log = logging.getLogger(__name__)


class FileInspector(object):
    """
    1. There is a source file with data in it
    2. Profile data, get metadata
    3. Create a the needed database tables
        a. staging table
        b. historization table
    4. There need to be an import folder
    5. Archive folder
    :param file: filepath
    :param seperator: file seperator for csv and txt files
    :param header: Is a header row with columns names given
    :param type: File type {csv, json}
    :param columns: Custom column names if no header is given
    :param orient: Json orientation
    :param con: Sqlalchemy database connection
    """
    def __init__(self,
                 cfg_path: str,
                 file: str = None,
                 seperator: str = ',',
                 header: int = 0,
                 type: str = None,
                 columns: list = None,
                 orient: str = 'records',
                 con=None
                 ):
        self._cfg_path = cfg_path
        self._file = file
        self._seperator = seperator
        self._header = header
        self._type = type
        self._columns = columns
        self._orient = orient
        self._data = None
        self._his_data = None
        self._con = con
        self.config = Config()

        self.load_config(cfg_path=self._cfg_path)

    def _has_header(self) -> bool:
        """
        Instance method that checks if header exist or not
        """
        if self._header is None and self._columns is None:
            return False
        return True

    @property
    def file(self):
        """
        Get file
        """
        return self._file

    @file.setter
    def file(self, value):
        """
        change file
        """
        try:
            self._file = Path(value)
        except Exception as e:
            log.error(f"Please provide a valid path. Error: {e}")

    def load_config(self, cfg_path: str) -> None:
        """
        method that load the ini configuration file
        """
        self.config.ini_path = cfg_path
        self.config.read()

    def _read_data(self) -> None:
        """
        Method that reads data and save it as a instance variable
        """
        if not self._is_file():
            log.error(f"Can't find the a file. Check file path!")
        if self._type == 'csv':
            if not self._has_header():
                self._columns = None
            try:
                self._data = pd.read_csv(filepath_or_buffer=self._file,
                                         sep=self._seperator,
                                         header=self._header,
                                         names=self._columns)
            except Exception:
                track = traceback.format_exc()
                log.error(f"Upps. Check file and location. Error: {track}")
        elif self._type == 'json':
            try:
                self._data = pd.read_json(
                    path_or_buf=self._file,
                    orient=self._orient)
            except Exception:
                track = traceback.format_exc()
                log.error(f"Upps. Check file and location. Error: {track}")

    def show_columns(self) -> list:
        """
        Method that shows all columns of a provided dataset
        """
        if not self._data:
            self._read_data()
        return self._data.columns

    def get_dtypes(self) -> dict:
        """
        Method that get columns datatype as dict
        """
        if self._data is None:
            self._read_data()
        if not self._has_header():
            log.error("Column datatypes are asked but no columns are "
                      "specified or given in a file.")
            raise NoColumnsError
        return self._data.dtypes.to_dict()

    def col_to_str(self) -> None:
        """
        Method that change all column data type to a string type
        """
        if self._data is None:
            self._read_data()
        self._data = self._data.astype(str)

    def _is_file(self) -> bool:
        """
        Checks if a given file exist or not
        """
        self._file = Path(self._file)
        return self._file.is_file()

    def get_staging_ddl(self,
                        path,
                        table_name: str,
                        schema: str = None,
                        database: str = None,
                        dtype=None) -> None:
        """
        Method that get the sql ddl statement and save it as a file
        in a target path
        :param path: Directory path where file should be saved
        :table name: DDL table name
        :schema: DDL schema name. If not specified take schema from the
        configuration ini file
        :database: DDL database name
        :dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns. The SQL type should
        be a SQLAlchemy type, or a string for sqlite3 fallback connection.
        """
        if self._data is None:
            self._read_data()
        try:
            tpath = Path(path)
        except Exception as e:
            log.error(f"Please provide a valid path. Error: {e}")
            raise e
        try:
            qu_name = self._build_name(table=table_name,
                                       db=database,
                                       schema=schema,
                                       table_type="staging")
            ddl = pd.io.sql.get_schema(frame=self._data,
                                       name=qu_name,
                                       con=self._con,
                                       dtype=dtype
                                       )
            self._save_file(
                path=tpath,
                fname=qu_name,
                content=ddl
            )
        except ValueError as e:
            log.error(f"{e}")

    def get_history_ddl(self,
                        path,
                        table_name: str,
                        schema: str = None,
                        database: str = None,
                        dtype=None) -> None:
        """
        Method that get the sql history ddl statement and save it as a file
        in a target path
        :param path: Directory path where file should be saved
        :table name: DDL table name
        :schema: DDL schema name
        :database: DDL database name
        :dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns. The SQL type should
        be a SQLAlchemy type, or a string for sqlite3 fallback connection.
        """
        if self._data is None:
            self._read_data()
        try:
            tpath = Path(path)
        except Exception as e:
            log.error(f"Please provide a valid path. Error: {e}")
            raise e
        self._add_scd2_attributes()
        try:
            qu_name = self._build_name(table=table_name,
                                       db=database,
                                       schema=schema,
                                       table_type="history")
            ddl = pd.io.sql.get_schema(frame=self._his_data,
                                       name=qu_name,
                                       con=self._con,
                                       dtype=dtype
                                       )
            self._save_file(
                path=tpath,
                fname=qu_name,
                content=ddl
            )
        except ValueError as e:
            log.error(f"{e}")

    def _add_scd2_attributes(self) -> None:
        """
        Instance method that add scd2 relevant attributes
        """
        if self._data is None:
            self._read_data()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        self._his_data = self._data.copy()
        self._his_data['ROW'] = pd.Int64Dtype()
        self._his_data['UPDATED_AT'] = pd.Timestamp(now_str)
        self._his_data['VALID_FROM_DATE'] = pd.Timestamp(now_str)
        self._his_data['VALID_TO_DATE'] = pd.Timestamp(now_str)

    def _build_name(self,
                    table: str,
                    table_type: str,
                    db: str = None,
                    schema: str = None
                    ) -> str:
        """
        Method that create the final qualified sql object name
        """
        if table_type == 'staging':
            t_pre = self.config.config['Staging']['table_prefix_name']
            schema = schema or self.config.config['Staging']['schema_name']
        elif table_type == 'history':
            sec = 'PersistentStaging'
            t_pre = self.config.config[sec]['table_prefix_name']
            schema = schema or self.config.config[sec]['schema_name']

        if table is None:
            log.error(f"No table name given but needed. Please specify name:")
            raise NoTableNameGivenError
        if (db and schema):
            return f"{db}.{schema}.{t_pre}_{table}"
        elif (db and not schema):
            return f"{db}.{t_pre}_{table}"
        elif (schema and not db):
            return f"{schema}.{t_pre}_{table}"
        elif (table and not schema and not db):
            return f"{t_pre}_{table}"

    def _save_file(self,
                   path: str,
                   fname: str,
                   content: str,
                   ) -> None:
        """
        method that save a string content to a given path.
        :param path: Directory path where file shall be saved
        :fname : Target filename
        :content: File content
        """
        if not os.path.isdir(path):
            log.error(f"Given Path is not a valid directory. Please check!")
            raise FileNotFoundError

        nfname = f"{fname.replace('.', '_')}.sql"
        with open(os.path.join(path, nfname), "w") as f:
            f.write(content)

    def create_db_table(self,
                        table_name: str,
                        type: str,
                        schema: str = None,
                        if_exists: str = 'replace',
                        dtype=None
                        ) -> None:
        """
        method that creates the database table without data
        :table_name path: target table name
        :schema : Target database schema name
        :if_exists: How to behave if the table already exists.
        {‘fail’, ‘replace’, ‘append’}
        :dtype : dict of column name to SQL type, default None
        Optional specifying the datatype for columns. The SQL type should
        be a SQLAlchemy type, or a string for sqlite3 fallback connection.
        :type : str {'staging', 'history'}. Determine if the staging or
        history table shall be created
        """
        if self._data is None:
            self._read_data()
        if schema:
            insp = inspect(self._con)
            schemas = insp.get_schema_names()
            if schema not in schemas:
                self._con.execute(CreateSchema(schema))
        try:
            if type == "staging":
                df = self._data[:0]
            elif type == "history":
                df = self._his_data[:0]
            df.to_sql(
                name=table_name,
                schema=schema,
                con=self._con,
                if_exists=if_exists,
                index=False,
                dtype=dtype
            )
        except Exception as e:
            log.error(f"Can't create db table. Error: {e}")
            raise e


if __name__ == "__main__":
    pass
