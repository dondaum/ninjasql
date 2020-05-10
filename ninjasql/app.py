from pathlib import Path
import os
import logging
import pandas as pd
from pandas import DataFrame
from pandas.io.sql import (
    pandasSQL_builder,
    SQLTable,
    SQLDatabase,
    SQLiteTable)
from sqlalchemy.schema import CreateSchema
from sqlalchemy import inspect
from datetime import datetime
import traceback

from ninjasql.errors import NoColumnsError, NoTableNameGivenError
from ninjasql.settings import Config
from ninjasql.db.sqa_dml_extractor import SqaExtractor
from ninjasql.db.sqa_table_loads import get_sqa_tableload
from ninjasql.dep.table_dependency import TableDep

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
        self._Dag = TableDep.Instance()

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
            self._csv_reader()
        elif self._type == 'json':
            self._json_reader()

    def _build_header(self) -> None:
        """
        Instance method that implements a correct header
        TODO: This method has side effects
        """
        if not self._has_header():
            self._columns = None

    def _csv_reader(self) -> None:
        """
        Instance method that implements a csv reader
        TODO: This method has side effects
        """
        self._build_header()
        try:
            self._data = pd.read_csv(
                filepath_or_buffer=self._file,
                sep=self._seperator,
                header=self._header,
                names=self._columns)
        except Exception:
            track = traceback.format_exc()
            log.error(f"Upps. Check file and location. Error: {track}")

    def _json_reader(self):
        """
        Instance method that implements a json reader
        """
        self._build_header()
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
        self._load_df_if_empty()
        return self._data.columns

    def get_dtypes(self) -> dict:
        """
        Method that get columns datatype as dict
        """
        self._load_df_if_empty()
        if not self._has_header():
            log.error("Column datatypes are asked but no columns are "
                      "specified or given in a file.")
            raise NoColumnsError
        return self._data.dtypes.to_dict()

    def col_to_str(self) -> None:
        """
        Method that change all column data type to a string type
        """
        self._load_df_if_empty()
        self._data = self._data.astype(str)

    def _is_file(self) -> bool:
        """
        Checks if a given file exist or not
        """
        self._file = Path(self._file)
        return self._file.is_file()

    def _load_df_if_empty(self) -> None:
        """
        Instance method that loads data in a pandas df
        if not already done.
        TODO: This method has side effects
        """
        if self._data is None:
            self._read_data()

    def _set_path(self, path: str) -> Path:
        """
        Instance method that converts a str to a Path object
        """
        try:
            tpath = Path(path)
            return tpath
        except Exception as e:
            log.error(f"Please provide a valid path. Error: {e}")
            raise e

    def save_staging_ddl(self,
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
        self._load_df_if_empty()
        tpath = self._set_path(path=path)

        try:
            qu_name = self._build_name(
                table=table_name,
                db=database,
                schema=schema,
                table_type="staging"
            )
            ddl = self._extract_ddl(
                frame=self._data,
                name=qu_name,
                dtype=dtype
            )
            self._save_file(
                path=tpath,
                fname=qu_name,
                content=ddl,
                subdir='DDL'
            )
        except ValueError as e:
            log.error(f"{e}")

    def _extract_ddl(self,
                     frame: DataFrame,
                     name: str,
                     dtype: dict) -> str:
        """
        Instance method that extracts the ddl statement of the
        pandals sql core module
        """
        return pd.io.sql.get_schema(frame=frame,
                                    name=name,
                                    con=self._con,
                                    dtype=dtype)

    def save_history_ddl(self,
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
        self._load_df_if_empty()
        tpath = self._set_path(path=path)

        self._add_scd2_attributes()
        try:
            qu_name = self._build_name(table=table_name,
                                       db=database,
                                       schema=schema,
                                       table_type="history")
            ddl = self._extract_ddl(
                frame=self._his_data,
                name=qu_name,
                dtype=dtype
            )

            self._save_file(
                path=tpath,
                fname=qu_name,
                content=ddl,
                subdir='DDL'
            )
        except ValueError as e:
            log.error(f"{e}")

    def _add_scd2_attributes(self) -> None:
        """
        Instance method that add scd2 relevant attributes
        """
        self._load_df_if_empty()
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        self._his_data = self._data.copy()
        self._his_data['UPDATED_AT'] = pd.Timestamp(now_str)
        self._his_data['BATCH_RUN_AT'] = pd.Timestamp(now_str)
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
                   subdir: str = None,
                   ) -> None:
        """
        method that save a string content to a given path.
        :param path: Directory path where file shall be saved
        :fname : Target filename
        :content: File content
        :subdir: If a subdir is given the file is saved in the subdir
        """
        if not os.path.isdir(path):
            log.error(f"Given Path is not a valid directory. Please check!")
            raise FileNotFoundError

        basename = f"{fname.replace('.', '_')}"
        modelname = basename.split('_')[-1]
        nfname = f"{basename}.sql"

        if subdir:
            modelname = os.path.join(modelname, subdir)

        Path(os.path.join(path, modelname)).mkdir(parents=True, exist_ok=True)

        with open(os.path.join(path, modelname, nfname), "w") as f:
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
        self._load_df_if_empty()
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

    def create_file_elt_blueprint(self,
                                  path: str,
                                  table_name: str,
                                  logical_pk: list,
                                  load_strategy: str,
                                  ):
        self._load_df_if_empty()
        self.save_staging_ddl(path=path, table_name=table_name)
        self.save_history_ddl(path=path, table_name=table_name)
        stg = self._get_sqa_table(
            table_name=table_name,
            table_type="staging")
        his = self._get_sqa_table(
            table_name=table_name,
            table_type="history")
        c = SqaExtractor(
            staging_table=stg,
            history_table=his,
            logical_pk=logical_pk,
            load_strategy=load_strategy,
            con=self._con)

        base_name = c.get_hist_table_name()
        self._save_file(
                path=path,
                fname=f"scd2_1_{base_name}",
                content=c.scd2_new_insert(),
                subdir='DML'
        )

        self._save_file(
                path=path,
                fname=f"scd2_2_{base_name}",
                content=c.scd2_updated_insert(),
                subdir='DML'
        )
        self._Dag.addTable(f"scd2_2_{base_name}.sql",
                           f"scd2_1_{base_name}.sql")

        self._save_file(
                path=path,
                fname=f"scd2_3_{base_name}",
                content=c.scd2_updated_update(),
                subdir='DML'
        )

        self._Dag.addTable(f"scd2_3_{base_name}.sql",
                           f"scd2_2_{base_name}.sql")

        self._save_file(
                path=path,
                fname=f"scd2_4_{base_name}",
                content=c.scd2_deleted_update(),
                subdir='DML'
        )
        self._Dag.addTable(f"scd2_4_{base_name}.sql",
                           f"scd2_3_{base_name}.sql")
        if load_strategy == 'database_table':
            self._save_file(
                    path=path,
                    fname="JOBTABLE_TABLELOAD",
                    content=get_sqa_tableload(con=self._con),
                    subdir='DDL'
            )

            self._save_file(
                path=path,
                fname=f"{base_name}_INSERT_TABLELOAD",
                content=c.get_tableload_insert(),
                subdir='DML'
            )

    def _get_sqa_table(self,
                       table_name: str,
                       table_type: str,
                       schema: str = None,
                       database: str = None,
                       dtype=None
                       ) -> None:
        """
        Method that extracts sqa table object
        """
        self._load_df_if_empty()
        pandas_sql = pandasSQL_builder(con=self._con)

        sqllite = False
        if not isinstance(pandas_sql, SQLDatabase):
            sqllite = True

        pd_db = SQLDatabase(engine=self._con)
        table_klass = SQLTable if not sqllite else SQLiteTable
        target_frame = (self._data if table_type == "staging"
                        else self._his_data)
        target_name = self._build_name(table=table_name, table_type=table_type)
        table = table_klass(
            name=target_name,
            pandas_sql_engine=pd_db,
            index=False,
            frame=target_frame
        )
        return table._create_table_setup()


if __name__ == "__main__":
    pass
