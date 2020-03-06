from pathlib import Path
import logging
import pandas as pd
import traceback
from ninjasql.errors import NoColumnsError

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(name)s %(levelname)s:%(message)s]')
log = logging.getLogger(__name__)


class NinjaSql(object):
    """
    1. There is a source file with data in it
    2. Profile data, get metadata
    3. Create a the needed database tables
        a. staging table
        b. historization table
    4. There need to be an import folder
    5. Archive folder
    """
    def __init__(self,
                 file: str = None,
                 seperator: str = ',',
                 header: int = 0,
                 type: str = None,
                 columns: list = None,
                 orient: str = 'records',
                 ):
        self._file = file
        self._seperator = seperator
        self._header = header
        self._type = type
        self._columns = columns
        self._orient = orient
        self._data = None

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

    def _is_file(self) -> bool:
        """
        Checks if a given file exist or not
        """
        self._file = Path(self._file)
        return self._file.is_file()


if __name__ == "__main__":
    c = NinjaSql(type="json", seperator='|')
