from pathlib import Path
import logging
import pandas as pd
import traceback

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

    def _read_data(self):
        """
        Method that reads data
        """
        pass

    def show_columns(self) -> list:
        """
        Method that shows all columns of a provided dataset
        """
        if not self._is_file():
            log.error(f"Can't find the a file. Check file path!")
        if self._type == 'csv':
            if (self._columns and not self._header):
                self._columns = None
            try:
                data = pd.read_csv(filepath_or_buffer=self._file,
                                   sep=self._seperator,
                                   header=self._header,
                                   names=self._columns)
                return data.columns
            except Exception:
                track = traceback.format_exc()
                log.info(track)
                log.error(f"Upps. Check file and location. Error: {track}")
        elif self._type == 'json':
            try:
                data = pd.read_json(
                    path_or_buf=self._file,
                    orient=self._orient)
                return data.columns
            except Exception:
                track = traceback.format_exc()
                log.info(track)
                log.error(f"Upps. Check file and location. Error: {track}")

    def get_dtypes(self) -> dict:
        """
        Method that get columns datatype as dict
        """
        pass

    def _is_file(self) -> bool:
        """
        Checks if a given file exist or not
        """
        self._file = Path(self._file)
        return self._file.is_file()


if __name__ == "__main__":
    c = NinjaSql(type="json", seperator='|')
