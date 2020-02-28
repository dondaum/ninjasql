from pathlib import Path
import logging
import pandas as pd

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
                 type: str = None):
        self._file = file
        self._seperator = seperator
        self._header = header
        self._type = type

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

    def show_columns(self) -> list:
        """
        Method that shows all columns of a provided dataset
        """
        if self._type == 'csv':
            try:
                data = pd.read_csv(filepath_or_buffer=self._file,
                                   sep=self._seperator,
                                   header=self._header)
                return data.columns
            except Exception as e:
                log.info(e)
                log.error(f"Upps. Check file and location. Error: {e}")


if __name__ == "__main__":
    c = NinjaSql(type="csv", seperator='|')
