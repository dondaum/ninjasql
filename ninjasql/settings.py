from pathlib import Path
import logging
import configparser

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(name)s %(levelname)s:%(message)s]')
log = logging.getLogger(__name__)


class Config:
    """
    :ini_path : path of the ini file for the configuration
    """
    def __init__(self,
                 ini_path: str = ''):
        self._ini_path = Path(ini_path)
        self._config = None

    @property
    def ini_path(self):
        """
        Get ini path
        """
        return self._ini_path

    @ini_path.setter
    def ini_path(self, value):
        """
        change ini path
        """
        try:
            self._ini_path = Path(value)
        except Exception as e:
            log.error(f"Please provide a valid path. Error: {e}")

    @property
    def config(self):
        """
        Get config
        """
        return self._config

    def read(self) -> None:
        """
        method that reads a ini file and its content
        """
        self._config = configparser.ConfigParser()
        self._config.read(self._ini_path)
