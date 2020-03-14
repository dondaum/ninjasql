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
    SECTIONS = {
        "Staging": [
            "schema_name",
            "table_prefix_name"
        ],
        "PersistentStaging": [
            "schema_name",
            "table_prefix_name"
        ]
    }

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
        self._check_content()

    def _check_content(self) -> None:
        """
        method that reads expected sections and options of the
        ini file
        """
        for key, value in self.__class__.SECTIONS.items():
            for sec in value:
                try:
                    self._config.get(key, sec)
                except configparser.NoSectionError as e:
                    log.error(f"Can't find Section: {key} in ini file."
                              "Error: {e}")
                    raise e
                except configparser.NoOptionError as e:
                    log.error(f"Can't find option: {sec} in section {key} "
                              "ini file. Error: {e}")
                    raise e
