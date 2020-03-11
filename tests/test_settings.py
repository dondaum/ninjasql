import unittest
import os
import textwrap
from tests import config
from ninjasql.settings import Config


CONFIGPATH = os.path.dirname(config.__file__)


class ConfigSettingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.inif_name = "ninjasql.ini"
        cls.nf_path = os.path.join(CONFIGPATH, cls.inif_name)
        ConfigSettingTest._save_file(
            content=ConfigSettingTest._ini_file_content(),
            path=cls.nf_path
        )

    @classmethod
    def tearDownClass(cls):
        ConfigSettingTest._rm_file(cls.nf_path)

    @staticmethod
    def _ini_file_content() -> str:
        return textwrap.dedent("""\
        [Staging]
        schema_name=STAGING
        table_prefix_name=STG_

        [PersistentStaging]
        schema_name=PERS_STAGING
        table_prefix_name=PERS_STG
        """)

    @staticmethod
    def _save_file(content: str, path: str) -> None:
        with open(path, "w") as f:
            f.write(content)

    @staticmethod
    def _rm_file(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass

    def _get_config_path(self) -> str:
        """
        return config path
        """
        inif_name = "ninjasql.ini"
        return os.path.join(CONFIGPATH, inif_name)

    def test_if_init_file_path_is_settable(self):
        """
        test if a path for ini file can be set
        """
        self.assertIsNotNone(Config(ini_path=self._get_config_path()))

    def test_if_ini_file_exist(self):
        """
        test if a ini file exist at the given path
        """
        self.assertEqual(os.path.exists(self._get_config_path()), True)

    def test_configfile_is_readable(self):
        """
        test if config ini file can be read
        """
        conf = self._get_config_path()
        c = Config()
        c.ini_path = conf
        c.read()
        self.assertIsNotNone(c.config)

    def test_if_main_section_exists(self):
        """
        test if main config sections are available
        """
        conf = self._get_config_path()
        c = Config(ini_path=conf)
        c.read()

        expected_sections = [
            "Staging",
            "PersistentStaging"
        ]

        for _ in expected_sections:
            self.assertIn(_, c.config.sections())


if __name__ == "__main__":
    unittest.main()
