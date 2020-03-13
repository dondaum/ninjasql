import os
import textwrap


class IniGenerator(object):

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
