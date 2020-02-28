import pandas as pd
import os
from tests import landingzone

FILEPATH = os.path.dirname(landingzone.__file__)


class FileGenerator(object):
    def __init__(self,
                 type: str,
                 name: str,
                 header: bool = True,
                 seperator: str = ','
                 ):
        self.type = type
        self.name = name
        self.header = header
        self.seperator = seperator
        self.df = []

    def create(self):
        df = pd.DataFrame(self.df)
        if self.type == 'csv':
            f_path = os.path.join(FILEPATH, f"{self.name}.{self.type}")
            df.to_csv(path_or_buf=f_path,
                      index=False,
                      sep=self.seperator,
                      header=self.header)

    def add_rows(self, key: dict):
        self.df.append(key)


if __name__ == "__main__":
    c = FileGenerator(type="csv")
