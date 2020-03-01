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
        f_path = os.path.join(FILEPATH, f"{self.name}.{self.type}")
        if self.type == 'csv':
            df.to_csv(path_or_buf=f_path,
                      index=False,
                      sep=self.seperator,
                      header=self.header)
        elif self.type == 'json':
            df.to_json(path_or_buf=f_path,
                       orient='records')

    def add_rows(self, key: dict):
        self.df.append(key)


if __name__ == "__main__":
    c = FileGenerator(type="csv")
