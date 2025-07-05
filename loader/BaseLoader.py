import pandas as pd

from transformer.transform import Transform




class BaseLoader(object):
    def __init__(self, config):
        self.config = config
        self.transformer = Transform()

    def load(self) -> pd.DataFrame:
        return pd.DataFrame()