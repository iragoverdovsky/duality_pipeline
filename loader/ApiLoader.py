import pandas as pd
import requests
from loader.BaseLoader import BaseLoader


class ApiLoader(BaseLoader):
    def __init__(self, config):
        super().__init__(config)

    def load(self) -> pd.DataFrame:
        df = self.load_api()
        if df.empty:
            return pd.DataFrame()
        return df

    def load_api(conf)-> pd.DataFrame:
        _conf = conf['api']
        resp = requests.get(_conf["url"], headers=_conf.get("headers", {}))
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
