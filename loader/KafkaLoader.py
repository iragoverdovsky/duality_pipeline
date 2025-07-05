import pandas as pd
from kafka import KafkaConsumer
import json
from loader.BaseLoader import BaseLoader


class KafkaLoader(BaseLoader):
    def __init__(self, config):
        super().__init__(config)
        self._conf = config['kafka']
        self._topic = self._conf["topic"],
        self._bootstrap_servers = self._conf["bootstrap_servers"],
        self._auto_offset_reset = 'earliest',
        self._group_id = self._conf.get("group_id", "duality_pipeline"),
        self._value_deserializer = lambda m: json.loads(m.decode("utf-8"))



    def load(self)-> pd.DataFrame:
        df = self.load_kafka()
        if df.empty:
            return pd.DataFrame()
        return df

    def load_kafka(self)-> pd.DataFrame:
        _consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset='earliest',
            group_id=self._group_id,
            value_deserializer=self._value_deserializer,
        )
        records = [msg.value for msg in _consumer]
        return pd.DataFrame(records)