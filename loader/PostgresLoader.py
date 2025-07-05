import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
from loader.BaseLoader import BaseLoader

DEFAULT_THRESHOLD = 5_000_000  # rows
DEFAULT_MEMORY_MB = 1024
CHUNKSIZE_SQL = 1_000_000
PROGRESS_EVERY = 10
logger = logging.getLogger(__name__)

class PostgresLoader(BaseLoader):
    def __init__(self, config):
        super().__init__(config)
        self._conf = self.config['postgres']
        self._user = self._conf['user']
        self._password = self._conf['password']
        self._host = self._conf['host']
        self._port = self._conf['port']
        self._dbname = self._conf['dbname']
        self._schema = self._conf['schema']
        self._conn_str = f"postgresql://{self._user}:{self._password}@{self._host}:{self._port}/{self._dbname}"
        self.query_or_path = self._conf['query']

        self.chunksize: int = int(self._conf.get("chunksize", CHUNKSIZE_SQL))
        self.rows_threshold: int = int(self._conf.get("rows_threshold", DEFAULT_THRESHOLD))
        self.memory_limit_mb: int = int(self._conf.get("memory_limit_mb", DEFAULT_MEMORY_MB))
        self.progress_every: int = int(self._conf.get("progress_every", PROGRESS_EVERY))



    def load(self)-> pd.DataFrame:
        df  = self.load_postgres()
        if df.empty:
            return pd.DataFrame()
        return df

    def load_postgres(self)-> pd.DataFrame:
        engine = create_engine(self._conn_str)
        sql_text = self._read_query()
        est_rows = self._estimate_rows(sql_text)
        eager_possible = (
                est_rows is not None
                and est_rows < self.rows_threshold
                and self._fits_memory_estimate(est_rows)
        )

        if eager_possible:
            logger.info("SqlLoader eager mode (rows≈%s)", est_rows)
            df = pd.read_sql_query(sql_text, self.engine)
            return self.transformer.map_and_transform(df)

        logger.info(
            "SqlLoader streaming with chunksize=%d (rows≈%s)",
            self.chunksize,
            est_rows or "unknown",
        )
        return self._stream_and_concat(sql_text)

    def _stream_and_concat(self, sql_text: str) -> pd.DataFrame:
        parts: List[pd.DataFrame] = []
        for i, chunk in enumerate(
                pd.read_sql_query(sql_text, self.engine, chunksize=self.chunksize), start=1
        ):
            transformed = self.transformer.map_and_transform(chunk)
            parts.append(transformed)
            if i % self.progress_every == 0:
                logger.info(" … processed %d chunks (rows≈%d)", i, i * self.chunksize)
        result = pd.concat(parts, ignore_index=True)
        logger.info("Finished SQL streaming. Total rows=%d", len(result))
        return result

    def _read_query(self) -> str:
        path = Path(self.query_or_path)
        if path.is_file():
            return path.read_text(encoding="utf-8")
        return self.query_or_path  # treat as inline SQL

    def _estimate_rows(self, sql_text: str) -> Optional[int]:
        """Attempt to estimate row‑count via COUNT(*) rewrite; returns None if fails."""
        try:
            count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql_text}) subquery"
            with self.engine.connect() as conn:
                result = conn.execute(sa.text(count_sql))
                return int(result.scalar())  # type: ignore[call-arg]
        except Exception:  # pragma: no cover
            return None

    def _fits_memory_estimate(self, n_rows: int) -> bool:
        est_mb = n_rows * 0.5 / 1024
        return est_mb < self.memory_limit_mb
