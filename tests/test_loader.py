import sys
import types
from functools import partial

import pandas as pd
import pytest


# Dummy Transform that just records calls to `map_and_transform`
class _DummyTransform:
    def __init__(self):
        self.calls = []

    def map_and_transform(self, df: pd.DataFrame):
        # store length of each chunk for assertions
        self.calls.append(len(df))


@pytest.fixture(autouse=True)
def _patch_transform(monkeypatch):
    """Inject DummyTransform in place of the real transformer.Transform."""
    dummy_module = types.ModuleType("transformer.transform")
    dummy_module.Transform = _DummyTransform
    monkeypatch.setitem(sys.modules, "transformer.transform", dummy_module)
    # also update the parent alias 'transformer'
    transformer_pkg = types.ModuleType("transformer")
    transformer_pkg.transform = dummy_module
    monkeypatch.setitem(sys.modules, "transformer", transformer_pkg)
    yield


def test_csv_loader_calls_transformer(tmp_path, monkeypatch):
    # -- create sample CSV ---------------------------------------------------
    # df = pd.DataFrame({"a": range(5), "b": list("abcde")})
    # csv_path = tmp_path / "sample.csv"
    # df.to_csv(csv_path, index=False)

    # -- loader config -------------------------------------------------------
    from importlib import import_module
    csv_path = "/Users/irenagov/work/duality_pipeline/data/transactions.csv"
    config = {"csv": {"chunksize": 2, "csv_path": str(csv_path)}}
    CsvLoader = import_module("loader.CsvLoader").CsvLoader

    loader = CsvLoader(config)

    # swap in dummy transformer instance so we can inspect later
    dummy_transform = _DummyTransform()
    monkeypatch.setattr(loader, "transformer", dummy_transform, raising=True)

    # -- exercise ------------------------------------------------------------
    res = loader.load_csv()
    print(f"loaded: {res}")
    # -- verify --------------------------------------------------------------
    assert dummy_transform.calls == [3]  # 5 rows → 2,2,1 with chunk=2


#@pytest.mark.parametrize("engine", ["pyarrow", "fastparquet", None])
def test_parquet_loader_roundtrip(tmp_path, monkeypatch):
    # # skip if pyarrow/fastparquet missing when requested
    # if engine and engine == "pyarrow":
    #     pytest.importorskip("pyarrow")
    # if engine and engine == "fastparquet":
    #     pytest.importorskip("fastparquet")

    df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    pq_path = tmp_path / "sample.parquet"
    df.to_parquet(pq_path)

    from importlib import import_module

    config = {"parquet": {"path" : str(pq_path)}}
    ParquetLoader = import_module("loader.ParquetLoader").ParquetLoader
    loader = ParquetLoader(config)
    dummy_transform = _DummyTransform()
    monkeypatch.setattr(loader, "transformer", dummy_transform, raising=True)

    res = loader.load_parquet()
    print(f"loaded: {res}")
    # -- verify --------------------------------------------------------------
    assert dummy_transform.calls == [3]
   # pd.testing.assert_frame_equal(res.sort_index(axis=1), df.sort_index(axis=1))
