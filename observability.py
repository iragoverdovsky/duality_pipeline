
from __future__ import annotations

import functools
import logging
import os
import time
from typing import Any, Callable, Optional, TypeVar

from prometheus_client import Counter, Histogram, Info, start_http_server

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (  # type: ignore
        ConsoleSpanExporter,
        SimpleSpanProcessor,
    )
except ImportError:  # pragma: no cover – tracing becomes no‑op
    trace = None  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------#
# Metrics                                                                    #
# ---------------------------------------------------------------------------#
REQUEST_DURATION = Histogram(
    "duality_stage_duration_seconds",
    "Execution time of pipeline stage",
    ["stage"],
)
ROWS_PROCESSED = Counter(
    "duality_rows_processed_total",
    "Rows processed by stage",
    ["stage"],
)
EXCEPTIONS = Counter(
    "duality_stage_exceptions_total",
    "Exceptions raised by stage",
    ["stage", "exception_type"],
)
BUILD_INFO = Info("duality_build", "Version info")
BUILD_INFO.info({"version": "1.0.0"})

_T = TypeVar("_T")


def _record_metrics(stage: str, start: float, exc: Optional[BaseException], result):
    REQUEST_DURATION.labels(stage=stage).observe(time.time() - start)
    if exc:
        EXCEPTIONS.labels(stage=stage, exception_type=type(exc).__name__).inc()
    if result is not None and hasattr(result, "__len__"):
        try:
            ROWS_PROCESSED.labels(stage=stage).inc(len(result))
        except Exception:  # pragma: no cover – len() unsupported
            pass


# ---------------------------------------------------------------------------#
# Tracing                                                                    #
# ---------------------------------------------------------------------------#

def _get_tracer(stage: str):
    if trace is None:
        return None
    provider = trace.get_tracer_provider()
    if isinstance(provider, trace.NoOpTracerProvider):
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
    return trace.get_tracer(stage)


# ---------------------------------------------------------------------------#
# Decorator                                                                  #
# ---------------------------------------------------------------------------#

def observe(stage: str) -> Callable[[Callable[..., _T]], Callable[..., _T]]:
    """Decorator that records Prometheus metrics and OpenTelemetry traces."""

    def decorator(func: Callable[..., _T]) -> Callable[..., _T]:  # type: ignore[misc]
        tracer = _get_tracer(stage)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[override]
            start = time.time()
            span_ctx = (
                tracer.start_as_current_span(stage) if tracer else nullcontext()  # type: ignore[name-defined]
            )
            exc: Optional[BaseException] = None
            result = None
            with span_ctx:
                try:
                    result = func(*args, **kwargs)
                    return result
                except BaseException as err:  # noqa: BLE001 – propagate later
                    exc = err
                    raise
                finally:
                    _record_metrics(stage, start, exc, result)

        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------#
# Automatic instrumentation helpers                                          #
# ---------------------------------------------------------------------------#

def _patch_method(cls: Any, method_name: str, stage_name: str):
    original = getattr(cls, method_name)
    if getattr(original, "_is_observed", False):
        return  # already patched
    wrapped = observe(stage_name)(original)
    wrapped._is_observed = True  # type: ignore[attr-defined]
    setattr(cls, method_name, wrapped)
    logger.debug("Instrumented %s.%s", cls.__name__, method_name)


def auto_instrument():
    """Monkey‑patch key classes so their public methods emit metrics/traces."""
    from importlib import import_module

    # Load modules lazily so import order doesn't matter
    CsvLoader = import_module("duality_pipeline.loader.CsvLoader").CsvLoader
    ParquetLoader = import_module("duality_pipeline.loader.ParquetSqlLoaders").ParquetLoader
    SqlLoader = import_module("duality_pipeline.loader.ParquetSqlLoaders").SqlLoader
    QueryEngine = import_module("duality_pipeline.query_engine").QueryEngine

    _patch_method(CsvLoader, "load_csv", "csv_load")
    _patch_method(ParquetLoader, "load_parquet", "parquet_load")
    _patch_method(SqlLoader, "load_sql", "sql_load")
    _patch_method(QueryEngine, "apply_duality_query", "query_engine")

    logger.info("Auto‑instrumentation enabled for CsvLoader, ParquetLoader, SqlLoader, QueryEngine")


# ---------------------------------------------------------------------------#
# Public bootstrap                                                           #
# ---------------------------------------------------------------------------#

def init_metrics(port: int = 8000):
    """Start Prometheus HTTP exporter on *port* (default :8000)."""
    if os.getenv("DUALITY_METRICS_DISABLED") == "1":
        logger.warning("Prometheus metrics disabled via env var")
        return
    start_http_server(port)
    logger.info("Prometheus exporter serving on :%d/metrics", port)


# ---------------------------------------------------------------------------#
# Fallback context manager when tracing is unavailable                       #
# ---------------------------------------------------------------------------#
from contextlib import contextmanager, nullcontext  # noqa: E402 – after CP imports


@contextmanager
def nullcontext():  # type: ignore[override]
    yield
