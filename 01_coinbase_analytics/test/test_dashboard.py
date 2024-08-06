from dashboard import (
    add_5min_change,
    TICKER_SCHEMA,
    WithAverageCalculator,
    AVERAGE_SCHEMA,
)
import pandas as pd


def test_add_5min_change():
    add_5min_change(TICKER_SCHEMA.empty_table(), TICKER_SCHEMA.empty_table())


def test_5min_average():
    average = WithAverageCalculator()
    results = average(TICKER_SCHEMA.empty_table(), pd.Timestamp.now())
    assert results.schema == AVERAGE_SCHEMA
