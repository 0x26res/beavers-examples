from dashboard import add_5min_change, TICKER_SCHEMA


def test_add_5min_change():
    add_5min_change(TICKER_SCHEMA.empty_table(), TICKER_SCHEMA.empty_table())
