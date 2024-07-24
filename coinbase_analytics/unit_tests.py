import io

import pyarrow.json
import pyarrow as pa
import pytest

from coinbase_analytics.dashboard import fix_schema


def make_file(payload: bytes) -> io.BytesIO:
    buffer = io.BytesIO()
    buffer.write(payload)
    buffer.seek(0)
    return buffer


def test_json_parse():
    buffer = make_file(b'{"price":""}\n')

    with pytest.raises(
        pa.ArrowInvalid,
        match=r"JSON parse error\: Column\(\/price\) changed from number to string in row 0",
    ):
        pyarrow.json.read_json(
            buffer,
            parse_options=pyarrow.json.ParseOptions(
                explicit_schema=pa.schema([pa.field("price", pa.float64())])
            ),
        )


def test_fix_schema():
    assert fix_schema(
        pa.table({"price": ["", "123.0"]}), pa.schema([pa.field("price", pa.float64())])
    )["price"].to_pylist() == [None, 123.0]
