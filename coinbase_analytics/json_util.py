import dataclasses
import io
from typing import Sequence

import confluent_kafka
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json


def get_raw_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [
            f.with_type(pa.string()) if pa.types.is_floating(f.type) else f
            for f in schema
        ]
    )


def fix_schema(table: pa.Table, schema: pa.Schema):
    for f in schema:
        if pa.types.is_floating(f.type):
            column = table.column(f.name)
            table = table.set_column(
                table.schema.get_field_index(f.name),
                f.name,
                pc.replace_with_mask(
                    column,
                    pc.equal(pc.utf8_length(column), 0).combine_chunks(),
                    pa.scalar(None, pa.string()),
                ).cast(pa.float64()),
            )
    return table


@dataclasses.dataclass(frozen=True)
class JsonArrowParser:
    schema: pa.Schema
    raw_schema: pa.Schema

    @staticmethod
    def create(schema: pa.Schema) -> "JsonArrowParser":
        return JsonArrowParser(schema, get_raw_schema(schema))

    def __call__(self, messages: Sequence[confluent_kafka.Message]) -> pa.Table:
        if messages:
            with io.BytesIO() as buffer:
                for message in messages:
                    if message.value():
                        buffer.write(message.value())
                        buffer.write(b"\n")
                buffer.seek(0)
                table = pyarrow.json.read_json(
                    buffer,
                    parse_options=pyarrow.json.ParseOptions(
                        explicit_schema=self.raw_schema
                    ),
                )

                return fix_schema(table, self.schema)
        else:
            return self.schema.empty_table()
