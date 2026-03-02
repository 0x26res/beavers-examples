import dataclasses
from datetime import datetime
from typing import Optional, Sequence, Tuple, Type

import confluent_kafka
import protarrow
import pyarrow as pa
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    record_subject_name_strategy,
)
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
)
from google.protobuf.message import Message as ProtoMessage
from google.protobuf.timestamp_pb2 import Timestamp

from aiven_protos.coinbase_pb2 import Status, Ticker


def _optional_float(data: dict, key: str) -> Optional[float]:
    v = data.get(key)
    if v is not None and v != "":
        return float(v)
    return None


def _parse_timestamp(time_str: str) -> Timestamp:
    dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def make_ticker(data: dict) -> Ticker:
    kwargs = {}
    if "sequence" in data:
        kwargs["sequence"] = int(data["sequence"])
    if "product_id" in data:
        kwargs["product_id"] = data["product_id"]
    for field in (
        "open_24h",
        "low_24h",
        "high_24h",
        "volume_24h",
        "volume_30d",
        "best_bid",
        "best_bid_size",
        "best_ask",
        "best_ask_size",
        "price",
        "last_size",
    ):
        v = _optional_float(data, field)
        if v is not None:
            kwargs[field] = v
    if "side" in data:
        kwargs["side"] = data["side"]
    if "time" in data:
        kwargs["time"] = _parse_timestamp(data["time"])
    v = data.get("trade_id")
    if v is not None:
        kwargs["trade_id"] = int(v)
    return Ticker(**kwargs)


def make_status(data: dict) -> Status:
    kwargs = {}
    for field in (
        "id",
        "base_currency",
        "quote_currency",
        "display_name",
        "status",
        "status_message",
        "type",
    ):
        if field in data:
            kwargs[field] = data[field]
    for field in (
        "base_increment",
        "quote_increment",
        "min_market_funds",
        "max_slippage_percentage",
    ):
        v = _optional_float(data, field)
        if v is not None:
            kwargs[field] = v
    for field in (
        "post_only",
        "limit_only",
        "cancel_only",
        "fx_stablecoin",
        "margin_enabled",
        "auction_mode",
    ):
        if field in data:
            kwargs[field] = bool(data[field])
    return Status(**kwargs)


def make_serializers(
    schema_registry_client: SchemaRegistryClient,
) -> Tuple[ProtobufSerializer, ProtobufSerializer]:
    conf = {"subject.name.strategy": record_subject_name_strategy}
    return (
        ProtobufSerializer(Ticker, schema_registry_client, conf=conf),
        ProtobufSerializer(Status, schema_registry_client, conf=conf),
    )


@dataclasses.dataclass(frozen=True)
class ProtoArrowParser:
    message_type: Type[ProtoMessage]
    deserializer: ProtobufDeserializer
    schema: pa.Schema

    @staticmethod
    def create(message_type: Type[ProtoMessage]) -> "ProtoArrowParser":
        deserializer = ProtobufDeserializer(
            message_type,
            conf={"subject.name.strategy": record_subject_name_strategy},
        )
        schema = protarrow.message_type_to_schema(message_type)
        return ProtoArrowParser(message_type, deserializer, schema)

    def __call__(self, messages: Sequence[confluent_kafka.Message]) -> pa.Table:
        protos = []
        for msg in messages:
            if msg.value():
                protos.append(self.deserializer(msg.value(), None))
        if protos:
            return protarrow.messages_to_table(protos, self.message_type)
        return self.schema.empty_table()
