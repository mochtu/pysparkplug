"""Unit tests for EdgeNode alias conformance behavior."""

from __future__ import annotations

from typing import Callable, cast

import pytest

from pysparkplug._client import Client
from pysparkplug._datatype import DataType
from pysparkplug._edge_node import Device, EdgeNode
from pysparkplug._enums import MessageType
from pysparkplug._message import Message
from pysparkplug._metric import Metric
from pysparkplug._payload import NBirth, NData
from pysparkplug._topic import Topic


class FakeClient:
    """Small test double for the low-level MQTT client."""

    def __init__(self) -> None:
        self.published: list[tuple[Message, bool]] = []
        self.subscriptions: list[tuple[Topic, int, Callable[[Client, Message], None]]] = []
        self.will: Message | None = None

    def set_will(self, message: Message | None) -> None:
        self.will = message

    def connect(self, host: str, **kwargs: object) -> None:
        del host
        callback_obj = kwargs.get("callback")
        if callback_obj is None:
            return
        callback = cast(Callable[[Client], None], callback_obj)
        callback(cast(Client, self))

    def disconnect(self) -> None:
        return None

    def publish(self, message: Message, *, include_dtypes: bool = False) -> None:
        self.published.append((message, include_dtypes))

    def subscribe(
        self,
        topic: Topic,
        qos: int,
        callback: Callable[[Client, Message], None],
    ) -> None:
        self.subscriptions.append((topic, qos, callback))

    def unsubscribe(self, topic: Topic) -> None:
        self.subscriptions = [entry for entry in self.subscriptions if entry[0] != topic]


def _metric(name: str, alias: int | None = None) -> Metric:
    return Metric(
        timestamp=1,
        name=name,
        datatype=DataType.INT64,
        value=1,
        alias=alias,
    )


def test_edge_node_alias_mode_requires_aliases_for_all_metrics() -> None:
    fake_client = FakeClient()
    with pytest.raises(ValueError, match="must define aliases"):
        EdgeNode(
            group_id="G1",
            edge_node_id="E1",
            metrics=(
                _metric("m1", alias=1),
                _metric("m2"),
            ),
            client=cast(Client, fake_client),
        )


def test_device_alias_mode_requires_aliases_for_all_metrics() -> None:
    with pytest.raises(ValueError, match="must define aliases"):
        Device(
            device_id="D1",
            metrics=(
                _metric("d1", alias=1),
                _metric("d2"),
            ),
        )


def test_aliases_must_be_unique_across_edge_node_and_devices() -> None:
    fake_client = FakeClient()
    node = EdgeNode(
        group_id="G1",
        edge_node_id="E1",
        metrics=(_metric("m1", alias=7),),
        client=cast(Client, fake_client),
    )
    device = Device(
        device_id="D1",
        metrics=(_metric("d1", alias=7),),
    )
    with pytest.raises(ValueError, match="Duplicate alias"):
        node.register(device)


def test_nbirth_auto_metrics_get_aliases_when_alias_mode_enabled() -> None:
    fake_client = FakeClient()
    node = EdgeNode(
        group_id="G1",
        edge_node_id="E1",
        metrics=(_metric("m1", alias=1),),
        client=cast(Client, fake_client),
    )

    node.connect("localhost")

    nbirth_message, _ = fake_client.published[0]
    assert nbirth_message.topic.message_type == MessageType.NBIRTH
    nbirth_payload = cast(NBirth, nbirth_message.payload)
    assert all(metric.alias is not None for metric in nbirth_payload.metrics)


def test_alias_mode_publishes_ndata_alias_only() -> None:
    fake_client = FakeClient()
    node = EdgeNode(
        group_id="G1",
        edge_node_id="E1",
        metrics=(_metric("m1", alias=1),),
        client=cast(Client, fake_client),
    )

    node.connect("localhost")
    node.update((_metric("m1"),))

    ndata_message, include_dtypes = fake_client.published[-1]
    assert ndata_message.topic.message_type == MessageType.NDATA
    assert include_dtypes is False
    ndata_payload = cast(NData, ndata_message.payload)
    assert len(ndata_payload.metrics) == 1
    published_metric = ndata_payload.metrics[0]
    assert published_metric.alias == 1
    assert published_metric.name is None
