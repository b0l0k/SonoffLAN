import asyncio
import json

import pytest

from custom_components.sonoff.core.ewelink import SIGNAL_UPDATE
from custom_components.sonoff.core.ewelink.local import encrypt

from . import DEVICEID, DummyRegistry, init


@pytest.fixture(autouse=True)
def restore_asyncio(monkeypatch):
    # The shared init helper replaces these globals while constructing entities.
    monkeypatch.setattr(asyncio, "create_task", asyncio.create_task)
    monkeypatch.setattr(asyncio, "get_running_loop", asyncio.get_running_loop)


@pytest.mark.parametrize(
    "agent,action",
    [("device", "update"), ("app", "update"), ("device", None), ("device", "sysmsg")],
)
def test_cloud_updates_keep_legacy_parameters_and_message_context(agent, action):
    reg, entities = init({"extra": {"uiid": 1}, "params": {"switch": "on"}})
    received = []
    reg.dispatcher_connect(DEVICEID, received.append)  # Existing one-argument API.
    params = {"switch": "off"}
    msg = {
        "deviceid": DEVICEID,
        "params": params,
        "userAgent": agent,
        "sequence": "command-1",
        "d_seq": "report-1",
    }
    if action is not None:
        msg["action"] = action
    reg.call(reg.cloud._process_ws_msg(msg))

    assert received == [params]
    update = received[0]
    assert isinstance(update, dict)
    assert update.keys() == params.keys()
    assert json.loads(json.dumps(update)) == params
    assert update.source == "cloud"
    assert update.message == msg
    assert entities[0].state == "off"


@pytest.mark.parametrize("encrypted", [False, True])
def test_local_updates_keep_decrypted_parameters_and_message_context(encrypted):
    reg, entities = init({"extra": {"uiid": 1}, "params": {"switch": "off"}})
    received = []
    reg.dispatcher_connect(DEVICEID, received.append)
    params = {"switch": "on"}
    msg = {"deviceid": DEVICEID, "localtype": "plug", "seq": "lan-report-1"}
    if encrypted:
        key = "synthetic-test-device-key"
        reg.devices[DEVICEID]["devicekey"] = key
        msg.update(encrypt({"data": params}, key))
    else:
        msg["params"] = params
    reg.local.dispatcher_send(SIGNAL_UPDATE, msg)

    assert received == [params]
    update = received[0]
    assert update.source == "local"
    assert update.message == msg
    assert entities[0].state == "on"
    assert type(reg.devices[DEVICEID]["params"]) is dict
    assert "source" not in reg.devices[DEVICEID]["params"]
    assert "message" not in reg.devices[DEVICEID]["params"]


def test_unknown_sensor_does_not_publish_message_metadata():
    reg, entities = init({"extra": {"uiid": 0}, "params": {"property": 123}})
    reg.cloud.dispatcher_send(
        SIGNAL_UPDATE,
        {
            "deviceid": DEVICEID,
            "action": "update",
            "userAgent": "device",
            "d_seq": "report-1",
            "apikey": "synthetic-message-metadata",
            "params": {"property": 456},
        },
    )
    sensor = entities[0]
    assert sensor.extra_state_attributes == {"property": 456}
    state = sensor.hass.states.get(sensor.entity_id)
    assert state.attributes["property"] == 456
    assert "synthetic-message-metadata" not in json.dumps(dict(state.attributes))


def test_nested_dispatch_keeps_each_updates_own_context():
    reg = DummyRegistry()
    reg.devices[DEVICEID] = {"online": True, "params": {}}
    received = []

    def receive(update):
        if update.source == "cloud":
            reg.local_update(
                {"deviceid": DEVICEID, "seq": "inner", "params": {"switch": "off"}}
            )
        received.append((update.source, update.message.get("seq"), dict(update)))

    reg.dispatcher_connect(DEVICEID, receive)
    reg.cloud_update({"deviceid": DEVICEID, "params": {"switch": "on"}})
    assert received == [
        ("local", "inner", {"switch": "off"}),
        ("cloud", None, {"switch": "on"}),
    ]


def test_parent_updates_do_not_restore_a_removed_child():
    parentid = "1000parent"
    reg, entities = init(
        [
            {
                "deviceid": parentid,
                "extra": {"uiid": 1},
                "params": {"switch": "off"},
            },
            {
                "deviceid": DEVICEID,
                "extra": {"uiid": 1},
                "params": {"switch": "off", "parentid": parentid},
            },
        ]
    )
    child = next(e for e in entities if e.device["deviceid"] == DEVICEID and not e.uid)
    loop = asyncio.new_event_loop()
    try:
        child.hass.loop = loop
        loop.run_until_complete(child.async_remove(force_remove=True))
        reg.devices[DEVICEID]["online"] = False
        reg.cloud_update({"deviceid": parentid, "params": {"switch": "on"}})
        assert child.hass.states.get(child.entity_id) is None
    finally:
        loop.close()
