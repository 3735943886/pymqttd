from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import textwrap
import time

import paho.mqtt.client as mqtt
import pymqttd
import pytest


def client(port: int, client_id: str, **kwargs) -> mqtt.Client:
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    if "username" in kwargs:
        c.username_pw_set(kwargs["username"], kwargs.get("password"))
    if "will" in kwargs:
        c.will_set(*kwargs["will"])
    return c


def collect(port: int, client_id: str, topic: str, wait: float = 0.5) -> list[tuple[str, bytes, bool]]:
    """What a fresh client subscribed to `topic` receives within `wait` seconds."""
    seen: list[tuple[str, bytes, bool]] = []
    c = client(port, client_id)
    c.on_message = lambda _c, _u, m: seen.append((m.topic, m.payload, bool(m.retain)))
    c.connect("127.0.0.1", port)
    c.loop_start()
    c.subscribe(topic, 1)
    time.sleep(wait)
    c.loop_stop()
    c.disconnect()
    return seen


def publish(port: int, items: list[tuple[str, str, bool]]) -> None:
    c = client(port, "publisher")
    c.connect("127.0.0.1", port)
    c.loop_start()
    for topic, payload, retain in items:
        c.publish(topic, payload, 1, retain).wait_for_publish(5)
    c.loop_stop()
    c.disconnect()


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def test_start_picks_a_free_port_and_stop_ends_the_process():
    broker = pymqttd.Broker()
    await broker.start()
    assert broker.running and broker.port > 0
    pid = broker._proc.pid
    socket.create_connection(("127.0.0.1", broker.port), timeout=1).close()
    await broker.stop()
    assert not broker.running and not alive(pid)


def test_retained_messages_go_only_to_matching_subscriptions():
    """A new subscriber gets the retained messages its own filter matches, once each, and nothing of what other
    clients subscribe to (amqtt 0.12.1 replays every client's subscriptions to each new one)."""
    with pymqttd.BrokerProcess() as broker:
        port = broker.port
        publish(port, [("a/1/state", "x", True), ("b/1/state", "y", True), ("a/2/state", "z", True)])
        other = client(port, "other")
        other.connect("127.0.0.1", port)
        other.loop_start()
        other.subscribe("b/#", 1)
        time.sleep(0.2)
        try:
            seen = collect(port, "fresh", "a/+/state")
        finally:
            other.loop_stop()
            other.disconnect()
        assert sorted(seen) == [("a/1/state", b"x", True), ("a/2/state", b"z", True)]


def test_an_empty_retained_payload_clears_it():
    with pymqttd.BrokerProcess() as broker:
        publish(broker.port, [("t/1", "v", True), ("t/1", "", True)])
        assert collect(broker.port, "late", "t/#") == []


def test_a_live_publish_reaches_a_subscriber_not_retained():
    with pymqttd.BrokerProcess() as broker:
        port = broker.port
        seen = []
        sub = client(port, "sub")
        sub.on_message = lambda _c, _u, m: seen.append((m.topic, m.payload, bool(m.retain)))
        sub.connect("127.0.0.1", port)
        sub.loop_start()
        sub.subscribe("live/#", 1)
        time.sleep(0.2)
        publish(port, [("live/x", "1", False)])
        time.sleep(0.3)
        sub.loop_stop()
        sub.disconnect()
        assert seen == [("live/x", b"1", False)]


def test_a_dropped_client_publishes_its_will():
    with pymqttd.BrokerProcess() as broker:
        port = broker.port
        seen = []
        watcher = client(port, "watcher")
        watcher.on_message = lambda _c, _u, m: seen.append((m.topic, m.payload))
        watcher.connect("127.0.0.1", port)
        watcher.loop_start()
        watcher.subscribe("status/#", 1)
        time.sleep(0.2)

        doomed = client(port, "doomed", will=("status/doomed", "offline", 1, True))
        doomed.connect("127.0.0.1", port)
        doomed.loop_start()
        time.sleep(0.2)
        doomed.socket().close()                          # no DISCONNECT: the broker sends the will
        doomed.loop_stop()
        for _ in range(30):
            if seen:
                break
            time.sleep(0.1)
        watcher.loop_stop()
        watcher.disconnect()
        assert ("status/doomed", b"offline") in seen


def connected(c: mqtt.Client, port: int) -> bool:
    """Whether the broker accepts `c`'s CONNECT."""
    c.connect("127.0.0.1", port)
    for _ in range(20):
        c.loop(0.1)
        if c.is_connected():
            break
    ok = c.is_connected()
    c.disconnect()
    return ok


async def test_credentials_refuse_anonymous_clients():
    async with pymqttd.Broker(username="u", password="p") as broker:
        assert not connected(client(broker.port, "anon"), broker.port)
        assert not connected(client(broker.port, "wrong", username="u", password="nope"), broker.port)
        assert connected(client(broker.port, "authed", username="u", password="p"), broker.port)


async def test_a_taken_port_is_an_error():
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        with pytest.raises(pymqttd.BrokerError, match="cannot listen"):
            await pymqttd.Broker(port=port).start()


def test_username_without_password_is_refused():
    with pytest.raises(ValueError):
        pymqttd.Broker(username="u")


def test_the_sync_process():
    with pymqttd.BrokerProcess() as broker:
        publish(broker.port, [("s/1", "v", True)])
        assert collect(broker.port, "late", "s/#") == [("s/1", b"v", True)]
        pid = broker._proc.pid
    assert not alive(pid)


def test_the_broker_exits_when_its_parent_is_killed():
    parent = subprocess.Popen([sys.executable, "-c", textwrap.dedent("""
        import pymqttd, time
        b = pymqttd.BrokerProcess()
        b.start()
        print(b._proc.pid, flush=True)
        time.sleep(60)
    """)], stdout=subprocess.PIPE)
    pid = int(parent.stdout.readline())
    assert alive(pid)
    parent.send_signal(signal.SIGKILL)
    parent.wait()
    for _ in range(50):
        if not alive(pid):
            break
        time.sleep(0.1)
    else:
        os.kill(pid, signal.SIGKILL)
        pytest.fail("the broker outlived its parent")


def test_python_dash_m_runs_the_executable():
    out = subprocess.run([sys.executable, "-m", "pymqttd", "--version"], capture_output=True, text=True, check=True)
    assert out.stdout.startswith("pymqttd ")


def test_a_slow_subscriber_does_not_hold_up_a_publisher():
    """A subscriber that stops reading must not slow down a publisher on another connection: the broker takes all of
    it (the closing QoS 1 message's PUBACK comes after every earlier message on the connection was read)."""
    with pymqttd.BrokerProcess() as broker:
        port = broker.port
        stalled = client(port, "stalled")
        stalled.connect("127.0.0.1", port)
        stalled.subscribe("flood/#", 0)
        stalled.loop(1)                                  # subscribed; then never read again
        pub = client(port, "fast")
        pub.max_queued_messages_set(0)
        pub.connect("127.0.0.1", port)
        pub.loop_start()
        payload = "x" * 1024
        started = time.monotonic()
        for i in range(20000):                           # 20 MB, far more than the stalled socket's buffers
            pub.publish(f"flood/{i % 10}", payload, 0)
        pub.publish("flood/end", "end", 1).wait_for_publish(20)
        elapsed = time.monotonic() - started
        pub.loop_stop()
        pub.disconnect()
        stalled.socket().close()
        assert elapsed < 20
