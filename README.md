# pymqttd

A small MQTT 3.1.1 broker you can `pip install`. It is [rumqttd](https://github.com/bytebeamio/rumqtt) (Rust) built
as an executable and shipped in a wheel, with a Python launcher that runs it as a child process.

It is meant for an application that needs a broker on the same host and cannot count on one being installed: start it
on a free local port, connect your clients, and it goes away with your process.

```sh
pip install pymqttd
```

## From Python

```python
import pymqttd

async with pymqttd.Broker() as broker:          # 127.0.0.1, a free port
    print(broker.port)
```

Without asyncio:

```python
with pymqttd.BrokerProcess() as broker:
    print(broker.port)
```

Both take `host`, `port` (0, the default, picks a free one), `username` and `password` (together; anonymous clients
are then refused), `max_payload_size` (bytes, default 1 MiB) and `log` (rumqttd's log filter, off by default).
`start()` returns once the broker accepts MQTT connections and raises `pymqttd.BrokerError` if it cannot start.

The broker runs in its own process, so a busy or blocked Python process does not slow it down. It exits when its parent
does, however the parent ends: it watches the stdin pipe the parent holds.

## From the command line

```sh
pymqttd --host 127.0.0.1 --port 1883
python -m pymqttd --help
```

Once it accepts connections it prints `ready <host>:<port>` on stdout. `--exit-on-stdin-close` makes it exit when stdin
reaches end of file.

## Limits

- MQTT 3.1.1 over plain TCP. No TLS, no websockets, no MQTT 5.
- Retained messages and sessions live in memory: they are gone when the broker stops.
- rumqttd logs every client disconnect, clean ones included, as an error, so its log is off unless you pass `--log`
  (`log=` in Python).

## Licence

MIT. The executable includes rumqttd, which is licensed under Apache-2.0.
