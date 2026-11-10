"""A small MQTT 3.1.1 broker (rumqttd) as a child process of a Python application.

    async with pymqttd.Broker() as broker:          # 127.0.0.1, a free port
        connect_to("127.0.0.1", broker.port)

The broker runs in its own process: a slow or blocked Python process never slows it down, and it exits with its parent,
however the parent ends (it watches the stdin pipe the parent holds). `BrokerProcess` is the same without asyncio.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Self

__all__ = ["Broker", "BrokerError", "BrokerProcess", "find_executable"]

_LOGGER = logging.getLogger(__name__)
_EXE = "pymqttd.exe" if sys.platform == "win32" else "pymqttd"
_PORT_ATTEMPTS = 5


class BrokerError(RuntimeError):
    """The broker did not start."""


def find_executable() -> str:
    """The `pymqttd` executable installed with this package."""
    candidates = [Path(sysconfig.get_path("scripts")) / _EXE]
    user_scheme = f"{os.name}_user"
    if user_scheme in sysconfig.get_scheme_names():
        candidates.append(Path(sysconfig.get_path("scripts", user_scheme)) / _EXE)
    # `pip install --target <dir>`: the package is <dir>/pymqttd, the executable <dir>/bin/pymqttd
    candidates.append(Path(__file__).resolve().parents[1] / "bin" / _EXE)
    for path in candidates:
        if path.is_file():
            return str(path)
    raise FileNotFoundError(f"the pymqttd executable is not installed (looked for {', '.join(map(str, candidates))})")


def _free_port(host: str) -> int:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class _Options:
    def __init__(self, host: str, port: int, username: str | None, password: str | None,
                 max_payload_size: int | None, log: str | None, executable: str | None) -> None:
        if (username is None) != (password is None):
            raise ValueError("username and password go together")
        self.host = host
        self.requested_port = port
        self.username = username
        self.password = password
        self.max_payload_size = max_payload_size
        self.log = log
        self.executable = executable

    def ports(self) -> list[int]:
        """The ports to try: the one asked for, or a few free ones (another process can take one first)."""
        if self.requested_port:
            return [self.requested_port]
        return [_free_port(self.host) for _ in range(_PORT_ATTEMPTS)]

    def command(self, port: int) -> list[str]:
        cmd = [self.executable or find_executable(), "--host", self.host, "--port", str(port), "--exit-on-stdin-close"]
        if self.username is not None:
            cmd += ["--username", self.username, "--password", self.password]
        if self.max_payload_size is not None:
            cmd += ["--max-payload-size", str(self.max_payload_size)]
        if self.log is not None:
            cmd += ["--log", self.log]
        return cmd


class Broker:
    """The broker as an asyncio-managed child process. `port=0` picks a free port; `port` holds it once started."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, username: str | None = None,
                 password: str | None = None, max_payload_size: int | None = None, log: str | None = None,
                 executable: str | None = None) -> None:
        self._options = _Options(host, port, username, password, max_payload_size, log, executable)
        self.host = host
        self.port = port
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    async def start(self, timeout: float = 10.0) -> None:
        if self._proc is not None:
            raise RuntimeError("the broker has already been started")
        errors = []
        for port in self._options.ports():
            proc = await asyncio.create_subprocess_exec(
                *self._options.command(port), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout)
            except BaseException:
                await self._kill(proc)
                raise
            if line.startswith(b"ready "):
                self._proc, self.port = proc, port
                self._stderr_task = asyncio.create_task(self._log_stderr(proc))
                return
            await self._kill(proc)
            errors.append((await proc.stderr.read()).decode(errors="replace").strip() or f"exit {proc.returncode}")
        raise BrokerError(f"pymqttd did not start: {'; '.join(errors)}")

    async def stop(self, timeout: float = 5.0) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            if proc.returncode is None:
                proc.stdin.close()                      # the broker's cue to exit
                try:
                    await asyncio.wait_for(proc.wait(), timeout)
                except TimeoutError:
                    await self._kill(proc)
        finally:
            if self._stderr_task is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await self._stderr_task
                self._stderr_task = None

    async def wait(self) -> int:
        """Until the broker exits; its exit status."""
        if self._proc is None:
            raise RuntimeError("the broker is not running")
        return await self._proc.wait()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    @staticmethod
    async def _kill(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()

    @staticmethod
    async def _log_stderr(proc: asyncio.subprocess.Process) -> None:
        async for line in proc.stderr:
            _LOGGER.warning("%s", line.decode(errors="replace").rstrip())


class BrokerProcess:
    """`Broker` without asyncio: a context manager, or `start()` / `stop()`. The broker's stderr is the parent's."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, *, username: str | None = None,
                 password: str | None = None, max_payload_size: int | None = None, log: str | None = None,
                 executable: str | None = None) -> None:
        self._options = _Options(host, port, username, password, max_payload_size, log, executable)
        self.host = host
        self.port = port
        self._proc: subprocess.Popen | None = None

    @property
    def running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def start(self, timeout: float = 10.0) -> None:
        if self._proc is not None:
            raise RuntimeError("the broker has already been started")
        for port in self._options.ports():
            proc = subprocess.Popen(self._options.command(port), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            try:
                line = _readline(proc, timeout)
            except BaseException:
                proc.kill()
                proc.wait()
                raise
            if line.startswith(b"ready "):
                self._proc, self.port = proc, port
                return
            proc.kill()
            proc.wait()
        raise BrokerError("pymqttd did not start (its stderr says why)")

    def stop(self, timeout: float = 5.0) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        if proc.poll() is None:
            proc.stdin.close()
            try:
                proc.wait(timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        proc.stdout.close()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()


def _readline(proc: subprocess.Popen, timeout: float) -> bytes:
    """The child's first stdout line, or b"" if it exits first; TimeoutError after `timeout` seconds."""
    import threading

    result: list[bytes] = []
    reader = threading.Thread(target=lambda: result.append(proc.stdout.readline()), daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():
        raise TimeoutError(f"pymqttd did not report ready within {timeout} s")
    return result[0]
