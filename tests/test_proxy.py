"""TCP proxy primitives: read_exactly + forward_and_tap + handshake replay."""

import asyncio

import pytest

from rdc_proxy import proxy as proxy_mod
from rdc_proxy import state as state_mod
from rdc_proxy.state import HANDSHAKE


class FakeReader:
    def __init__(self, data=b""):
        self.data = data
        self.pos = 0

    async def read(self, n):
        if self.pos >= len(self.data):
            return b""
        chunk = self.data[self.pos: self.pos + n]
        self.pos += len(chunk)
        return chunk


class FakeWriter:
    def __init__(self):
        self.buf = bytearray()
        self.closed = False

    def write(self, data):
        self.buf.extend(data)

    async def drain(self):
        return

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return

    def get_extra_info(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_read_exactly_returns_full_bytes():
    r = FakeReader(b"hello world")
    got = await proxy_mod.read_exactly(r, 11, timeout=1)
    assert got == b"hello world"


@pytest.mark.asyncio
async def test_read_exactly_returns_none_on_eof():
    r = FakeReader(b"hi")
    got = await proxy_mod.read_exactly(r, 10, timeout=1)
    assert got is None


@pytest.mark.asyncio
async def test_forward_and_tap_calls_tap_and_forwards():
    src = FakeReader(b"ABCDE")
    dst = FakeWriter()
    taps = []
    await proxy_mod.forward_and_tap(src, dst, taps.append, label="t")
    assert bytes(dst.buf) == b"ABCDE"
    assert b"".join(taps) == b"ABCDE"


@pytest.mark.asyncio
async def test_local_mode_replays_handshake(reset_handshake, monkeypatch):
    """LOCAL mode sends cloud greeting, waits for 576-byte RDC response,
    then sends config_msg. Telemetry after is ingested."""
    HANDSHAKE["cloud_greeting"] = b"G" * 576
    HANDSHAKE["rdc_response"] = b"R" * 576
    HANDSHAKE["config_msg"] = b"C" * 36

    rdc_response_bytes = b"R" * 576
    telemetry_bytes = b"T" * 32  # not a valid TLV — ingest will parse 0 records, that's fine
    rdc_reader = FakeReader(rdc_response_bytes + telemetry_bytes)
    rdc_writer = FakeWriter()

    # Prevent the loop from trying to upgrade to PROXY mid-test
    state_mod.STATE.internet_up = False

    await proxy_mod.local_mode(rdc_reader, rdc_writer)

    # Writer received: greeting + config_msg
    assert bytes(rdc_writer.buf) == (b"G" * 576) + (b"C" * 36)
    # Mode was set
    assert state_mod.STATE.proxy_mode == "local"


@pytest.mark.asyncio
async def test_check_internet_offline_returns_false(monkeypatch):
    """check_internet should return False when socket.create_connection fails."""
    import socket

    def fake_create_connection(*a, **kw):
        raise OSError("nope")

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    assert proxy_mod.check_internet() is False


@pytest.mark.asyncio
async def test_resolve_cloud_returns_empty_on_gaierror(monkeypatch):
    import socket

    def raise_it(*a, **kw):
        raise socket.gaierror("no resolve")

    monkeypatch.setattr(socket, "getaddrinfo", raise_it)
    # Ensure CFG has the expected keys
    from rdc_proxy.config import CFG
    CFG.setdefault("cloud_dns", "devices.kohler.com")
    CFG.setdefault("cloud_port", 5253)
    assert proxy_mod.resolve_cloud() == []


@pytest.mark.asyncio
async def test_start_server_uses_configured_listen_address():
    from rdc_proxy.config import CFG

    CFG["proxy_listen_addr"] = "127.0.0.1"
    CFG["proxy_port"] = 0

    server = await proxy_mod.start_server()
    try:
        sock = server.sockets[0]
        host, _port = sock.getsockname()[:2]
        assert host == "127.0.0.1"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_abort_connection_sends_reset_not_eof():
    """The peer must see a reset (ConnectionResetError), not a clean EOF."""
    async def handler(_reader, writer):
        proxy_mod.abort_connection(writer)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        with pytest.raises(ConnectionResetError):
            await asyncio.wait_for(reader.read(1), timeout=5)
        writer.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_abort_connection_falls_back_to_close_without_transport():
    w = FakeWriter()
    proxy_mod.abort_connection(w)
    assert w.closed is True
    proxy_mod.abort_connection(None)  # must not raise


@pytest.mark.asyncio
async def test_handle_rdc_connection_closes_rdc_side_when_session_ends(reset_handshake):
    """Whatever path the session takes, the RDC-side writer is torn down."""
    HANDSHAKE["cloud_greeting"] = b"G" * 576
    HANDSHAKE["rdc_response"] = b"R" * 576
    HANDSHAKE["config_msg"] = b"C" * 36
    state_mod.STATE.internet_up = False
    state_mod.STATE.internet_stable_since = None

    rdc_writer = FakeWriter()
    await proxy_mod.handle_rdc_connection(FakeReader(b"R" * 576), rdc_writer)

    assert rdc_writer.closed is True
    assert state_mod.STATE.rdc_connected is False


def test_configure_keepalive_enables_keepalive_on_real_socket():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        proxy_mod.configure_keepalive(s, user_timeout_s=60)
        assert s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
    finally:
        s.close()


def test_configure_keepalive_tolerates_missing_or_fake_socket():
    proxy_mod.configure_keepalive(None)
    proxy_mod.configure_keepalive(object())  # no setsockopt — must not raise


class SilentReader:
    """A peer that is connected but never sends anything."""

    async def read(self, _n):
        await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_forward_and_tap_stops_when_source_goes_silent():
    dst = FakeWriter()
    await asyncio.wait_for(
        proxy_mod.forward_and_tap(SilentReader(), dst, None, label="t", idle_timeout=0.05),
        timeout=5,
    )
    assert bytes(dst.buf) == b""


@pytest.mark.asyncio
async def test_local_mode_ends_when_rdc_goes_silent(reset_handshake):
    """After the handshake, an RDC that stops sending must not hold the
    session open forever."""
    from rdc_proxy.config import CFG

    HANDSHAKE["cloud_greeting"] = b"G" * 576
    HANDSHAKE["rdc_response"] = b"R" * 576
    HANDSHAKE["config_msg"] = b"C" * 36
    state_mod.STATE.internet_up = False

    class HandshakeThenSilence(FakeReader):
        async def read(self, n):
            if self.pos >= len(self.data):
                await asyncio.sleep(3600)
            return await super().read(n)

    CFG["rdc_idle_timeout_s"] = 0.05
    try:
        await asyncio.wait_for(
            proxy_mod.local_mode(HandshakeThenSilence(b"R" * 576), FakeWriter()),
            timeout=5,
        )
    finally:
        CFG.pop("rdc_idle_timeout_s", None)


@pytest.mark.parametrize("ip,expected", [
    ("127.0.0.1", True),
    ("0.0.0.0", True),
    ("10.99.0.1", True),
    ("192.168.6.20", True),
    ("172.16.4.4", True),
    ("169.254.1.1", True),
    ("::1", True),
    ("", True),
    ("8.8.8.8", False),
    ("20.42.6.10", False),
])
def test_is_private_or_local(ip, expected):
    assert proxy_mod.is_private_or_local(ip) is expected


@pytest.mark.asyncio
async def test_redirected_connection_resolves_cloud_instead_of_dialing_itself(monkeypatch):
    """A connection that arrived via DNAT/REDIRECT has a private local
    address. The proxy must look the cloud up, not relay to that address."""
    import time

    class RedirectedWriter(FakeWriter):
        class _Sock:
            def getsockname(self):
                return ("10.99.0.1", 5253)

            def setsockopt(self, *_a):
                return

        def get_extra_info(self, name, *_a, **_kw):
            return self._Sock() if name == "socket" else None

    calls = []

    async def fake_proxy_mode(_r, _w, cloud_ip, cloud_port=None):
        calls.append((cloud_ip, cloud_port))

    monkeypatch.setattr(proxy_mod, "proxy_mode", fake_proxy_mode)
    monkeypatch.setattr(proxy_mod, "check_cloud_reachable", lambda: "20.42.6.10")
    state_mod.STATE.internet_up = True
    state_mod.STATE.internet_stable_since = time.time() - 10_000
    try:
        await proxy_mod.handle_rdc_connection(FakeReader(), RedirectedWriter())
    finally:
        state_mod.STATE.internet_up = False
        state_mod.STATE.internet_stable_since = None

    assert calls == [("20.42.6.10", None)]


@pytest.mark.asyncio
async def test_forward_and_tap_reports_socket_timeout_distinctly(capsys):
    """A keepalive / TCP_USER_TIMEOUT failure surfaces as TimeoutError with an
    errno; it must end the session and not be reported as our idle timer."""
    import errno

    class TimedOutReader:
        async def read(self, _n):
            raise TimeoutError(errno.ETIMEDOUT, "Connection timed out")

    await proxy_mod.forward_and_tap(TimedOutReader(), FakeWriter(), None, label="t")
    out = capsys.readouterr().out
    assert "connection timed out" in out
    assert "None" not in out
