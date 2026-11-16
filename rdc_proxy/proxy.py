"""Transparent TCP proxy + internet/cloud monitoring for rdc-proxy.

Three modes (decided per RDC connection):
- PROXY: pass-through to real cloud, capture handshake, tap telemetry into STATE
- LOCAL: replay captured handshake, ACK telemetry into STATE (no cloud)
- WAITING: no internet + no handshake — hold the RDC connection until internet
"""

import asyncio
import ipaddress
import socket
import struct
import time

from rdc_proxy.config import CFG
from rdc_proxy.state import HANDSHAKE, STATE, have_handshake, save_handshake


# ── Internet & cloud reachability ──────────────────────────────────────────

def check_internet():
    try:
        socket.setdefaulttimeout(3)
        socket.create_connection(("8.8.8.8", 53), timeout=3).close()
        return True
    except OSError:
        return False


def resolve_cloud():
    try:
        results = socket.getaddrinfo(
            CFG["cloud_dns"], CFG["cloud_port"], socket.AF_INET, socket.SOCK_STREAM
        )
        return [r[4][0] for r in results]
    except socket.gaierror:
        return []


def check_cloud_reachable():
    for ip in resolve_cloud()[:3]:
        try:
            s = socket.create_connection((ip, CFG["cloud_port"]), timeout=5)
            s.close()
            return ip
        except OSError:
            continue
    return None


async def internet_monitor():
    interval = CFG.get("internet_check_interval_s", 30)
    loop = asyncio.get_running_loop()
    while True:
        # Run blocking probes in a thread so we don't stall handle_rdc_connection.
        up = await loop.run_in_executor(None, check_internet)
        STATE.internet_up = up
        if up:
            if STATE.internet_stable_since is None:
                STATE.internet_stable_since = time.time()
                print("[internet] connection detected, starting stability timer", flush=True)
            cloud_ip = await loop.run_in_executor(None, check_cloud_reachable)
            STATE.set_cloud_check_result(cloud_ip)
        else:
            if STATE.internet_stable_since is not None:
                print("[internet] connection lost, resetting stability timer", flush=True)
            STATE.internet_stable_since = None
            STATE.set_cloud_check_result(None)
        await asyncio.sleep(interval)


# ── TCP proxy primitives ───────────────────────────────────────────────────

def abort_connection(writer):
    """Close our side of a connection with a TCP RST instead of a FIN.

    The RDC does not reliably reconnect after a graceful close (FIN): it can
    sit half-open indefinitely. A reset makes it notice at once and dial
    again. SO_LINGER with a zero timeout turns the close into a reset;
    transport.abort() then closes without waiting to flush.
    """
    if writer is None:
        return
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
    except (OSError, AttributeError):
        pass  # already closed, or not a real socket
    transport = getattr(writer, "transport", None)
    try:
        if transport is not None:
            transport.abort()
        else:
            writer.close()
    except Exception:
        pass


def configure_keepalive(sock, user_timeout_s=None):
    """Turn on TCP keepalive so a silently dead peer is noticed in about a
    minute instead of never.

    Without this, a path that goes dark without closing the connection (a
    router reboot that forgets its NAT state, a pulled cable) leaves the
    session open forever: nothing is sent, so nothing fails.

    user_timeout_s additionally bounds how long *sent* data may stay
    unacknowledged (Linux TCP_USER_TIMEOUT) — otherwise the kernel retransmits
    for many minutes before giving up. Options the platform lacks are skipped.
    """
    if sock is None:
        return
    idle = int(CFG.get("tcp_keepalive_idle_s", 30))
    interval = int(CFG.get("tcp_keepalive_interval_s", 10))
    count = int(CFG.get("tcp_keepalive_count", 3))
    options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    # Linux calls the idle time TCP_KEEPIDLE; macOS calls it TCP_KEEPALIVE.
    idle_opt = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    if idle_opt is not None:
        options.append((socket.IPPROTO_TCP, idle_opt, idle))
    if hasattr(socket, "TCP_KEEPINTVL"):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval))
    if hasattr(socket, "TCP_KEEPCNT"):
        options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count))
    if user_timeout_s and hasattr(socket, "TCP_USER_TIMEOUT"):
        options.append((socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, int(user_timeout_s * 1000)))
    for level, opt, value in options:
        try:
            sock.setsockopt(level, opt, value)
        except (OSError, AttributeError):
            pass


async def read_exactly(reader, n, timeout=30):
    data = b""
    while len(data) < n:
        try:
            chunk = await asyncio.wait_for(reader.read(n - len(data)), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if not chunk:
            return None
        data += chunk
    return data


def _timeout_reason(exc, idle_timeout):
    """Say which timeout fired. On Python 3.11+ asyncio.TimeoutError is the
    builtin TimeoutError, which is also what a socket raises when keepalive or
    TCP_USER_TIMEOUT gives up on the peer (errno ETIMEDOUT) — a different event
    from our own idle timer expiring, which carries no errno."""
    if getattr(exc, "errno", None):
        return "connection timed out (peer stopped responding)"
    return f"no data for {idle_timeout}s"


async def forward_and_tap(src_reader, dst_writer, tap_fn, label="", idle_timeout=None):
    """Copy src -> dst until EOF or error. If idle_timeout is set, also stop
    when the source has sent nothing for that many seconds."""
    try:
        while True:
            try:
                data = await asyncio.wait_for(src_reader.read(8192), timeout=idle_timeout or None)
            except asyncio.TimeoutError as e:
                print(f"[forward:{label}] {_timeout_reason(e, idle_timeout)} — treating session as dead", flush=True)
                break
            if not data:
                break
            if tap_fn:
                tap_fn(data)
            dst_writer.write(data)
            await dst_writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    except Exception as e:
        print(f"[forward:{label}] error: {e}", flush=True)


# ── Connection lifecycle / mode dispatch ───────────────────────────────────

def is_private_or_local(ip):
    """True when ip cannot be the real cloud: loopback, unspecified,
    link-local or private (RFC 1918) space.

    With TPROXY the accepted socket's local address is the cloud address the
    RDC dialed, and we relay to exactly that. When the RDC reaches us some
    other way — DNS override, DNAT/REDIRECT, a port-forward — the local address
    is one of our own, and relaying to it would connect the proxy to itself.
    """
    if not ip:
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_unspecified


async def _find_cloud():
    """check_cloud_reachable() does blocking DNS + connects; keep it off the
    event loop so telemetry keeps flowing while it runs."""
    return await asyncio.get_running_loop().run_in_executor(None, check_cloud_reachable)


async def handle_rdc_connection(rdc_reader, rdc_writer):
    peer = rdc_writer.get_extra_info("peername")
    sock = rdc_writer.get_extra_info("socket")

    orig_dst = None
    if sock:
        try:
            orig_dst = sock.getsockname()
            if is_private_or_local(orig_dst[0]):
                orig_dst = None  # not TPROXY — we resolve the cloud ourselves
        except Exception:
            pass

    print(
        f"[proxy] RDC connected from {peer}"
        + (f" (orig dst {orig_dst[0]}:{orig_dst[1]})" if orig_dst else ""),
        flush=True,
    )
    STATE.rdc_connected = True
    configure_keepalive(sock)

    try:
        stable_threshold = CFG.get("internet_stable_before_proxy_s", 300)
        internet_stable = (
            STATE.internet_up
            and STATE.internet_stable_since is not None
            and (time.time() - STATE.internet_stable_since) >= stable_threshold
        )

        if internet_stable:
            cloud_ip = orig_dst[0] if orig_dst else await _find_cloud()
            cloud_port = orig_dst[1] if orig_dst else None
            if cloud_ip:
                await proxy_mode(rdc_reader, rdc_writer, cloud_ip, cloud_port)
                return

        if have_handshake():
            await local_mode(rdc_reader, rdc_writer)
        else:
            STATE.set_proxy_mode("waiting")
            print("[proxy] no handshake + no stable internet — WAITING mode", flush=True)
            while not STATE.internet_up:
                await asyncio.sleep(5)
            cloud_ip = await _find_cloud()
            if cloud_ip:
                await proxy_mode(rdc_reader, rdc_writer, cloud_ip)
            else:
                print("[proxy] cloud unreachable despite internet — closing", flush=True)
    finally:
        STATE.rdc_connected = False
        STATE.cloud_connected = False
        if CFG.get("rdc_reset_on_close", True):
            abort_connection(rdc_writer)
        else:
            rdc_writer.close()
        print("[proxy] RDC connection ended", flush=True)


async def proxy_mode(rdc_reader, rdc_writer, cloud_ip, cloud_port=None):
    """Bidirectional pass-through. Captures handshake on first run."""
    STATE.set_proxy_mode("proxy")
    STATE.cloud_ip = cloud_ip
    cp = cloud_port or CFG["cloud_port"]
    print(f"[proxy] PROXY mode — connecting to cloud {cloud_ip}:{cp}", flush=True)

    try:
        cloud_reader, cloud_writer = await asyncio.open_connection(cloud_ip, cp)
    except OSError as e:
        print(f"[proxy] cloud connect failed: {e}", flush=True)
        if have_handshake():
            await local_mode(rdc_reader, rdc_writer)
        return

    STATE.cloud_connected = True
    configure_keepalive(
        cloud_writer.get_extra_info("socket"),
        user_timeout_s=CFG.get("cloud_tcp_user_timeout_s", 60),
    )

    try:
        cloud_greeting = await read_exactly(cloud_reader, 576, timeout=30)
        if not cloud_greeting or len(cloud_greeting) != 576:
            print("[proxy] failed to read cloud greeting", flush=True)
            return
        rdc_writer.write(cloud_greeting)
        await rdc_writer.drain()

        rdc_response = await read_exactly(rdc_reader, 576, timeout=30)
        if not rdc_response or len(rdc_response) != 576:
            print("[proxy] failed to read RDC response", flush=True)
            return
        cloud_writer.write(rdc_response)
        await cloud_writer.drain()

        config_msg = await read_exactly(cloud_reader, 36, timeout=30)
        if not config_msg or len(config_msg) != 36:
            print("[proxy] failed to read cloud config", flush=True)
            return
        rdc_writer.write(config_msg)
        await rdc_writer.drain()

        if not have_handshake():
            HANDSHAKE["cloud_greeting"] = cloud_greeting
            HANDSHAKE["rdc_response"] = rdc_response
            HANDSHAKE["config_msg"] = config_msg
            save_handshake()
            print("[proxy] handshake captured and persisted!", flush=True)

        print("[proxy] handshake complete — forwarding data", flush=True)

        rdc_to_cloud = asyncio.create_task(
            # The RDC reports every few seconds even in standby, so a long
            # silence means the session is dead. The cloud side is legitimately
            # quiet, so it gets no idle timeout (keepalive covers it).
            forward_and_tap(
                rdc_reader, cloud_writer, STATE.ingest_buffer, "rdc->cloud",
                idle_timeout=CFG.get("rdc_idle_timeout_s", 120),
            )
        )
        cloud_to_rdc = asyncio.create_task(
            forward_and_tap(cloud_reader, rdc_writer, None, "cloud->rdc")
        )

        done, pending = await asyncio.wait(
            [rdc_to_cloud, cloud_to_rdc], return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except Exception as e:
        print(f"[proxy] proxy_mode error: {e}", flush=True)
    finally:
        STATE.cloud_connected = False
        try:
            cloud_writer.close()
            await cloud_writer.wait_closed()
        except Exception:
            pass
        if not STATE.internet_up and have_handshake():
            print("[proxy] internet lost during proxy — will serve locally on reconnect", flush=True)


async def local_mode(rdc_reader, rdc_writer):
    """Replay the captured cloud handshake; absorb telemetry."""
    STATE.set_proxy_mode("local")
    STATE.cloud_connected = False
    print("[proxy] LOCAL mode — serving as cloud", flush=True)

    try:
        rdc_writer.write(HANDSHAKE["cloud_greeting"])
        await rdc_writer.drain()

        rdc_response = await read_exactly(rdc_reader, 576, timeout=30)
        if not rdc_response:
            print("[proxy] RDC didn't respond to greeting", flush=True)
            return

        rdc_writer.write(HANDSHAKE["config_msg"])
        await rdc_writer.drain()
        print("[proxy] local handshake complete — ingesting telemetry", flush=True)

        stable_threshold = CFG.get("internet_stable_before_proxy_s", 300)
        idle_timeout = CFG.get("rdc_idle_timeout_s", 120) or None
        while True:
            try:
                data = await asyncio.wait_for(rdc_reader.read(8192), timeout=idle_timeout)
            except asyncio.TimeoutError as e:
                print(f"[proxy] RDC: {_timeout_reason(e, idle_timeout)} — ending local session", flush=True)
                break
            if not data:
                break
            STATE.ingest_buffer(data)
            if (
                STATE.internet_up
                and STATE.internet_stable_since
                and (time.time() - STATE.internet_stable_since) >= stable_threshold
            ):
                cloud_ip = await _find_cloud()
                if cloud_ip:
                    print("[proxy] internet stable + cloud reachable — terminating local session for proxy switchover", flush=True)
                    break

    except (ConnectionError, asyncio.CancelledError):
        pass
    except Exception as e:
        print(f"[proxy] local_mode error: {e}", flush=True)


# ── Server bootstrap ───────────────────────────────────────────────────────

async def start_server():
    proxy_addr = CFG.get("proxy_listen_addr", "0.0.0.0")
    proxy_port = CFG.get("proxy_port", 5253)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        IP_TRANSPARENT = 19
        sock.setsockopt(socket.SOL_IP, IP_TRANSPARENT, 1)
    except OSError:
        print("[proxy] WARNING: IP_TRANSPARENT not available — TPROXY won't work", flush=True)
    sock.bind((proxy_addr, proxy_port))
    sock.listen(32)
    sock.setblocking(False)

    server = await asyncio.start_server(handle_rdc_connection, sock=sock)
    print(f"[proxy] listening on {proxy_addr}:{proxy_port} (TPROXY)", flush=True)
    return server
