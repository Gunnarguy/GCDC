from __future__ import annotations

import os
import socket
import subprocess
import sys
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


DEFAULT_EXPLORER_PORTS: tuple[int, ...] = (8506,)


def _can_bind(address_family: int, host: str, port: int) -> bool:
    with closing(socket.socket(address_family, socket.SOCK_STREAM)) as sock:
        if address_family == socket.AF_INET6:
            try:
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            except OSError:
                pass
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def _is_port_available(port: int) -> bool:
    if not _can_bind(socket.AF_INET, "0.0.0.0", port):
        return False
    if socket.has_ipv6 and not _can_bind(socket.AF_INET6, "::", port):
        return False
    return True


def _has_http_listener(port: int) -> bool:
    try:
        with urlopen(f"http://127.0.0.1:{port}", timeout=2) as response:
            return response.status < 500
    except HTTPError as error:
        return error.code < 500
    except URLError:
        return False


def resolve_explorer_port(
    requested_port: int, max_attempts: int = 20
) -> tuple[int, bool]:
    if _is_port_available(requested_port):
        return requested_port, False

    if _has_http_listener(requested_port):
        return requested_port, True

    for candidate in range(requested_port + 1, requested_port + max_attempts + 1):
        if _is_port_available(candidate):
            print(
                f"Port {requested_port} is busy. Launching GrandChase Atlas on http://localhost:{candidate} instead."
            )
            return candidate, False

    raise RuntimeError(
        f"Unable to find an open port starting at {requested_port}. Try a different --port value."
    )


def resolve_preferred_explorer_ports(
    requested_ports: Sequence[int],
) -> tuple[int, bool]:
    candidates: list[int] = []
    seen_ports: set[int] = set()
    for raw_port in requested_ports:
        port = int(raw_port)
        if port in seen_ports:
            continue
        seen_ports.add(port)
        candidates.append(port)

    if not candidates:
        raise RuntimeError("No preferred explorer ports were configured.")

    for candidate in candidates:
        if _is_port_available(candidate):
            if candidate != candidates[0]:
                print(
                    "Configured explorer port "
                    f"{candidates[0]} is busy. Launching GrandChase Atlas on "
                    f"http://localhost:{candidate} from the configured port pool instead."
                )
            return candidate, False
        if _has_http_listener(candidate):
            return candidate, True

    preferred_text = ", ".join(str(port) for port in candidates)
    raise RuntimeError(
        "Unable to find an open configured explorer port. "
        f"Checked: {preferred_text}. Stop the conflicting service or update config/config.json."
    )


def launch_explorer(
    port: int | None = None,
    headless: bool = False,
    preferred_ports: Sequence[int] | None = None,
) -> dict[str, object]:
    app_path = Path(__file__).with_name("explorer_app.py")
    if port is None:
        resolved_port, reused_existing = resolve_preferred_explorer_ports(
            preferred_ports or DEFAULT_EXPLORER_PORTS
        )
    else:
        resolved_port, reused_existing = resolve_explorer_port(port)
    if reused_existing:
        print(
            f"GrandChase Atlas is already running at http://localhost:{resolved_port}"
        )
        return {
            "headless": headless,
            "port": resolved_port,
            "reused_existing": True,
            "url": f"http://localhost:{resolved_port}",
        }

    environment = {
        **os.environ,
        "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
        "STREAMLIT_SERVER_FILE_WATCHER_TYPE": os.environ.get(
            "STREAMLIT_SERVER_FILE_WATCHER_TYPE", "none"
        ),
        "STREAMLIT_SERVER_RUN_ON_SAVE": os.environ.get(
            "STREAMLIT_SERVER_RUN_ON_SAVE", "false"
        ),
    }
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.port",
        str(resolved_port),
        "--server.headless",
        "true" if headless else "false",
        "--browser.gatherUsageStats",
        "false",
    ]
    subprocess.run(command, check=True, env=environment)
    return {
        "headless": headless,
        "port": resolved_port,
        "reused_existing": False,
        "url": f"http://localhost:{resolved_port}",
    }
