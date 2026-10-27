"""Process-wide turn admission shared by Hermes platform plugins.

Both Nextcloud adapters are loaded as separate plugins but run in the same
Gateway interpreter. The registry lives in ``builtins`` so module copies share
atomic admission and turn ownership without an import-time plugin dependency.
"""
from __future__ import annotations

import asyncio
import builtins
import threading
from typing import Dict, Tuple

_STATE_ATTR = "_hermes_platform_turns_v1"
_LOCK_ATTR = "_hermes_platform_turns_lock_v1"
_LIMIT_ATTR = "_hermes_platform_turn_limit_v1"
_CONNECTIONS_ATTR = "_hermes_platform_connections_v1"
_PRESENCE_TURNS_ATTR = "_hermes_platform_presence_turns_v1"
_PRESENCE_STATUSES_ATTR = "_hermes_platform_presence_statuses_v1"


def _shared_state() -> tuple[Dict[Tuple[str, str], int], threading.RLock]:
    namespace = builtins.__dict__
    lock = namespace.setdefault(_LOCK_ATTR, threading.RLock())
    with lock:
        state = namespace.setdefault(_STATE_ATTR, {})
        if not isinstance(state, dict):
            state = {}
            namespace[_STATE_ATTR] = state
    return state, lock


def configure_platform_turn_limit(limit: int) -> None:
    """Set the process-wide maximum for admitted model turns; zero is unlimited."""
    try:
        normalized = max(0, int(limit))
    except (TypeError, ValueError):
        normalized = 0
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        namespace[_LIMIT_ATTR] = normalized


def platform_turn_limit() -> int:
    """Return the current shared limit, or zero when no platform configured one."""
    _state, lock = _shared_state()
    with lock:
        try:
            return max(0, int(builtins.__dict__.get(_LIMIT_ATTR, 0)))
        except (TypeError, ValueError):
            return 0


def try_acquire_platform_turn(platform: str, token: str) -> bool:
    """Atomically reserve one global turn slot, without waiting."""
    key = (str(platform or "").strip().lower(), str(token or "").strip())
    if not all(key):
        return False
    state, lock = _shared_state()
    with lock:
        # Repeated start hooks for the same event are idempotent.
        if key in state:
            return True
        try:
            limit = max(0, int(builtins.__dict__.get(_LIMIT_ATTR, 0)))
        except (TypeError, ValueError):
            limit = 0
        if limit and sum(int(value) for value in state.values()) >= limit:
            return False
        state[key] = 1
        return True


async def acquire_platform_turn(platform: str, token: str) -> None:
    """Wait cancellation-safely until this turn owns a shared slot.

    Admission and reservation are one locked operation. The short async retry
    avoids blocking the Gateway event loop while allowing releases from any
    plugin copy/thread to wake queued turns without loop-bound shared locks.
    """
    while not try_acquire_platform_turn(platform, token):
        await asyncio.sleep(0.05)


def start_platform_turn(platform: str, token: str) -> None:
    """Compatibility registration for legacy callers; new code should acquire."""
    key = (str(platform or "").strip().lower(), str(token or "").strip())
    if not all(key):
        return
    state, lock = _shared_state()
    with lock:
        state[key] = int(state.get(key, 0)) + 1


def finish_platform_turn(platform: str, token: str) -> None:
    """Release one reference to a platform turn; duplicate finishes are harmless."""
    key = (str(platform or "").strip().lower(), str(token or "").strip())
    if not all(key):
        return
    state, lock = _shared_state()
    with lock:
        count = int(state.get(key, 0))
        if count <= 1:
            state.pop(key, None)
        else:
            state[key] = count - 1


def presence_scope(base_url: str, username: str) -> str:
    return f"{str(base_url or '').strip().rstrip('/').lower()}|{str(username or '').strip().casefold()}"


def register_presence_turn(account: str, platform: str, token: str) -> None:
    key = (str(account or "").strip().lower(), str(platform or "").strip().lower(), str(token or "").strip())
    if not all(key):
        return
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        namespace.setdefault(_PRESENCE_TURNS_ATTR, set()).add(key)


def unregister_presence_turn(account: str, platform: str, token: str) -> None:
    key = (str(account or "").strip().lower(), str(platform or "").strip().lower(), str(token or "").strip())
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        namespace.setdefault(_PRESENCE_TURNS_ATTR, set()).discard(key)
        namespace.setdefault(_PRESENCE_STATUSES_ATTR, {}).pop(key, None)


def active_presence_turns(account: str) -> int:
    account_key = str(account or "").strip().lower()
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        return sum(1 for scope, _platform, _token in namespace.setdefault(_PRESENCE_TURNS_ATTR, set()) if scope == account_key)


def set_presence_turn_status(account: str, platform: str, token: str, message: str, icon: str | None) -> None:
    key = (str(account or "").strip().lower(), str(platform or "").strip().lower(), str(token or "").strip())
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        turns = namespace.setdefault(_PRESENCE_TURNS_ATTR, set())
        if key in turns:
            statuses = namespace.setdefault(_PRESENCE_STATUSES_ATTR, {})
            statuses.pop(key, None)
            statuses[key] = (str(message or ""), icon)


def active_presence_status(account: str) -> tuple[str, str | None] | None:
    account_key = str(account or "").strip().lower()
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        turns = namespace.setdefault(_PRESENCE_TURNS_ATTR, set())
        statuses = namespace.setdefault(_PRESENCE_STATUSES_ATTR, {})
        for key, status in reversed(list(statuses.items())):
            if key[0] == account_key and key in turns:
                return status
    return None


def register_platform_connection(platform: str, token: str, account: str = "") -> None:
    key = (str(platform or "").strip().lower(), str(token or "").strip(), str(account or "").strip().lower())
    if not all(key):
        return
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        connections = namespace.setdefault(_CONNECTIONS_ATTR, set())
        connections.add(key)


def unregister_platform_connection(platform: str, token: str, account: str = "") -> None:
    key = (str(platform or "").strip().lower(), str(token or "").strip(), str(account or "").strip().lower())
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    with lock:
        connections = namespace.setdefault(_CONNECTIONS_ATTR, set())
        connections.discard(key)


def active_platform_connections(exclude_platform: str | None = None, account: str | None = None) -> int:
    namespace = builtins.__dict__
    _state, lock = _shared_state()
    excluded = str(exclude_platform or "").strip().lower()
    with lock:
        connections = namespace.setdefault(_CONNECTIONS_ATTR, set())
        account_key = str(account or "").strip().lower()
        return sum(
            1 for platform, _token, scope in connections
            if (not excluded or platform != excluded) and (not account_key or scope == account_key)
        )


def active_platform_turns(exclude_platform: str | None = None) -> int:
    state, lock = _shared_state()
    excluded = str(exclude_platform or "").strip().lower()
    with lock:
        return sum(
            int(count)
            for (platform, _token), count in state.items()
            if not excluded or platform != excluded
        )
