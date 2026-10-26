"""Process-wide in-flight markers shared by Hermes platform plugins.

The Deck and Talk adapters run in one Gateway process but own separate adapter
instances. A small builtins-backed registry lets them coordinate without
coupling their runtime modules or persisting stale turns across restarts.
"""
from __future__ import annotations

import builtins
import threading
from typing import Dict, Tuple

_STATE_ATTR = "_hermes_platform_turns_v1"
_LOCK_ATTR = "_hermes_platform_turns_lock_v1"


def _shared_state() -> tuple[Dict[Tuple[str, str], int], threading.RLock]:
    namespace = builtins.__dict__
    lock = namespace.setdefault(_LOCK_ATTR, threading.RLock())
    with lock:
        state = namespace.setdefault(_STATE_ATTR, {})
        if not isinstance(state, dict):
            state = {}
            namespace[_STATE_ATTR] = state
    return state, lock


def start_platform_turn(platform: str, token: str) -> None:
    key = (str(platform or "").strip().lower(), str(token or "").strip())
    if not all(key):
        return
    state, lock = _shared_state()
    with lock:
        state[key] = int(state.get(key, 0)) + 1


def finish_platform_turn(platform: str, token: str) -> None:
    key = (str(platform or "").strip().lower(), str(token or "").strip())
    state, lock = _shared_state()
    with lock:
        count = int(state.get(key, 0))
        if count <= 1:
            state.pop(key, None)
        else:
            state[key] = count - 1


def active_platform_turns(exclude_platform: str | None = None) -> int:
    state, lock = _shared_state()
    excluded = str(exclude_platform or "").strip().lower()
    with lock:
        return sum(
            int(count)
            for (platform, _token), count in state.items()
            if not excluded or platform != excluded
        )
