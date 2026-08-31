from __future__ import annotations

"""Outbound-Kategorisierung für Gateway-Nachrichten.

Stellt die Loop-Prävention wieder her: Interne Gateway-Meldungen
(Lifecycle, Fehler, Fortschritts-Rauschen) werden kategorisiert und
von `NextcloudTalkPlatform.send_message` entsprechend geroutet,
statt ungefiltert in den Chat geschrieben zu werden.
"""

from typing import Any, Dict, Tuple
import re

Category = str

_ERROR_PATTERNS = [
    "processing stopped",
    "no response was generated",
    "session too large",
    "interrupted before processing",
    "authentication failed",
    "provider.*failed",
    "provider.*rejected",
    "tool.*failed",
    "no response after",
]

_SUPPRESS_PATTERNS = [
    "gateway.*queued",
    "compressing context",
    "compression timed out",
    "compression aborted",
    "working —",
    "subagent working",
    "steer failed",
    # Upstream hermes-agent Noise-Statuses (_prepare_gateway_status_message):
    # Retry / Rate-Limit / Fallback
    "retrying in",
    "rate limited. waiting",
    "max retries",
    "trying fallback",
    # Compression-Preflight & -Varianten
    "preflight compression",
    "pre-api compression",
    "context reduced to",
    "skipping concurrent compression",
    "compression skipped",
    "compression lock",
    "title generation failed",
    "compression summary failed",
    "compression model",
    "recovered using main model",
    # Delivery-Ledger Redelivery-Marker
    "recovered reply",
    # Session-Stall-Watchdog
    "agent session appears stalled",
    "try /new to reset",
]

# Interne Turn-Marker (Memory-Plugin: _INTERNAL_GATEWAY_TURN_RE) und
# Silence-Narration (upstream filter_silence_narration) — Loop-Vektoren
# in Bot-zu-Bot-Kanälen.
_INTERNAL_MARKER_RE = re.compile(
    r"^\[(async|context|prior|your active task list|important|background)",
    re.IGNORECASE,
)

_SILENCE_NARRATION_RE = re.compile(
    r"^\W*(\(silent\)|🔇|🔇|…|\.{1,3}|silent|no response|no reply)\W*$",
    re.IGNORECASE,
)


def categorize_gateway_message(text: str) -> Tuple[Category, Dict[str, Any]]:
    """Kategorisiert eine ausgehende Nachricht.

    Returns:
        Tuple aus Kategorie und Details:
        - ``lifecycle``: Gateway-Statusänderung (offline/online/draining)
        - ``error``: Fehlermeldung (nur als Status bzw. formatierter Reply)
        - ``suppress``: internes Rauschen, still verwerfen
        - ``forward``: normale Chat-Nachricht
    """
    if not text:
        return ("forward", {"text": text})

    normalized = " ".join(text.split()).strip().lower()

    if "gateway restarting" in normalized:
        return ("lifecycle", {"state": "offline", "text": text})
    if "gateway shutting down" in normalized:
        return ("lifecycle", {"state": "offline", "text": text})
    if "gateway online" in normalized:
        return ("lifecycle", {"state": "online", "text": text})
    if "draining" in normalized and "active" in normalized and "agent" in normalized:
        return ("lifecycle", {"state": "draining", "text": text})

    # Silence-Narration & interne Turn-Marker: still verwerfen (Loop-Prävention)
    if _SILENCE_NARRATION_RE.match(text.strip()):
        return ("suppress", {})
    if _INTERNAL_MARKER_RE.match(text.strip()):
        return ("suppress", {})

    # ⚠️-Präfix: nur als Fehler werten, wenn es kein bekanntes Suppress-Muster ist
    # (z.B. "⚠️ Max retries … trying fallback", "⚠️ Agent session appears stalled").
    if text.strip().startswith("⚠️") and not any(
        pattern in normalized for pattern in _SUPPRESS_PATTERNS
    ):
        return ("error", {"text": text})

    if any(pattern in normalized for pattern in _ERROR_PATTERNS):
        return ("error", {"text": text})

    if any(pattern in normalized for pattern in _SUPPRESS_PATTERNS):
        return ("suppress", {})

    return ("forward", {"text": text})
