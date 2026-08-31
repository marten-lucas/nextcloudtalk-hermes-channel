from __future__ import annotations

"""Outbound-Kategorisierung für Gateway-Nachrichten.

Stellt die Loop-Prävention wieder her: Interne Gateway-Meldungen
(Lifecycle, Fehler, Fortschritts-Rauschen) werden kategorisiert und
von `NextcloudTalkPlatform.send_message` entsprechend geroutet,
statt ungefiltert in den Chat geschrieben zu werden.
"""

from typing import Any, Dict, Tuple

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
]


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
    if "gateway online" in normalized:
        return ("lifecycle", {"state": "online", "text": text})
    if "draining" in normalized and "active" in normalized and "agent" in normalized:
        return ("lifecycle", {"state": "draining", "text": text})

    if text.strip().startswith("⚠️"):
        return ("error", {"text": text})

    if any(pattern in normalized for pattern in _ERROR_PATTERNS):
        return ("error", {"text": text})

    if any(pattern in normalized for pattern in _SUPPRESS_PATTERNS):
        return ("suppress", {})

    return ("forward", {"text": text})
