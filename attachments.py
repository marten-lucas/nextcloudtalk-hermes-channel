from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)


class NextcloudAttachmentManager:
    """Verwaltet Metadaten-Extraktion und Downloads von Dateien aus Chat-Nachrichten."""

    def __init__(self, client: Any, tmp_dir: str = ""):
        self.client = client
        self.tmp_dir = tmp_dir or tempfile.gettempdir()

    def extract_attachments(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        direct = event.get("attachments") or event.get("files") or []
        attachments: List[Dict[str, Any]] = []
        if isinstance(direct, list):
            for item in direct:
                if isinstance(item, dict):
                    attachments.append(item)

        message_parameters = event.get("messageParameters") or event.get("parameters") or {}
        if isinstance(message_parameters, dict):
            for param in message_parameters.values():
                if not isinstance(param, dict) or str(param.get("type") or "").lower() != "file":
                    continue
                attachment: Dict[str, Any] = {}
                if param.get("id") is not None:
                    attachment["id"] = param.get("id")
                if param.get("path"):
                    attachment["path"] = param.get("path")
                if param.get("link"):
                    attachment["url"] = param.get("link")
                if attachment:
                    attachments.append(attachment)
        return attachments

    async def download_attachment(
        self,
        file_id: Optional[str] = None,
        remote_path: Optional[str] = None,
        file_url: Optional[str] = None,
    ) -> Optional[str]:
        session = await self.client.ensure_session()
        url = file_url
        if not url and remote_path:
            quoted_path = quote(remote_path.lstrip("/"))
            url = f"{self.client.base_url}/remote.php/dav/files/{quote(self.client.username)}/{quoted_path}"
        if not url and file_id:
            url = self.client.talk_url(f"apps/spreed/api/v1/chat/{file_id}")
        if not url:
            return None

        Path(self.tmp_dir).mkdir(parents=True, exist_ok=True)
        suffix = Path(str(remote_path or file_id or "attachment")).suffix
        with tempfile.NamedTemporaryFile(delete=False, dir=self.tmp_dir, suffix=suffix) as tmp:
            tmp_path = tmp.name

        try:
            async with session.get(url, headers=self.client.ocs_headers()) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"Attachment download failed with status {resp.status}")
                with open(tmp_path, "wb") as out:
                    out.write(await resp.read())
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
        return tmp_path