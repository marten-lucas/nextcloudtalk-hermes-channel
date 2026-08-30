from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger(__name__)


class NextcloudOCSException(Exception):
    """Custom exception for Nextcloud OCS API errors containing status code, message, and path."""

    def __init__(self, status_code: int, message: str, path: str):
        self.status_code = status_code
        self.message = message
        self.path = path
        super().__init__(f"Nextcloud OCS request failed for {path}: {status_code} {message}")


class NextcloudTalkClient:
    """Reiner HTTP-Client für Nextcloud OCS REST APIs."""

    def __init__(self, base_url: str, username: str, app_password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.app_password = app_password
        self._session: Optional[aiohttp.ClientSession] = None

    def auth_header(self) -> str:
        return aiohttp.encode_basic_auth(self.username, self.app_password)

    def ocs_headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.auth_header(),
            "OCS-APIRequest": "true",
            "Accept": "application/json",
        }

    async def ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def talk_url(self, path: str) -> str:
        return urljoin(f"{self.base_url}/ocs/v2.php/", path.lstrip("/"))

    def cloud_ocs_url(self, path: str) -> str:
        return urljoin(f"{self.base_url}/ocs/v1.php/cloud/", path.lstrip("/"))

    async def ocs_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        session = await self.ensure_session()
        query = {"format": "json"}
        if params:
            query.update(params)
        async with session.get(self.talk_url(path), params=query, headers=self.ocs_headers()) as resp:
            if resp.status == 304:
                return []
            body = await resp.json()
        self._raise_for_ocs_error(path, body)
        return body.get("ocs", {}).get("data")

    async def cloud_ocs_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        session = await self.ensure_session()
        query = {"format": "json"}
        if params:
            query.update(params)
        async with session.get(self.cloud_ocs_url(path), params=query, headers=self.ocs_headers()) as resp:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                body = await resp.json()
            else:
                body = {"ocs": {"meta": {"status": "ok", "statuscode": resp.status}, "data": {}}}
        self._raise_for_ocs_error(path, body)
        return body

    async def ocs_post(self, path: str, data: Dict[str, Any]) -> Any:
        return await self._ocs_request("post", path, data=data)

    async def ocs_put(self, path: str, data: Dict[str, Any]) -> Any:
        return await self._ocs_request("put", path, data=data)

    async def ocs_delete(self, path: str) -> Any:
        return await self._ocs_request("delete", path)

    async def _ocs_request(
        self,
        method: str,
        path: str,
        *,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        session = await self.ensure_session()
        query = {"format": "json"}
        if params:
            query.update(params)
        method_lower = method.lower()
        request_fn = getattr(session, method_lower)
        async with request_fn(self.talk_url(path), params=query, data=data, headers=self.ocs_headers()) as resp:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                body = await resp.json()
            else:
                raw_text = await resp.text()
                body = {"ocs": {"meta": {"status": "ok", "statuscode": resp.status}, "data": raw_text}}
        self._raise_for_ocs_error(path, body)
        return body.get("ocs", {}).get("data")

    @staticmethod
    def _raise_for_ocs_error(path: str, body: Dict[str, Any]) -> None:
        meta = body.get("ocs", {}).get("meta", {})
        status = str(meta.get("status", "ok")).lower()
        status_code = int(meta.get("statuscode", 100))
        if status != "ok" or status_code >= 400:
            message = meta.get("message", "unknown OCS error")
            raise NextcloudOCSException(status_code=status_code, message=message, path=path)
