from __future__ import annotations

import asyncio
import json
import logging
from json import JSONDecodeError
from typing import Any

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ...const import DEFAULT_FEISHU_TARGET_TYPE, FEISHU_TOKEN_URL

_LOGGER = logging.getLogger(__name__)
_TOKEN_URL = FEISHU_TOKEN_URL
_REPLY_MAX_LENGTH = 1800
_SEND_MESSAGE_URL = "https://open.feishu.cn/open-apis/im/v1/messages"


def import_lark() -> tuple[Any, Any]:
    import lark_oapi as lark
    import lark_oapi.ws.client as lark_ws_client
    return lark, lark_ws_client


_TOKEN_CACHE_TTL = 7000


class FeishuApiClient:
    def __init__(self, hass: HomeAssistant, app_id: str, app_secret: str) -> None:
        self._hass = hass
        self._app_id = app_id
        self._app_secret = app_secret
        self._session = async_get_clientsession(hass)
        self._cached_token: str | None = None
        self._token_expires_at: float = 0.0

    async def async_validate_connection(self) -> None:
        await self.async_get_tenant_access_token()

    async def async_get_tenant_access_token(self) -> str:
        import time
        now = time.monotonic()
        if self._cached_token and now < self._token_expires_at:
            return self._cached_token
        async with asyncio.timeout(15):
            response = await self._session.post(_TOKEN_URL, json={"app_id": self._app_id, "app_secret": self._app_secret})
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            raise RuntimeError(f"token request failed: {data.get('msg', response.reason)}")
        token = data.get("tenant_access_token")
        if not token:
            raise RuntimeError("token request succeeded but token missing")
        self._cached_token = token
        self._token_expires_at = now + _TOKEN_CACHE_TTL
        return token

    async def async_send_text_message(self, *, receive_id: str, text: str, receive_id_type: str = DEFAULT_FEISHU_TARGET_TYPE, at_user_ids: list[str] | None = None) -> None:
        elements: list[dict[str, Any]] = [{"tag": "md", "text": text[:_REPLY_MAX_LENGTH]}]
        if at_user_ids:
            for uid in at_user_ids:
                elements.append({"tag": "at", "user_id": uid})
        content = json.dumps(
            {"zh_cn": {"content": [elements]}},
            ensure_ascii=False,
        )
        await self._async_send_message(receive_id, "post", content, receive_id_type)

    async def async_send_image_message(self, *, receive_id: str, image_bytes: bytes, receive_id_type: str = DEFAULT_FEISHU_TARGET_TYPE) -> None:
        if not image_bytes:
            raise ValueError("Feishu image data is empty")
        image_key = await self.async_upload_image(image_bytes, strict=True)
        content = json.dumps({"image_key": image_key}, ensure_ascii=False)
        await self._async_send_message(receive_id, "image", content, receive_id_type)

    async def async_send_safe_reply(self, *, receive_id: str, text: str, receive_id_type: str) -> None:
        try:
            await self.async_send_text_message(receive_id=receive_id, text=text, receive_id_type=receive_id_type)
        except Exception as err:
            _LOGGER.warning("Failed to send message back to Feishu: %s", err)

    async def async_upload_image(self, image_data: bytes, *, strict: bool = False) -> str | None:
        token = await self.async_get_tenant_access_token()
        form = aiohttp.FormData()
        form.add_field("image_type", "message")
        form.add_field("image", image_data, filename="snapshot.jpg", content_type="image/jpeg")
        async with asyncio.timeout(30):
            response = await self._session.post(
                "https://open.feishu.cn/open-apis/im/v1/images",
                headers={"Authorization": f"Bearer {token}"},
                data=form,
            )
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            if strict:
                raise RuntimeError(f"upload image failed: {data.get('msg', response.reason)}")
            _LOGGER.warning("Failed to upload image to Feishu: %s", data.get("msg"))
            return None
        image_key = str((data.get("data") or {}).get("image_key") or "")
        if strict and not image_key:
            raise RuntimeError("upload image succeeded but image_key missing")
        return image_key

    async def async_upload_file(self, file_data: bytes, file_name: str, file_type: str = "stream") -> str:
        token = await self.async_get_tenant_access_token()
        form = aiohttp.FormData()
        form.add_field("file_type", file_type)
        form.add_field("file_name", file_name)
        form.add_field("file", file_data, filename=file_name, content_type="application/octet-stream")
        async with asyncio.timeout(60):
            response = await self._session.post(
                "https://open.feishu.cn/open-apis/im/v1/files",
                headers={"Authorization": f"Bearer {token}"},
                data=form,
            )
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            raise RuntimeError(f"upload file failed: {data.get('msg', response.reason)}")
        file_key = str((data.get("data") or {}).get("file_key") or "")
        if not file_key:
            raise RuntimeError("upload file succeeded but file_key missing")
        return file_key

    async def async_send_file_message(self, *, receive_id: str, file_bytes: bytes, file_name: str, receive_id_type: str = DEFAULT_FEISHU_TARGET_TYPE) -> None:
        if not file_bytes:
            raise ValueError("Feishu file data is empty")
        file_key = await self.async_upload_file(file_bytes, file_name)
        content = json.dumps({"file_key": file_key}, ensure_ascii=False)
        await self._async_send_message(receive_id, "file", content, receive_id_type)

    async def async_send_video_message(self, *, receive_id: str, video_bytes: bytes, file_name: str = "video.mp4", receive_id_type: str = DEFAULT_FEISHU_TARGET_TYPE) -> None:
        if not video_bytes:
            raise ValueError("Feishu video data is empty")
        image_key = ""
        file_key = await self.async_upload_file(video_bytes, file_name, file_type="mp4")
        content = json.dumps({"file_key": file_key, "image_key": image_key}, ensure_ascii=False)
        await self._async_send_message(receive_id, "media", content, receive_id_type)

    async def _async_send_message(self, receive_id: str, msg_type: str, content: str, receive_id_type: str) -> None:
        token = await self.async_get_tenant_access_token()
        async with asyncio.timeout(30):
            response = await self._session.post(
                _SEND_MESSAGE_URL,
                params={"receive_id_type": receive_id_type},
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
                json={"receive_id": receive_id, "msg_type": msg_type, "content": content},
            )
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            raise RuntimeError(f"send {msg_type} failed: {data.get('msg', response.reason)}")

    async def _async_send_message_with_id(
        self, receive_id: str, msg_type: str, content: str, receive_id_type: str
    ) -> str:
        token = await self.async_get_tenant_access_token()
        async with asyncio.timeout(30):
            response = await self._session.post(
                _SEND_MESSAGE_URL,
                params={"receive_id_type": receive_id_type},
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
                json={"receive_id": receive_id, "msg_type": msg_type, "content": content},
            )
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            raise RuntimeError(f"send {msg_type} failed: {data.get('msg', response.reason)}")
        message_id = str((data.get("data") or {}).get("message_id") or "")
        if not message_id:
            raise RuntimeError("send succeeded but message_id missing")
        return message_id

    async def async_send_progress_message(
        self,
        *,
        receive_id: str,
        text: str,
        receive_id_type: str = DEFAULT_FEISHU_TARGET_TYPE,
    ) -> str:
        content = json.dumps({"text": text[:_REPLY_MAX_LENGTH]}, ensure_ascii=False)
        return await self._async_send_message_with_id(receive_id, "text", content, receive_id_type)

    async def async_update_text_message(self, *, message_id: str, text: str) -> None:
        token = await self.async_get_tenant_access_token()
        content = json.dumps({"text": text[:_REPLY_MAX_LENGTH]}, ensure_ascii=False)
        async with asyncio.timeout(15):
            response = await self._session.patch(
                f"https://open.feishu.cn/open-apis/im/v1/messages/{message_id}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json; charset=utf-8",
                },
                json={"msg_type": "text", "content": content},
            )
        data = await async_read_json(response)
        if response.status != 200 or data.get("code") != 0:
            raise RuntimeError(f"update text failed: {data.get('msg', response.reason)}")


async def async_read_json(response: aiohttp.ClientResponse) -> dict[str, Any]:
    try:
        data = await response.json(content_type=None)
    except (aiohttp.ContentTypeError, JSONDecodeError) as err:
        body = await response.text()
        data = parse_json_from_text(body)
        if data is None:
            raise RuntimeError(f"invalid json response status={response.status}") from err
    if not isinstance(data, dict):
        raise RuntimeError(f"invalid response payload type: {type(data).__name__}")
    return data


def parse_json_from_text(body: str) -> dict[str, Any] | None:
    body = body.strip()
    if not body:
        return None
    try:
        data = json.loads(body)
        return data if isinstance(data, dict) else None
    except JSONDecodeError:
        pass
    start, end = body.find("{"), body.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        data = json.loads(body[start : end + 1])
    except JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None
