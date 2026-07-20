"""Tencent Weixin OpenClaw auth and API helpers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
from urllib.parse import quote
from dataclasses import dataclass
from io import BytesIO
from typing import Any
from uuid import uuid4

import aiohttp
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import segno
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ...const import WECHAT_CDN_BASE_URL, WECHAT_DEFAULT_BASE_URL

_LOGGER = logging.getLogger(__name__)

_QR_LONG_POLL_TIMEOUT_MS = 35_000
_DEFAULT_LONG_POLL_TIMEOUT_MS = 35_000
_DEFAULT_API_TIMEOUT_MS = 15_000
_DEFAULT_ILINK_BOT_TYPE = "3"
SESSION_EXPIRED_ERRCODE = -14
_WECHAT_CDN_BASE_URL = WECHAT_CDN_BASE_URL


@dataclass(slots=True)
class WeixinLoginSession:
    session_key: str
    qrcode: str
    qrcode_url: str
    qrcode_data_url: str


@dataclass(slots=True)
class WeixinLoginResult:
    connected: bool
    message: str
    token: str = ""
    account_id: str = ""
    base_url: str = ""
    user_id: str = ""
    already_connected: bool = False


def _random_wechat_uin() -> str:
    digest = hashlib.sha256(uuid4().bytes).digest()[:4]
    value = int.from_bytes(digest, "big", signed=False)
    return base64.b64encode(str(value).encode("utf-8")).decode("ascii")


def _build_headers(body: str, token: str | None = None) -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        "AuthorizationType": "ilink_bot_token",
        "Content-Length": str(len(body.encode("utf-8"))),
        "X-WECHAT-UIN": _random_wechat_uin(),
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _api_post(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    endpoint: str,
    payload: dict[str, Any],
    token: str | None = None,
    timeout_ms: int = _DEFAULT_API_TIMEOUT_MS,
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    body = json.dumps(payload, ensure_ascii=False)
    timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000)
    async with session.post(url, data=body.encode("utf-8"), headers=_build_headers(body, token), timeout=timeout) as resp:
        raw = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"{endpoint} {resp.status}: {raw}")
    data = json.loads(raw) if raw else {}
    if not isinstance(data, dict):
        raise ValueError(f"invalid {endpoint} response")
    return data


def _check_send_message_resp(resp: dict[str, Any]) -> None:
    """Validate SendMessage response, raise on non-zero ret.

    Mirrors openclaw-weixin 2.4.5: previously the response body was discarded
    and a non-zero ``ret`` (rate-limit, stale context_token, risk control, ...)
    caused the failure to be silently dropped.
    """
    ret = resp.get("ret")
    if isinstance(ret, int) and ret != 0:
        errmsg = resp.get("errmsg")
        raise RuntimeError(f"sendMessage ret={ret} errmsg={errmsg or '(none)'}")


async def async_start_weixin_login(
    hass: HomeAssistant,
    *,
    base_url: str = WECHAT_DEFAULT_BASE_URL,
    session_key: str | None = None,
    local_token_list: list[str],
) -> WeixinLoginSession:
    """Request a login QR code.

    Mirrors openclaw-weixin 2.3.1+ fetchQRCode: POST with ``local_token_list``
    of recently-bound bot tokens (max 10, newest first) so the server can
    recognize an already-bound bot and short-circuit with ``binded_redirect``
    in ``async_wait_weixin_login``.
    """
    session = async_get_clientsession(hass)
    url = f"{base_url.rstrip('/')}/ilink/bot/get_bot_qrcode?bot_type={_DEFAULT_ILINK_BOT_TYPE}"
    payload = {
        "local_token_list": list(local_token_list)[:10],
        "base_info": {"channel_version": "ha-cn-im-hub"},
    }
    body = json.dumps(payload, ensure_ascii=False)
    timeout = aiohttp.ClientTimeout(total=_DEFAULT_API_TIMEOUT_MS / 1000)
    async with session.post(url, data=body.encode("utf-8"), headers=_build_headers(body), timeout=timeout) as resp:
        raw = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"get_bot_qrcode {resp.status}: {raw}")
    data = json.loads(raw) if raw else {}
    qrcode = str(data.get("qrcode") or "")
    qrcode_url = str(data.get("qrcode_img_content") or "")
    if not qrcode or not qrcode_url:
        raise ValueError("failed to fetch login QR code")
    return WeixinLoginSession(
        session_key=session_key or uuid4().hex,
        qrcode=qrcode,
        qrcode_url=qrcode_url,
        qrcode_data_url=build_qr_data_url(qrcode_url),
    )


async def async_wait_weixin_login(
    hass: HomeAssistant,
    *,
    login: WeixinLoginSession,
    base_url: str = WECHAT_DEFAULT_BASE_URL,
    timeout_ms: int = 480_000,
) -> WeixinLoginResult:
    session = async_get_clientsession(hass)
    deadline = asyncio.get_running_loop().time() + max(timeout_ms / 1000, 1)
    while asyncio.get_running_loop().time() < deadline:
        url = f"{base_url.rstrip('/')}/ilink/bot/get_qrcode_status?qrcode={login.qrcode}"
        timeout = aiohttp.ClientTimeout(total=_QR_LONG_POLL_TIMEOUT_MS / 1000)
        try:
            async with session.get(url, headers={"iLink-App-ClientVersion": "1"}, timeout=timeout) as resp:
                raw = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"get_qrcode_status {resp.status}: {raw}")
            data = json.loads(raw) if raw else {}
        except TimeoutError:
            data = {"status": "wait"}
        status = str(data.get("status") or "wait")
        if status == "confirmed":
            account_id = str(data.get("ilink_bot_id") or "")
            token = str(data.get("bot_token") or "")
            user_id = str(data.get("ilink_user_id") or "")
            resolved_base_url = str(data.get("baseurl") or base_url)
            if not account_id or not token:
                raise ValueError("login confirmed but token/account id missing")
            return WeixinLoginResult(
                connected=True,
                message="与微信连接成功",
                token=token,
                account_id=account_id,
                base_url=resolved_base_url,
                user_id=user_id,
            )
        if status == "binded_redirect":
            return WeixinLoginResult(
                connected=False,
                already_connected=True,
                message="已连接过此 OpenClaw，无需重复连接。",
            )
        if status == "expired":
            raise ValueError("wechat login QR expired")
        await asyncio.sleep(2)
    raise TimeoutError("wechat login timeout")


async def async_get_updates(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    get_updates_buf: str,
    timeout_ms: int = _DEFAULT_LONG_POLL_TIMEOUT_MS,
) -> dict[str, Any]:
    session = async_get_clientsession(hass)
    try:
        return await _api_post(
            session,
            base_url=base_url,
            endpoint="ilink/bot/getupdates",
            payload={"get_updates_buf": get_updates_buf, "base_info": {"channel_version": "ha-cn-im-hub"}},
            token=token,
            timeout_ms=timeout_ms,
        )
    except TimeoutError:
        return {"ret": 0, "msgs": [], "get_updates_buf": get_updates_buf}


async def async_send_weixin_text(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    text: str,
) -> str:
    session = async_get_clientsession(hass)
    client_id = f"cn_im_hub_{uuid4().hex}"
    resp = await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/sendmessage",
        payload={
            "msg": {
                "from_user_id": "",
                "to_user_id": to_user_id,
                "client_id": client_id,
                "message_type": 2,
                "message_state": 2,
                "item_list": [{"type": 1, "text_item": {"text": text}}],
                "context_token": context_token,
            },
            "base_info": {"channel_version": "ha-cn-im-hub"},
        },
        token=token,
    )
    _check_send_message_resp(resp)
    return client_id


def _encrypt_aes_ecb(plaintext: bytes, key: bytes) -> bytes:
    padder = padding.PKCS7(128).padder()
    padded = padder.update(plaintext) + padder.finalize()
    cipher = Cipher(algorithms.AES(key), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(padded) + encryptor.finalize()


def _decrypt_aes_ecb(ciphertext: bytes, key: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(key), modes.ECB())
    decryptor = cipher.decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def _parse_aes_key(aes_key_b64: str) -> bytes:
    decoded = base64.b64decode(aes_key_b64)
    if len(decoded) == 16:
        return decoded
    if len(decoded) == 32:
        try:
            return bytes.fromhex(decoded.decode("ascii"))
        except (ValueError, UnicodeDecodeError):
            pass
    raise ValueError(f"Invalid aes_key: decoded length {len(decoded)}")


async def async_download_weixin_media(
    hass: HomeAssistant,
    *,
    encrypt_query_param: str | None = None,
    aes_key_b64: str | None = None,
    full_url: str | None = None,
    aeskey_hex: str | None = None,
) -> bytes:
    if aeskey_hex:
        key = bytes.fromhex(aeskey_hex)
    elif aes_key_b64:
        key = _parse_aes_key(aes_key_b64)
    else:
        raise ValueError("No AES key provided")
    url = full_url
    if not url:
        if not encrypt_query_param:
            raise ValueError("No download URL")
        url = f"{_WECHAT_CDN_BASE_URL}/download?encrypted_query_param={quote(encrypt_query_param, safe='')}"
    session = async_get_clientsession(hass)
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"CDN download {resp.status}")
        encrypted = await resp.read()
    return _decrypt_aes_ecb(encrypted, key)


@dataclass(slots=True)
class _UploadedMedia:
    encrypt_query_param: str
    aes_key_hex: str
    ciphertext_size: int
    plaintext_size: int


async def _async_upload_to_wechat_cdn(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    media_bytes: bytes,
    media_type: int,
) -> _UploadedMedia:
    """Generic CDN upload pipeline: encrypt → getuploadurl → POST CDN.

    Mirrors openclaw-weixin upload.ts uploadMediaToCdn().
    media_type: 1=IMAGE, 2=VIDEO, 3=FILE, 4=VOICE.
    """
    filekey = uuid4().hex
    aes_key = uuid4().bytes
    aes_key_hex = aes_key.hex()
    ciphertext = _encrypt_aes_ecb(media_bytes, aes_key)
    upload_data = await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/getuploadurl",
        payload={
            "filekey": filekey,
            "media_type": media_type,
            "to_user_id": to_user_id,
            "rawsize": len(media_bytes),
            "rawfilemd5": hashlib.md5(media_bytes).hexdigest(),
            "filesize": len(ciphertext),
            "no_need_thumb": True,
            "aeskey": aes_key_hex,
            "base_info": {"channel_version": "ha-cn-im-hub"},
        },
        token=token,
    )
    upload_param = str(upload_data.get("upload_param") or "")
    upload_full_url = str(upload_data.get("upload_full_url") or "").strip()
    if upload_full_url:
        upload_url = upload_full_url
    elif upload_param:
        upload_url = (
            f"{_WECHAT_CDN_BASE_URL}/upload"
            f"?encrypted_query_param={quote(upload_param, safe='')}"
            f"&filekey={quote(filekey, safe='')}"
        )
    else:
        raise ValueError(f"Weixin getuploadurl returned no usable upload url: {upload_data}")
    async with session.post(
        upload_url,
        data=ciphertext,
        headers={"Content-Type": "application/octet-stream"},
        timeout=aiohttp.ClientTimeout(total=120),
    ) as resp:
        if resp.status >= 400:
            raw = await resp.text()
            raise RuntimeError(f"wechat cdn upload {resp.status}: {raw}")
        encrypt_query_param = str(resp.headers.get("x-encrypted-param") or "")
    if not encrypt_query_param:
        raise ValueError("Weixin CDN upload missing x-encrypted-param")
    return _UploadedMedia(
        encrypt_query_param=encrypt_query_param,
        aes_key_hex=aes_key_hex,
        ciphertext_size=len(ciphertext),
        plaintext_size=len(media_bytes),
    )


def _build_cdn_media(uploaded: _UploadedMedia) -> dict[str, Any]:
    """Build CDNMedia dict matching openclaw-weixin send.ts."""
    return {
        "encrypt_query_param": uploaded.encrypt_query_param,
        "aes_key": base64.b64encode(uploaded.aes_key_hex.encode("utf-8")).decode("ascii"),
        "encrypt_type": 1,
    }


async def _async_send_message_with_item(
    session: aiohttp.ClientSession,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    item: dict[str, Any],
) -> str:
    client_id = f"cn_im_hub_{uuid4().hex}"
    resp = await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/sendmessage",
        payload={
            "msg": {
                "from_user_id": "",
                "to_user_id": to_user_id,
                "client_id": client_id,
                "message_type": 2,
                "message_state": 2,
                "item_list": [item],
                "context_token": context_token,
            },
            "base_info": {"channel_version": "ha-cn-im-hub"},
        },
        token=token,
    )
    _check_send_message_resp(resp)
    return client_id


async def async_send_weixin_image(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    image_bytes: bytes,
) -> str:
    """Send an image (no compression). Used for raw photos and animated GIFs."""
    if not image_bytes:
        raise ValueError("Weixin image data is empty")
    session = async_get_clientsession(hass)
    uploaded = await _async_upload_to_wechat_cdn(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        media_bytes=image_bytes,
        media_type=1,
    )
    item = {
        "type": 2,
        "image_item": {
            "media": _build_cdn_media(uploaded),
            "mid_size": uploaded.ciphertext_size,
        },
    }
    return await _async_send_message_with_item(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        context_token=context_token,
        item=item,
    )


async def async_send_weixin_video(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    video_bytes: bytes,
) -> str:
    """Send a video (no compression)."""
    if not video_bytes:
        raise ValueError("Weixin video data is empty")
    session = async_get_clientsession(hass)
    uploaded = await _async_upload_to_wechat_cdn(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        media_bytes=video_bytes,
        media_type=2,
    )
    item = {
        "type": 5,
        "video_item": {
            "media": _build_cdn_media(uploaded),
            "video_size": uploaded.ciphertext_size,
        },
    }
    return await _async_send_message_with_item(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        context_token=context_token,
        item=item,
    )


async def async_send_weixin_file(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    file_bytes: bytes,
    file_name: str,
) -> str:
    """Send a file attachment."""
    if not file_bytes:
        raise ValueError("Weixin file data is empty")
    if not file_name:
        raise ValueError("Weixin file name is required")
    session = async_get_clientsession(hass)
    uploaded = await _async_upload_to_wechat_cdn(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        media_bytes=file_bytes,
        media_type=3,
    )
    item = {
        "type": 4,
        "file_item": {
            "media": _build_cdn_media(uploaded),
            "file_name": file_name,
            "len": str(uploaded.plaintext_size),
        },
    }
    return await _async_send_message_with_item(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        context_token=context_token,
        item=item,
    )


async def async_send_weixin_voice(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    to_user_id: str,
    context_token: str,
    silk_bytes: bytes,
    duration_ms: int,
) -> str:
    """Send a voice message (SILK format)."""
    if not silk_bytes:
        raise ValueError("Weixin voice data is empty")
    session = async_get_clientsession(hass)
    uploaded = await _async_upload_to_wechat_cdn(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        media_bytes=silk_bytes,
        media_type=4,
    )
    cdn_media = _build_cdn_media(uploaded)
    cdn_media["full_url"] = (
        f"{_WECHAT_CDN_BASE_URL}/download"
        f"?encrypted_query_param={quote(uploaded.encrypt_query_param, safe='')}"
    )
    item = {
        "type": 3,
        "voice_item": {
            "media": cdn_media,
            "encode_type": 6,
            "bits_per_sample": 16,
            "sample_rate": 16000,
            "playtime": duration_ms,
        },
    }
    return await _async_send_message_with_item(
        session,
        base_url=base_url,
        token=token,
        to_user_id=to_user_id,
        context_token=context_token,
        item=item,
    )


async def async_get_typing_ticket(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    ilink_user_id: str,
    context_token: str,
) -> str:
    session = async_get_clientsession(hass)
    resp = await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/getconfig",
        payload={
            "ilink_user_id": ilink_user_id,
            "context_token": context_token,
            "base_info": {"channel_version": "ha-cn-im-hub"},
        },
        token=token,
    )
    return str(resp.get("typing_ticket") or "")


async def async_send_typing(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
    ilink_user_id: str,
    typing_ticket: str,
    status: int = 1,
) -> None:
    session = async_get_clientsession(hass)
    await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/sendtyping",
        payload={
            "ilink_user_id": ilink_user_id,
            "typing_ticket": typing_ticket,
            "status": status,
            "base_info": {"channel_version": "ha-cn-im-hub"},
        },
        token=token,
    )


async def async_notify_start(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
) -> None:
    """Notify Weixin server that this account is starting.

    Mirrors openclaw-weixin 2.1.10+ notifyStart. Used for server-side online
    state reconciliation. Failures are logged and ignored by the caller.
    """
    session = async_get_clientsession(hass)
    await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/msg/notifystart",
        payload={"base_info": {"channel_version": "ha-cn-im-hub"}},
        token=token,
    )


async def async_notify_stop(
    hass: HomeAssistant,
    *,
    base_url: str,
    token: str,
) -> None:
    """Notify Weixin server that this account is stopping.

    Mirrors openclaw-weixin 2.1.10+ notifyStop. Uses its own request so it
    can complete after the long-poll loop has already been cancelled.
    """
    session = async_get_clientsession(hass)
    await _api_post(
        session,
        base_url=base_url,
        endpoint="ilink/bot/msg/notifystop",
        payload={"base_info": {"channel_version": "ha-cn-im-hub"}},
        token=token,
    )


def extract_text_body(message: dict[str, Any]) -> str:
    item_list = message.get("item_list") or []
    if not isinstance(item_list, list):
        return ""
    for item in item_list:
        if not isinstance(item, dict):
            continue
        if item.get("type") == 1:
            text_item = item.get("text_item") or {}
            text = text_item.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
        if item.get("type") == 3:
            voice_item = item.get("voice_item") or {}
            text = voice_item.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


@dataclass(slots=True)
class InboundMedia:
    kind: str
    encrypt_query_param: str
    aes_key_b64: str
    full_url: str
    file_name: str
    aeskey_hex: str


def extract_inbound_media(message: dict[str, Any]) -> InboundMedia | None:
    item_list = message.get("item_list") or []
    if not isinstance(item_list, list):
        return None
    for item in item_list:
        if not isinstance(item, dict):
            continue
        itype = item.get("type")
        if itype == 2:
            img = item.get("image_item") or {}
            media = img.get("media") or {}
            eqp = media.get("encrypt_query_param") or ""
            aes_key = media.get("aes_key") or ""
            full_url = media.get("full_url") or ""
            aeskey_hex = img.get("aeskey") or ""
            if eqp or full_url:
                return InboundMedia("image", eqp, aes_key, full_url, "", aeskey_hex)
        if itype == 4:
            fi = item.get("file_item") or {}
            media = fi.get("media") or {}
            eqp = media.get("encrypt_query_param") or ""
            aes_key = media.get("aes_key") or ""
            full_url = media.get("full_url") or ""
            fname = fi.get("file_name") or ""
            return InboundMedia("file", eqp, aes_key, full_url, fname, "")
    return None


def build_qr_data_url(text: str) -> str:
    out = BytesIO()
    segno.make(text).save(out, kind="png", scale=6, border=2)
    return "data:image/png;base64," + base64.b64encode(out.getvalue()).decode("ascii")
