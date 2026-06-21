from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
from typing import Any

from aiohttp import web
from homeassistant.components.http import HomeAssistantView
from homeassistant.core import HomeAssistant
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ...const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class FeishuCardCallbackView(HomeAssistantView):
    requires_auth = False
    url = "/api/cn_im_hub/feishu/card_callback"
    name = "api:cn_im_hub:feishu:card_callback"

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass

    async def post(self, request: web.Request) -> web.Response:
        try:
            raw_text = await request.text()
            data = _parse_json(raw_text)
            verified = _verify_callback(self._hass, request, data)
            if verified is None:
                return web.json_response({"error": "unauthorized"}, status=401)

            data = verified
            if data.get("type") == "url_verification":
                return web.json_response({"challenge": data.get("challenge", "")})

            event = data.get("event", {})
            action = event.get("action", {})
            operator = event.get("operator", {})
            action_value = _decode_action_value(action.get("value", {}))
            self._hass.bus.async_fire(f"{DOMAIN}_feishu_card_action", {"action": action, "operator": operator, "raw_data": data})
            _LOGGER.info(
                "Feishu card action fired: value=%s, operator=%s",
                json.dumps(action_value, ensure_ascii=False)[:300],
                json.dumps(operator, ensure_ascii=False)[:200],
            )
            return web.json_response({"toast": {"type": "info", "content": _toast_content(action_value)}})
        except Exception:
            _LOGGER.exception("Feishu card callback error")
            return web.json_response({"error": "internal error"}, status=400)

    async def get(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "message": "Feishu card callback endpoint is active"})


async def _read_body(request: web.Request) -> dict[str, Any]:
    try:
        return json.loads(await request.text())
    except json.JSONDecodeError:
        _LOGGER.warning("Feishu card callback: invalid JSON body")
        return {}


def _parse_json(raw_text: str) -> dict[str, Any]:
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError:
        _LOGGER.warning("Feishu card callback: invalid JSON body")
        return {}
    return data if isinstance(data, dict) else {}


def _verify_callback(hass: HomeAssistant, request: web.Request, data: dict[str, Any]) -> dict[str, Any] | None:
    configs = hass.data.get(DOMAIN, {}).get("feishu_callback_configs", {})
    if not configs:
        _LOGGER.warning("Feishu card callback rejected: no Feishu callback config registered")
        return None

    headers = {str(key).lower(): value for key, value in request.headers.items()}
    for config in configs.values():
        verification_token = str(config.get("verification_token") or "").strip()
        encrypt_key = str(config.get("encrypt_key") or "").strip()
        parsed = _verify_callback_with_config(data, headers, verification_token, encrypt_key)
        if parsed is not None:
            return parsed

    _LOGGER.warning("Feishu card callback rejected: signature/token validation failed")
    return None


def _verify_callback_with_config(
    data: dict[str, Any],
    headers: dict[str, str],
    verification_token: str,
    encrypt_key: str,
) -> dict[str, Any] | None:
    if not data:
        return None

    if "encrypt" in data:
        if not encrypt_key:
            return None
        if not _check_event_signature(data, headers, encrypt_key):
            return None
        parsed = _decrypt_payload(str(data.get("encrypt") or ""), encrypt_key)
        if parsed is None:
            return None
        if verification_token and str(parsed.get("token") or "").strip() != verification_token:
            return None
        return {**parsed, **{key: value for key, value in data.items() if key != "encrypt"}}

    if "schema" in data:
        if not _check_event_signature(data, headers, encrypt_key):
            return None
        if verification_token and str(data.get("token") or "").strip() != verification_token:
            return None
        return data

    if verification_token and not _check_card_signature(data, headers, verification_token):
        return None
    if verification_token and str(data.get("token") or "").strip() != verification_token:
        return None
    return data


def _check_event_signature(data: dict[str, Any], headers: dict[str, str], encrypt_key: str) -> bool:
    if not encrypt_key:
        return True
    timestamp = headers.get("x-lark-request-timestamp", "")
    nonce = headers.get("x-lark-request-nonce", "")
    signature = headers.get("x-lark-signature", "")
    if not timestamp or not nonce or not signature:
        return False
    content = f"{timestamp}{nonce}{encrypt_key}{json.dumps(data, separators=(',', ':'), ensure_ascii=False)}"
    computed = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return hmac.compare_digest(computed, signature)


def _check_card_signature(data: dict[str, Any], headers: dict[str, str], verification_token: str) -> bool:
    if not verification_token:
        return True
    timestamp = headers.get("x-lark-request-timestamp", "")
    nonce = headers.get("x-lark-request-nonce", "")
    signature = headers.get("x-lark-signature", "")
    if not timestamp or not nonce or not signature:
        return False
    content = f"{timestamp}{nonce}{verification_token}{json.dumps(data, separators=(',', ':'), ensure_ascii=False)}"
    computed = hashlib.sha1(content.encode("utf-8")).hexdigest()
    return hmac.compare_digest(computed, signature)


def _decrypt_payload(encrypted: str, encrypt_key: str) -> dict[str, Any] | None:
    try:
        key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
        encrypted_bytes = base64.b64decode(encrypted)
        iv = encrypted_bytes[:16]
        ciphertext = encrypted_bytes[16:]
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded) + unpadder.finalize()
        payload = json.loads(plaintext.decode("utf-8"))
    except Exception:
        _LOGGER.exception("Feishu card callback: failed to decrypt payload")
        return None
    return payload if isinstance(payload, dict) else None


def _decode_action_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return {}


def _toast_content(action_value: Any) -> str:
    if not isinstance(action_value, dict):
        return "OK"
    template = action_value.get("toast")
    if not template:
        return "OK"
    try:
        return template.format(**action_value)
    except (KeyError, IndexError, ValueError):
        return template
