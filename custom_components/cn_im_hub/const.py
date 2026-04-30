"""Constants for CN IM Hub."""

from __future__ import annotations

from typing import Final

DOMAIN: Final = "cn_im_hub"

PROVIDER_FEISHU: Final = "feishu"
PROVIDER_WECOM: Final = "wecom"
PROVIDER_QQ: Final = "qq"
PROVIDER_DINGTALK: Final = "dingtalk"
PROVIDER_WECHAT: Final = "wechat"
PROVIDER_XIAOYI: Final = "xiaoyi"
PROVIDERS: Final = (PROVIDER_WECOM, PROVIDER_WECHAT, PROVIDER_QQ, PROVIDER_FEISHU, PROVIDER_DINGTALK, PROVIDER_XIAOYI)

CONF_ENABLED_PROVIDERS: Final = "enabled_providers"
CONF_PROVIDERS: Final = "providers"
CONF_AGENT_ID: Final = "agent_id"

CONF_FEISHU_APP_ID: Final = "app_id"
CONF_FEISHU_APP_SECRET: Final = "app_secret"

CONF_WECOM_BOT_ID: Final = "bot_id"
CONF_WECOM_SECRET: Final = "secret"

CONF_QQ_APP_ID: Final = "qq_app_id"
CONF_QQ_CLIENT_SECRET: Final = "qq_client_secret"

CONF_DINGTALK_CLIENT_ID: Final = "dingtalk_client_id"
CONF_DINGTALK_CLIENT_SECRET: Final = "dingtalk_client_secret"

CONF_WECHAT_TOKEN: Final = "wechat_token"
CONF_WECHAT_ACCOUNT_ID: Final = "wechat_account_id"
CONF_WECHAT_USER_ID: Final = "wechat_user_id"
CONF_WECHAT_BASE_URL: Final = "wechat_base_url"
CONF_WECHAT_SYNC_BUF: Final = "wechat_sync_buf"

CONF_XIAOYI_AK: Final = "xiaoyi_ak"
CONF_XIAOYI_SK: Final = "xiaoyi_sk"
CONF_XIAOYI_AGENT_ID: Final = "xiaoyi_agent_id"
CONF_XIAOYI_WS_URL_1: Final = "xiaoyi_ws_url_1"
CONF_XIAOYI_WS_URL_2: Final = "xiaoyi_ws_url_2"

XIAOYI_DEFAULT_WS_URL_1: Final = "wss://hag.cloud.huawei.com/openclaw/v1/ws/link"
XIAOYI_DEFAULT_WS_URL_2: Final = "wss://116.63.174.231/openclaw/v1/ws/link"

WECHAT_DEFAULT_BASE_URL: Final = "https://ilinkai.weixin.qq.com"

SERVICE_SEND_MESSAGE: Final = "send_message"

# Event bus topic published by the claw_assistant integration when an LLM
# inference produces an incremental progress chunk. Subscribed by the QQ /
# WeChat providers to stream "still working…" hints back to the user.
# This is a string-only contract so cn_im_hub must NOT import claw_assistant
# at module load — the integration is optional and may be missing.
CLAW_LIVE_PROGRESS_EVENT: Final = "ha_crack_live_progress"

ATTR_PROVIDER: Final = "provider"
ATTR_TARGET: Final = "target"
ATTR_MESSAGE: Final = "message"
ATTR_TEXT: Final = "text"
ATTR_TARGET_TYPE: Final = "target_type"
ATTR_CHANNEL: Final = "channel"
ATTR_WECHAT_ACCOUNT_ID: Final = "wechat_account_id"
ATTR_CAMERA_ENTITY: Final = "camera_entity"
ATTR_FILE_PATH: Final = "file_path"
ATTR_FILE_URL: Final = "file_url"
ATTR_FILE_NAME: Final = "file_name"
ATTR_MEDIA_TYPE: Final = "media_type"
ATTR_TTS_TEXT: Final = "tts_text"
ATTR_MESSAGE_FORMAT: Final = "message_format"
ATTR_APPROVAL_ID: Final = "approval_id"
ATTR_RECORD_DURATION: Final = "record_duration"
ATTR_LOOKBACK: Final = "lookback"
ATTR_GIF_FPS: Final = "gif_fps"
DEFAULT_VIDEO_RECORD_DURATION: Final = 10
DEFAULT_GIF_DURATION: Final = 3

DEFAULT_FEISHU_TARGET_TYPE: Final = "chat_id"

CHANNEL_FEISHU_CHAT_ID: Final = "feishu/chat_id"
CHANNEL_WECOM_CHATID: Final = "wecom/chatid"
CHANNEL_QQ_USER: Final = "qq/user"
CHANNEL_QQ_GROUP: Final = "qq/group"
CHANNEL_QQ_CHANNEL: Final = "qq/channel"
CHANNEL_DINGTALK_USER: Final = "dingtalk/user"
CHANNEL_DINGTALK_GROUP: Final = "dingtalk/group"
CHANNEL_WECHAT_USER_ID: Final = "wechat/user_id"

CHANNEL_OPTIONS: Final = (
    CHANNEL_FEISHU_CHAT_ID,
    CHANNEL_WECOM_CHATID,
    CHANNEL_QQ_USER,
    CHANNEL_QQ_GROUP,
    CHANNEL_QQ_CHANNEL,
    CHANNEL_DINGTALK_USER,
    CHANNEL_DINGTALK_GROUP,
    CHANNEL_WECHAT_USER_ID,
)
