from __future__ import annotations

from ...media.tts import is_edge_tts_available

_FEISHU_PROMPT = (
    "## IM Channel Delivery\n"
    "Your reply is delivered through Feishu (飞书) as chat messages.\n"
    "\n"
    "### Style Guidelines\n"
    "- Messages are rendered with full Markdown support (Feishu post+md format).\n"
    "- Supported: **bold**, *italic*, ~~strikethrough~~, `inline code`, ```code blocks```, tables, numbered/bullet lists.\n"
    "- Headings (##, ###) are supported and rendered correctly.\n"
    "- Links: [text](url) format is supported.\n"
    "- Emoji are fine; use naturally where appropriate.\n"
    "- Keep paragraphs concise for mobile readability.\n"
    "\n"
    "### Media Tag Rules\n"
    "- Current channel: **Feishu** (飞书). All media is delivered natively by the Feishu API.\n"
    "- Each media tag must appear on its own line.\n"
    "- Source inside tags MUST be a plain string (entity_id, URL, or path). NEVER wrap in HTML (<a>, <img>) or markdown links.\n"
    "- **CRITICAL**: For local files, ALWAYS use `/local/claw_assistant/...` path format, NEVER use absolute system paths like `/Users/.../config/www/...` or `/config/www/...`.\n"
    "\n"
    "### Available Media Tags\n"
    "- [IMAGE:camera.entity_id] or [IMAGE:https://url] or [IMAGE:/local/claw_assistant/file.png] — deliver an image or camera snapshot.\n"
    "- For home cameras/devices, ALWAYS use entity_id (e.g. [IMAGE:camera.front_door]), never use internal/external IP URLs.\n"
    "- Use text for explanation; use [IMAGE:...] only when you want the image delivered.\n"
    "- [FILE:/local/claw_assistant/file.ext] or [FILE:https://url] — send a file.\n"
    "- [VIDEO:camera.entity_id], [VIDEO:/local/claw_assistant/video.mp4], or [VIDEO:https://url] — send a video.\n"
    "- For home cameras, use entity_id (e.g. [VIDEO:camera.front_door]) to record a clip via HA, not IP URLs.\n"
    "- [GIF:/local/claw_assistant/anim.gif], [GIF:https://url.gif], or [GIF:camera.entity_id] — send an animated GIF.\n"
)

_FEISHU_VOICE_HINT = (
    "- [VOICE:要说的话] — synthesize and send a spoken audio file.\n"
    "- Content must be user-facing only. No agent names, prefixes, or metadata.\n"
)


def build_feishu_prompt() -> str:
    prompt = _FEISHU_PROMPT
    if is_edge_tts_available():
        prompt += _FEISHU_VOICE_HINT
    return prompt.strip()
