# 即时通信合集 / CN IM Hub

把常见即时通信平台聚合到一个 Home Assistant 集成中。  
Aggregate common Chinese IM platforms into one Home Assistant integration.

告别复杂的 Node-RED 工作流，告别容易出错的 YAML 编排。配合已收录在 HACS 中的 `AI HUB`，你可以直接用自然语言控制家里的设备，全部能力都在 Home Assistant 内完成。

[![Open your Home Assistant instance and open AI HUB in HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=ha-china&repository=ai_hub&category=integration)

## 文档 / Docs

- 中文配置指南：[`CONFIG.zh-CN.md`](CONFIG.zh-CN.md)
- Chinese setup guide for official platform backends: [`CONFIG.zh-CN.md`](CONFIG.zh-CN.md)

## 当前支持 / Supported Providers

- Feishu
- WeCom
- QQ（WebSocket 网关） / QQ (WebSocket gateway)
- DingTalk（Stream 模式） / DingTalk (Stream mode)
- WeChat（个人微信，支持多人绑定） / WeChat personal accounts with multi-binding
- XiaoYi（小艺 A2A WebSocket） / XiaoYi A2A WebSocket

## 功能 / Features

- 一个 Hub 统一接入多个 IM 平台  
  One Hub can connect multiple IM providers.
- 集成级只配置一次全局 `agent_id`  
  Configure `agent_id` once at integration level.
- 各平台通过 subentry 独立添加、独立更新  
  Each provider is managed as an independent subentry.
- 个人微信支持绑定多个账号  
  Personal WeChat supports multiple bound accounts.
- 统一的 `cn_im_hub.send_message` 服务  
  Unified `cn_im_hub.send_message` service.
- `camera_entity` 可直接抓拍并发送图片  
  `camera_entity` can capture and send snapshots directly.
- 图片出站当前支持 `WeChat`、`WeCom`、`Feishu`、`QQ`、`DingTalk`  
  Outbound image sending currently supports WeChat, WeCom, Feishu, QQ, and DingTalk.
- 语音只在平台已提供识别文本时转给 HA  
  Voice is passed to HA only when the platform already provides transcript text.
- 飞书自动卡片回复：当回复内容较长或包含列表/标题等结构时，自动以卡片形式发送  
  Feishu auto-card reply: responses are automatically sent as interactive cards when they are long or contain structured content (lists, headings, etc.).

## 安装 / Installation

1. 将本仓库部署到 HA 的 `custom_components/cn_im_hub`。  
   Deploy this repository to `custom_components/cn_im_hub`.
2. 重启 Home Assistant。  
   Restart Home Assistant.
3. 进入 `设置 -> 设备与服务 -> 添加集成`，搜索 `即时通信合集`。  
   Go to `Settings -> Devices & Services -> Add Integration`, then search for `即时通信合集`.
4. 首次添加时选择一次全局 `agent_id`。  
   Select the global `agent_id` once during first setup.
5. 之后按平台添加子服务。后台配置步骤见 [`CONFIG.zh-CN.md`](CONFIG.zh-CN.md)。  
   Then add provider subentries. Backend setup steps are documented in [`CONFIG.zh-CN.md`](CONFIG.zh-CN.md).

[![Open your Home Assistant instance and open this repository in HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=ha-china&repository=cn_im_hub&category=integration)

## HA 服务 / HA Service

- `cn_im_hub.send_message`
- 参数 / Fields:
  - `channel`
  - `target`
  - `message`
  - `camera_entity`
  - `wechat_account_id`（仅多微信账号时可选） / optional for multi-WeChat routing

## 目标地址格式 / Target Routing

- `channel` 选择发送通道与目标类型，例如：`feishu/chat_id`、`qq/group`、`wechat/user_id`。  
  `channel` selects the provider and target type, for example `feishu/chat_id`, `qq/group`, `wechat/user_id`.
- 如果存在多个同类平台实例，`send_message` 会先按 `target` 命中历史目标自动路由；如果没填 `target`，则自动使用当前唯一已选的 `target selector`。  
  If multiple instances of the same provider exist, `send_message` first routes by a known `target`; if `target` is empty, it falls back to the only currently selected `target selector`.
- 多个个人微信账号并存时，通常无需手填 `wechat_account_id`；仅在路由仍然歧义时才需要填写。  
  With multiple personal WeChat accounts, `wechat_account_id` is usually not required unless routing is still ambiguous.
- `camera_entity` 会抓取当前快照并作为图片发送。  
  `camera_entity` captures the current snapshot and sends it as an image.

## 对话方式 / Conversation Flow

- 消息统一转到集成级配置的 `agent_id` 对应的 HA conversation agent。  
  Messages are forwarded to the HA conversation agent bound to the integration-level `agent_id`.
- 以自然语言对话为主。  
  Natural-language conversation is the main interaction style.

## 参考 / References

- 平台后台配置与截图：[`CONFIG.zh-CN.md`](CONFIG.zh-CN.md)
- Upstream tracking: [`upstream.txt`](upstream.txt)
