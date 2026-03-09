# 备忘录：AstrBot 图像 Base64 自动注入机制与开发复盘

## 1. 核心机制沉淀 (Lesson Learned)

### A. 消息链与图片存储机制
*   **Adapter 的行为**：在接收消息时，Adapter（如 Telegram）并不总是立即下载图片。`Image` 组件初始时可能只有 `url`，`path` 属性为空。
*   **历史记录 (Conversation)**：AstrBot 存储历史记录时，会将消息对象序列化。如果图片没有被下载过，历史记录里就只有 `url`。
*   **本地缓存**：只有当代码显式调用下载逻辑（如 `download_image_by_url`）或某些核心流程触发下载后，本地缓存才会生成，`path` 才会指向有效文件。
*   **教训**：**不能假设 `path` 永远有值。** 插件必须具备“兜底下载”的能力，且必须处理网络超时（特别是 Telegram 链接）。

### B. Hook 机制的真相 (The Trap)
*   **`on_llm_response` (事后诸葛亮)**：这个 Hook 是在 **Agent Loop 彻底结束**（或者收到 LLM 最终响应并执行完工具）之后才触发的。**无法**用于拦截或修改工具参数。我们在这里浪费了大量时间尝试“无效注入”。
*   **`on_using_llm_tool` (真正的拦截点)**：这是在 `ToolLoopAgentRunner` 准备执行工具**之前**触发的。且 `tool_args` 是通过**引用传递**的字典，修改它**立即生效**。这才是参数注入的唯一正确入口。
*   **教训**：在设计拦截逻辑前，必须**查阅源码中 Hook 的调用位置**，而不是仅看名字猜测。

### C. LLM 的行为与引导
*   **自主决策**：即使代码默认值设为 `url`，如果工具描述中暗示了“失败后重试 Base64”，LLM 可能会为了“保险”而直接请求 Base64。
*   **提示词工程**：必须在 `description` 中使用强烈的语气（如 `ALWAYS use 'url' first`）来约束 LLM 的行为。

---

## 2. 最终架构：Magic Placeholder + Pre-Execution Hook

### A. 设计目标
在不将庞大的 Base64 原始数据发送给大语言模型（LLM）的前提下，确保 MCP 等工具在调用时能够获取到完整的图像数据。
- **节省 Token**：LLM 只处理轻量级的占位符。
- **解决连通性**：针对 Telegram 等平台链接不可直接访问的问题，在服务器本地完成数据准备。
- **自动化**：无需模型手动处理复杂的 Base64 转换。

### B. 占位符机制 (Explicit Tool)
当 LLM 调用 `get_image_from_context(return_type='base64')` 时，插件**不返回**真实数据，而是返回：
`base64://ASTRBOT_PLUGIN_CACHE_PENDING`
这既欺骗了 LLM 认为“任务完成”，又成为了 Hook 的识别信标。

### C. 拦截注入 (Implicit Hook)
使用 `on_using_llm_tool` 钩子。
1.  **拦截**：监控所有工具调用。
2.  **识别**：发现参数值为占位符，或参数名是 `image/url` 且为空。
3.  **注入**：调用插件内部逻辑，强制获取真实 Base64（查缓存 > 强下载），直接修改参数字典。

---

## 3. 开发过程复盘 (Post-Mortem)

### 踩坑记录
1.  **盲目自信**：初期未验证 Hook 时机，错误地使用了 `on_llm_response`，导致注入代码看似执行了，但工具拿到的还是旧参数。
2.  **网络假设**：假设 Telegram 链接可以直接用，或者 AstrBot 一定预下载了。事实是 Telegram 链接在 MCP 端无法访问，且本地缓存未必存在。
3.  **Token 浪费**：中期为了跑通流程，曾一度妥协让工具直出 Base64，导致 LLM 上下文爆炸。

### 正确路径
1.  **源码分析**：找到 `tool_loop_agent_runner.py`，看到 `on_tool_start` 触发了 `OnUsingLLMToolEvent`。
2.  **引用传递**：确认 `tool_args` 是可变字典。
3.  **闭环设计**：确立了“工具给假数据（占位符） -> Hook 换真数据 -> MCP 吃真数据”的流水线。

### 总结
**“源码面前，了无秘密。”**
下次开发涉及核心流程拦截的插件时，必须先定位 Hook 在源码中的具体行数和上下文，确认数据流向是否可逆/可变，再动手写代码。

---

**版本记录：** v1.1.4 (2026-02-01)
**维护者：** Thetail001 & Gemini CLI