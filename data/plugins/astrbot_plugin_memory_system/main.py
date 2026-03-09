"""
AstrBot 三层记忆系统插件

结构：
  Layer 1 (长期核心记忆): /AstrBot/data/workspace/*.md
  Layer 2 (每日日记):      /AstrBot/data/workspace/memory/YYYY-MM-DD.md
  Layer 3 (会话上下文):    当前会话（由 AstrBot 管理）

功能：
  - 自动在 LLM 请求前注入三层记忆上下文
  - 提供 LLM 工具：write_diary / recall_memory / distill_memory
  - 提供用户指令：/记忆状态 / /提炼记忆 / /查看日记 [日期]
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, register

WORKSPACE = Path("/AstrBot/data/workspace")
MEMORY_DIR = WORKSPACE / "memory"

CORE_FILES = {
    "SOUL":     WORKSPACE / "SOUL.md",
    "IDENTITY": WORKSPACE / "IDENTITY.md",
    "USER":     WORKSPACE / "USER.md",
    "MEMORY":   WORKSPACE / "MEMORY.md",
    "AGENTS":   WORKSPACE / "AGENTS.md",
}


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


def _diary_path(date: str) -> Path:
    return MEMORY_DIR / f"{date}.md"


def _read_file(path: Path) -> str:
    """安全读取文件，不存在时返回空字符串。"""
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def _append_diary(date: str, content: str) -> None:
    """向指定日期的日记追加内容。"""
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)
    path = _diary_path(date)
    ts = datetime.now().strftime("%H:%M")
    entry = f"\n### [{ts}]\n{content}\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(entry)


def _build_context_block() -> str:
    """
    构建注入到 LLM system_prompt 的三层记忆上下文块。
    Layer 1: 核心文件（SOUL / IDENTITY / USER / MEMORY / AGENTS）
    Layer 2: 今天 + 昨天的日记
    """
    sections: list[str] = []

    # ── Layer 1 ──
    layer1_parts: list[str] = []
    for name, path in CORE_FILES.items():
        text = _read_file(path)
        if text:
            layer1_parts.append(f"#### [{name}]\n{text}")
    if layer1_parts:
        sections.append(
            "## 🗂️ 长期核心记忆（Layer 1）\n" + "\n\n".join(layer1_parts)
        )

    # ── Layer 2 ──
    diary_parts: list[str] = []
    for date in [_yesterday(), _today()]:
        text = _read_file(_diary_path(date))
        if text:
            diary_parts.append(f"#### 📅 {date}\n{text}")
    if diary_parts:
        sections.append(
            "## 📅 每日日记（Layer 2）\n" + "\n\n".join(diary_parts)
        )

    if not sections:
        return ""

    return (
        "\n\n---\n<!-- 三层记忆系统注入 START -->\n"
        + "\n\n".join(sections)
        + "\n<!-- 三层记忆系统注入 END -->\n---\n"
    )


@register(
    "astrbot_plugin_memory_system",
    "Patori",
    "三层记忆系统：自动注入长期/中期/短期记忆，并提供日记写入与记忆提炼工具",
    "1.0.0",
)
class MemorySystemPlugin(Star):
    """三层记忆系统插件。"""

    def __init__(self, context: Context) -> None:
        super().__init__(context)
        MEMORY_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("[MemorySystem] 插件已加载，workspace=%s", WORKSPACE)

    # ════════════════════════════════════════
    #  LLM 请求钩子：注入三层记忆上下文
    # ════════════════════════════════════════


    # ══════════════════════════════════════════════════════════
    #  消息管线：过滤空白消息（aiocqhttp 输入中事件）
    # ══════════════════════════════════════════════════════════

    @filter.on_waiting_llm_request()
    async def filter_empty_message(
        self, event: AstrMessageEvent
    ) -> None:
        """
        过滤 aiocqhttp 上报的空白消息事件（如"用户正在输入"通知）。
        若消息内容为空字符串，则直接停止后续处理，不交给 LLM。
        """
        msg = event.message_str.strip() if event.message_str else ""
        if not msg:
            logger.debug("[MemorySystem] 拦截空白消息事件，跳过处理")
            event.stop_event()
            return

    @filter.on_llm_request()
    async def inject_memory_context(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        """在每次 LLM 请求前将三层记忆注入 system_prompt，并在上下文过大时自动截断。"""
        import json as _json

        # 1. 注入记忆上下文
        block = _build_context_block()
        if block:
            req.system_prompt = (req.system_prompt or "") + block
            logger.debug("[MemorySystem] 已注入记忆上下文（%d chars）", len(block))

        # 2. 预防性历史截断：如果 history 消息总 chars 超过阈值，自动截断旧消息
        AUTO_COMPACT_CHARS = 200_000  # 约 50k tokens 时自动截断（保守阈值）
        KEEP_RECENT_TURNS = 20
        try:
            if req.contexts:
                contexts_str = _json.dumps(req.contexts, ensure_ascii=False)
                if len(contexts_str) > AUTO_COMPACT_CHARS:
                    from astrbot.core.agent.context.truncator import ContextTruncator
                    from astrbot.core.agent.message import Message as _Msg
                    msgs = [_Msg.model_validate(m) if isinstance(m, dict) else m for m in req.contexts]
                    _truncator = ContextTruncator()
                    truncated = _truncator.truncate_by_turns(msgs, keep_most_recent_turns=KEEP_RECENT_TURNS)
                    req.contexts = [m.model_dump() if hasattr(m, "model_dump") else m for m in truncated]
                    logger.warning(
                        "[MemorySystem] 自动截断历史上下文：%d -> %d 条消息（原始 %d chars 超过阈值 %d）",
                        len(msgs), len(truncated), len(contexts_str), AUTO_COMPACT_CHARS,
                    )
        except Exception as _e:
            logger.warning("[MemorySystem] 预防性截断失败（非致命）: %s", _e)

    # ════════════════════════════════════════
    #  LLM 工具：写日记
    # ════════════════════════════════════════

    @filter.llm_tool(name="write_diary")
    async def write_diary(
        self,
        event: AstrMessageEvent,
        content: str,
        date: Optional[str] = None,
    ) -> MessageEventResult:
        """
        向每日日记写入一条记录（Layer 2 记忆）。
        适合记录主人的新偏好、完成的任务、特殊指示等重要信息。

        Args:
            content(string): 要写入日记的内容
            date(string, optional): 目标日期，格式 YYYY-MM-DD，默认今天
        """
        target_date = date or _today()
        # 简单校验日期格式
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", target_date):
            yield event.plain_result(f"❌ 日期格式错误：{target_date}，请使用 YYYY-MM-DD")
            return
        _append_diary(target_date, content)
        logger.info("[MemorySystem] 日记已写入 %s", target_date)
        yield event.plain_result(f"✅ 已记录到 {target_date} 的日记～")

    # ════════════════════════════════════════
    #  LLM 工具：召回记忆
    # ════════════════════════════════════════

    @filter.llm_tool(name="recall_memory")
    async def recall_memory(
        self,
        event: AstrMessageEvent,
        target: str = "today",
    ) -> MessageEventResult:
        """
        读取指定记忆文件或日记内容，用于主动回忆。

        Args:
            target(string): 目标，可选值：
                "today"     — 今天日记
                "yesterday" — 昨天日记
                "YYYY-MM-DD" — 指定日期日记
                "MEMORY"    — 长期记忆文件
                "USER"      — 主人信息
                "SOUL"      — 帕托莉灵魂设定
                "IDENTITY"  — 身份定义
                "AGENTS"    — 行为规范
        """
        if target in CORE_FILES:
            text = _read_file(CORE_FILES[target])
            label = target
        elif target == "today":
            text = _read_file(_diary_path(_today()))
            label = f"今日日记（{_today()}）"
        elif target == "yesterday":
            text = _read_file(_diary_path(_yesterday()))
            label = f"昨日日记（{_yesterday()}）"
        elif re.match(r"^\d{4}-\d{2}-\d{2}$", target):
            text = _read_file(_diary_path(target))
            label = f"日记（{target}）"
        else:
            yield event.plain_result(f"❌ 未知目标：{target}")
            return

        if text:
            yield event.plain_result(f"📖 [{label}]\n\n{text}")
        else:
            yield event.plain_result(f"📭 [{label}] 暂无内容")

    # ════════════════════════════════════════
    #  LLM 工具：提炼记忆
    # ════════════════════════════════════════

    @filter.llm_tool(name="distill_memory")
    async def distill_memory(
        self,
        event: AstrMessageEvent,
        distilled_content: str,
        target_file: str = "MEMORY",
    ) -> MessageEventResult:
        """
        将提炼好的关键信息写入长期核心记忆文件（Layer 1）。
        适合在主人说"提炼记忆"时，将日记中的重要信息整理后写入。

        Args:
            distilled_content(string): 提炼后的核心记忆内容（Markdown 格式）
            target_file(string): 目标文件，可选 "MEMORY"（默认）或 "USER"
        """
        if target_file not in ("MEMORY", "USER"):
            yield event.plain_result(f"❌ 不支持写入 {target_file}，只能写入 MEMORY 或 USER")
            return

        path = CORE_FILES[target_file]
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        new_section = f"\n\n---\n<!-- 提炼于 {ts} -->\n{distilled_content}\n"

        existing = _read_file(path)
        path.write_text(existing + new_section, encoding="utf-8")
        logger.info("[MemorySystem] 已提炼记忆写入 %s", path)
        yield event.plain_result(f"✅ 已将提炼内容写入 {target_file}.md ～")

    # ════════════════════════════════════════
    #  用户指令：/记忆状态
    # ════════════════════════════════════════

    @filter.command("记忆状态")
    async def cmd_memory_status(self, event: AstrMessageEvent) -> None:
        """查看三层记忆系统当前状态。"""
        lines = ["📊 **三层记忆系统状态**\n"]

        # Layer 1
        lines.append("### 🗂️ Layer 1 — 长期核心记忆")
        for name, path in CORE_FILES.items():
            text = _read_file(path)
            size = len(text)
            status = f"{size} 字符" if text else "（空）"
            lines.append(f"- **{name}.md** — {status}")

        # Layer 2
        lines.append("\n### 📅 Layer 2 — 每日日记")
        diary_files = sorted(MEMORY_DIR.glob("*.md"), reverse=True)[:7]
        if diary_files:
            for f in diary_files:
                size = len(f.read_text(encoding="utf-8"))
                lines.append(f"- **{f.stem}** — {size} 字符")
        else:
            lines.append("- （暂无日记）")

        lines.append("\n### 💬 Layer 3 — 会话上下文")
        lines.append("- 当前会话由 AstrBot 管理，自动保留。")

        yield event.plain_result("\n".join(lines))

    # ════════════════════════════════════════
    #  用户指令：/提炼记忆
    # ════════════════════════════════════════

    @filter.command("提炼记忆")
    async def cmd_distill(self, event: AstrMessageEvent) -> None:
        """
        触发 LLM 自动提炼最近日记并写入长期记忆。
        用法：/提炼记忆
        """
        # 收集近期日记作为素材
        diary_parts: list[str] = []
        for date in [_yesterday(), _today()]:
            text = _read_file(_diary_path(date))
            if text:
                diary_parts.append(f"### {date}\n{text}")

        if not diary_parts:
            yield event.plain_result("📭 最近两天没有日记内容，无法提炼～")
            return

        material = "\n\n".join(diary_parts)
        prompt = (
            "请根据以下日记内容，提炼出关于主人的重要特征、偏好、完成的任务和特殊指示，"
            "以简洁的 Markdown 要点格式输出，方便长期记忆存储。"
            "同时调用 distill_memory 工具将提炼结果写入 MEMORY 文件。\n\n"
            f"【日记素材】\n{material}"
        )
        yield event.plain_result(
            "🔄 正在让 LLM 提炼记忆，请稍等～\n\n（LLM 将自动调用 distill_memory 工具写入）"
        )
        # 发送到 LLM 处理（借用 event.request_llm 或直接描述意图即可）
        async for result in event.request_llm(prompt):
            yield result

    # ════════════════════════════════════════
    #  用户指令：/查看日记 [日期]
    # ════════════════════════════════════════

    @filter.command("查看日记")
    async def cmd_view_diary(
        self, event: AstrMessageEvent, date: Optional[str] = None
    ) -> None:
        """
        查看指定日期的日记。
        用法：/查看日记           — 查看今天
              /查看日记 2026-03-07 — 查看指定日期
        """
        target = date or _today()
        text = _read_file(_diary_path(target))
        if text:
            yield event.plain_result(f"📖 **{target} 日记**\n\n{text}")
        else:
            yield event.plain_result(f"📭 {target} 没有日记内容～")

    # ════════════════════════════════════════
    #  用户指令：/写日记 <内容>
    # ════════════════════════════════════════

    @filter.command("写日记")
    async def cmd_write_diary(
        self, event: AstrMessageEvent, *, content: str = ""
    ) -> None:
        """
        手动写入今天的日记。
        用法：/写日记 今天完成了项目 X 的部署
        """
        # 从消息中提取内容
        raw = event.message_str.strip()
        # 去掉指令前缀
        for prefix in ["/写日记", "写日记"]:
            if raw.startswith(prefix):
                raw = raw[len(prefix):].strip()
                break
        if not raw:
            yield event.plain_result("❓ 请在指令后附上要记录的内容，例如：/写日记 今天完成了XXX")
            return
        _append_diary(_today(), raw)
        yield event.plain_result(f"✅ 已记录到今天（{_today()}）的日记～")

    # ════════════════════════════════════════
    #  生命周期
    # ════════════════════════════════════════

    # ════════════════════════════════════════
    #  用户指令：/compact — 手动压缩当前上下文
    # ════════════════════════════════════════

    @filter.command("compact")
    async def cmd_compact(self, event: AstrMessageEvent) -> None:
        """
        手动触发上下文压缩。
        用法：
          /compact         — LLM 摘要压缩，保留最近 6 轮
          /compact 10      — LLM 摘要压缩，保留最近 10 轮
          /compact hard    — 强制截断，只保留最近 20 轮（不消耗 LLM）
          /compact clear   — 清空全部历史（谨慎！）
        """
        import json
        from astrbot.core.agent.context.compressor import LLMSummaryCompressor
        from astrbot.core.agent.context.truncator import ContextTruncator
        from astrbot.core.agent.message import Message

        umo = event.unified_msg_origin
        conv_mgr = self.context.conversation_manager

        # 解析参数
        raw_msg = event.message_str.strip()
        args = raw_msg.split()[1:]  # 去掉 /compact
        mode = "llm"
        keep_recent = 6
        if args:
            arg0 = args[0].lower()
            if arg0 == "hard":
                mode = "hard"
                keep_recent = 20
            elif arg0 == "clear":
                mode = "clear"
            else:
                try:
                    keep_recent = max(2, int(arg0))
                except ValueError:
                    yield event.plain_result("⚠️ 参数无效，请用 /compact [数字|hard|clear]")
                    return

        # 1. 获取当前 conversation
        cid = await conv_mgr.get_curr_conversation_id(umo)
        if not cid:
            yield event.plain_result("⚠️ 当前没有活动的对话，无法压缩哦～")
            return

        conv = await conv_mgr.get_conversation(umo, cid)
        if not conv:
            yield event.plain_result("⚠️ 找不到当前对话记录，无法压缩哦～")
            return

        history: list[dict] = json.loads(conv.history) if isinstance(conv.history, str) else (conv.history or [])
        non_system = [m for m in history if m.get("role") != "system"]

        if mode == "clear":
            await conv_mgr.update_conversation(
                unified_msg_origin=umo,
                conversation_id=cid,
                history=[],
            )
            yield event.plain_result(f"🗑️ 已清空全部历史记录（原有 {len(non_system)} 条消息）")
            return

        if len(non_system) < 2:
            yield event.plain_result("📭 当前对话内容太少，不需要压缩～")
            return

        if mode == "hard":
            # 仅截断，不使用 LLM
            msgs = [Message.model_validate(m) if isinstance(m, dict) else m for m in history]
            _truncator = ContextTruncator()
            truncated = _truncator.truncate_by_turns(msgs, keep_most_recent_turns=keep_recent)
            result_dicts = [m.model_dump() if hasattr(m, "model_dump") else dict(m) for m in truncated]
            await conv_mgr.update_conversation(
                unified_msg_origin=umo,
                conversation_id=cid,
                history=result_dicts,
            )
            after_cnt = len([m for m in result_dicts if m.get("role") != "system"])
            yield event.plain_result(
                f"✂️ 强制截断完成！\n"
                f"🔹 截断前：{len(non_system)} 条消息\n"
                f"🔹 截断后：{after_cnt} 条消息（保留最近 {keep_recent} 轮）"
            )
            return

        # LLM 摘要压缩
        provider = self.context.get_using_provider(umo)
        if not provider:
            yield event.plain_result("⚠️ 当前没有可用的 LLM Provider，尝试用 /compact hard 代替")
            return

        chars_before = len(json.dumps(history, ensure_ascii=False))
        yield event.plain_result(
            f"🗜️ 正在 LLM 摘要压缩（{len(non_system)} 条消息，约 {chars_before//4:,} tokens），保留最近 {keep_recent} 轮，请稍等～"
        )

        compressor = LLMSummaryCompressor(
            provider=provider,
            keep_recent=keep_recent,
        )
        messages = [Message.model_validate(m) if isinstance(m, dict) else m for m in history]
        compressed = await compressor(messages)

        compressed_dicts = [m.model_dump() if hasattr(m, "model_dump") else dict(m) for m in compressed]
        await conv_mgr.update_conversation(
            unified_msg_origin=umo,
            conversation_id=cid,
            history=compressed_dicts,
        )

        chars_after = len(json.dumps(compressed_dicts, ensure_ascii=False))
        after_cnt = len([m for m in compressed_dicts if m.get("role") != "system"])
        yield event.plain_result(
            f"✅ LLM 摘要压缩完成！\n"
            f"🔹 压缩前：{len(non_system)} 条消息 / ~{chars_before//4:,} tokens\n"
            f"🔹 压缩后：{after_cnt} 条消息 / ~{chars_after//4:,} tokens\n"
            f"🔹 压缩率：{(1 - chars_after/max(chars_before,1))*100:.1f}%（保留最近 {keep_recent} 轮 + 摘要）"
        )

    async def terminate(self) -> None:
        logger.info("[MemorySystem] 插件已卸载")
