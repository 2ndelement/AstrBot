"""
astrbot_plugin_task_control
============================
功能：
1. 后台长时任务工具（取代 shell_run）：
   bg_run    - 非阻塞启动后台 shell 任务，立即返回 job_id
   bg_await  - 等待任务完成（可设上限秒数，可多次调用）
   bg_status - 快速查询任务状态（不阻塞）
   bg_output - 分页读取任务的累积 stdout/stderr
   bg_kill   - 终止后台任务

   完成/超时后自动通过 cron_manager 注入新对话回合通知 agent。

2. 独立 HTTP 任务接口（Task API）：
   外部程序可直接 POST 派发任务，无需通过 AstrBot Dashboard CronJob API。
   - 端口/Host/Token/默认 session 均可在插件配置中设置
   - POST /task  → 立即（或定时）派发 LLM 任务
   - GET  /health → 健康检查

3. Pipeline 拦截器（monkey-patch FunctionToolExecutor.execute）：
   - 每次工具调用前自动记录到任务日志
   - 检查用户指令队列
   - 完全不依赖 LLM 主动配合，用户通过命令直接控制任务链

4. 命令（直接响应，不走 LLM，并发处理）：
   /stop   - 强杀子进程 + 设置停止标志，下次工具调用时链路中断
   /status - 立即返回自动记录的工具调用日志（LLM 正在做什么）

工作原理：
   AstrBot 通过 asyncio.create_task 并发调度每条消息，无 per-session 锁。
   因此 /status 消息可在 LLM 工具链的任意 await 间隙被并发处理，立刻回复。
   monkey-patch 的拦截器在每次工具调用前检查全局状态，无需 LLM 配合。
   bg_run 任务在独立协程中运行，完成时通过 cron_manager 触发新的 agent 对话回合。
"""

import asyncio
import base64
import json
import os
import secrets
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext
from astrbot.core.astr_agent_tool_exec import FunctionToolExecutor
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.star.filter.command import CommandFilter
from astrbot.core.star.filter.command_group import CommandGroupFilter
from astrbot.core.utils.session_lock import session_lock_manager

# 延迟导入 listeners 模块（同目录）

# 用于跟踪 follow-up 消息历史
_follow_up_history: dict = {}

# 尝试导入核心的 follow_up 模块
try:
    from astrbot.core.pipeline.process_stage.follow_up import _ACTIVE_AGENT_RUNNERS

    _FOLLOW_UP_AVAILABLE = True
except ImportError:
    _FOLLOW_UP_AVAILABLE = False
    _ACTIVE_AGENT_RUNNERS = {}


_PLUGIN_DIR = Path(__file__).parent

# ── 全局会话状态 ──────────────────────────────────────────────────────────────

# log_key -> deque[(timestamp, str)]  自动任务日志（最近 200 条，按任务分组）
# log_key = "umo::conversation_id"（/new 后 cid 改变，旧对话日志自动隔离）
_task_logs: dict[str, deque] = {}

# log_key -> float  上次工具调用的时间戳（用于检测任务边界）
_last_tool_time: dict[str, float] = {}

# 连续两次工具调用超过此间隔则视为新任务（秒）
_TASK_GAP_SECS = 30.0

# umo -> deque[str]  用户注入的指令队列（最多 50 条，防止内存耗尽）
_cmd_queues: dict[str, deque] = {}

# umo -> bool  记录是否有中途插入指令（agent loop 运行时检测到用户消息）
_had_injection: dict[str, bool] = {}

# 本插件自身注册的指令名（wake prefix 被剥离后的纯命令词），防止被误判为注入消息
_PLUGIN_OWN_COMMANDS: frozenset[str] = frozenset(["status", "stop"])

# umo -> list[asyncio.subprocess.Process]
_procs: dict[str, list] = {}

# ── 后台任务（bg_run）全局状态 ─────────────────────────────────────────────────
# job_id -> {
#   "proc":          asyncio.subprocess.Process,
#   "cmd":           str,
#   "label":         str,
#   "umo":           str,
#   "session":       str,
#   "start_ts":      float,
#   "timeout_secs":  float | None,
#   "status":        "running" | "done" | "failed" | "timeout" | "killed",
#   "exit_code":     int | None,
#   "stdout_buf":    deque[str],   # 最近 N 行 stdout+stderr
#   "notify":        bool,
#   "notify_fn":     coroutine_function | None,  # 注入后 cron_manager dispatch
#   "monitor_task":  asyncio.Task | None,
# }
_bg_jobs: dict[str, dict] = {}
_BG_JOB_OUTPUT_MAXLINES = 2000  # 每个 job 最多保留多少行输出

# 原始 execute classmethod 函数（用于恢复）
_original_execute_func: object | None = None
# 标记是否已安装 patch
_patch_installed: bool = False


def _conv_log_key(umo: str, cid: str | None) -> str:
    """对话级日志 key：umo + 当前 conversation_id，/new 后自动隔离。"""
    return f"{umo}::{cid}" if cid else umo


def _get_active_log_key(umo: str) -> str | None:
    """按最近活跃时间找到该 umo 对应的 log_key（同步，用于注入记录）。"""
    best_key, best_time = None, 0.0
    for key, t in _last_tool_time.items():
        if (key == umo or key.startswith(umo + "::")) and t > best_time:
            best_time, best_key = t, key
    return best_key


# ── 状态图片生成 ───────────────────────────────────────────────────────────────


def _tw(draw, text: str, font) -> int:
    """文本像素宽度（兼容 Pillow 12）。"""
    return draw.textbbox((0, 0), text, font=font)[2]


def _fit(draw, text: str, font, max_px: int) -> str:
    """将文本截断使其不超过 max_px 宽，超出时末尾加 '…'。"""
    if _tw(draw, text, font) <= max_px:
        return text
    for i in range(len(text) - 1, 0, -1):
        candidate = text[:i] + "…"
        if _tw(draw, candidate, font) <= max_px:
            return candidate
    return "…"


async def _render_status_image(
    logs: list,
    state: str,
    cid_short: str,
    max_count: int = 100,
    proc_count: int = 0,
) -> bytes:
    """自定义 PIL 暗色卡片渲染器，输出 PNG bytes。
    日志条目格式：
      "──── 新任务 @ HH:MM:SS ────"  → 任务分隔符
      "TRIGGER:<text>"               → 触发指令（紧跟分隔符）
      "INJECT:<text>"                → 用户中途注入
      其他                           → 工具调用
    显示顺序：从旧到新（顶=最早，底=最新），构成时间轴。
    """
    from io import BytesIO

    from PIL import Image, ImageDraw

    from astrbot.core.utils.t2i.local_strategy import FontManager

    # ── 尺寸 & 间距 ───────────────────────────────────────────
    W = 740  # 卡片总宽
    PH = 28  # 水平 padding
    X0 = PH
    X1 = W - PH
    CW = X1 - X0  # 内容区宽度

    # ── 颜色 ─────────────────────────────────────────────────
    C_BG = (11, 13, 20)
    C_CARD = (18, 22, 34)
    C_HDR = (22, 27, 44)
    C_SEP = (36, 42, 64)
    C_FOOT_BG = (13, 16, 26)
    C_WHITE = (226, 232, 240)
    C_MUTED = (72, 86, 110)
    C_TEXT = (200, 210, 225)
    C_ACCENT = (100, 120, 255)
    C_TOOL = (130, 142, 250)  # indigo  - 工具调用
    C_TASK = (50, 210, 150)  # emerald - 任务分隔
    C_TRIG = (145, 162, 182)  # slate   - 触发指令
    C_INJ = (250, 190, 36)  # amber   - 注入
    C_ERR = (248, 112, 112)  # red     - 错误

    # 状态徽章
    if "进行中" in state:
        S_BG, S_FG, S_BD = (18, 46, 34), C_TASK, (20, 98, 50)
        s_lbl = "运行中"
    elif "已完成" in state:
        S_BG, S_FG, S_BD = (18, 36, 58), (96, 165, 250), (28, 56, 92)
        s_lbl = "已完成"
    else:
        S_BG, S_FG, S_BD = (28, 30, 42), C_MUTED, C_SEP
        s_lbl = "空  闲"

    # ── 字体 ─────────────────────────────────────────────────
    F16 = FontManager.get_font(16)
    F18 = FontManager.get_font(18)
    F20 = FontManager.get_font(20)
    F22 = FontManager.get_font(22)
    F28 = FontManager.get_font(28)

    # ── 行高 ─────────────────────────────────────────────────
    H_HDR = 80
    H_LSEP = 30  # "TASK LOG" 标签行
    H_SEP = 42  # 任务分隔符
    H_TRIG = 28  # 触发指令
    H_TOOL = 34  # 工具调用
    H_INJ = 34  # 注入
    H_EMPTY = 72
    H_EXTRA = 42
    H_FOOT = 44
    H_PAD_T = 14  # 内容区顶部间距
    H_PAD_B = 14  # 内容区底部间距

    # ── 预处理日志条目（旧→新，构成时间轴）──────────────────
    raw = list(logs[-max_count:])
    rows = []  # (type, payload)
    for ts, msg in raw:
        t = time.strftime("%H:%M:%S", time.localtime(ts))
        if msg.startswith("────"):
            rows.append(("sep", msg.strip("─").strip()))
        elif msg.startswith("TRIGGER:"):
            rows.append(("trig", msg[8:]))
        elif msg.startswith("INJECT:"):
            rows.append(("inj", (t, msg[7:])))
        else:
            rows.append(("tool", (t, msg)))

    # ── 计算总高度 ──────────────────────────────────────────
    ROW_H_MAP = {"sep": H_SEP, "trig": H_TRIG, "inj": H_INJ, "tool": H_TOOL}
    content_h = H_PAD_T + H_LSEP
    content_h += sum(ROW_H_MAP.get(r, H_TOOL) for r, _ in rows) if rows else H_EMPTY
    content_h += H_PAD_B

    extra_h = H_EXTRA if proc_count else 0
    CT = 14  # card top offset
    CB = 14  # card bottom inset
    total_h = CT + H_HDR + content_h + extra_h + H_FOOT + CB + 8

    # ── 创建画布 ─────────────────────────────────────────────
    img = Image.new("RGB", (W, total_h), C_BG)
    draw = ImageDraw.Draw(img)

    # 卡片阴影（深色描边）
    draw.rounded_rectangle((6, CT + 4, W - 6, total_h - CB + 3), radius=18, fill=C_SEP)
    # 卡片主体
    draw.rounded_rectangle((6, CT, W - 6, total_h - CB), radius=18, fill=C_CARD)

    # ── 标题栏 ───────────────────────────────────────────────
    HDR_T = CT
    HDR_B = CT + H_HDR
    # 标题背景（圆角顶，平底）
    draw.rounded_rectangle((6, HDR_T, W - 6, HDR_B), radius=18, fill=C_HDR)
    draw.rectangle((6, HDR_T + 18, W - 6, HDR_B), fill=C_HDR)
    draw.line((X0, HDR_B, X1, HDR_B), fill=C_SEP, width=1)

    # 标题文字
    hy = HDR_T + 16
    draw.text((X0, hy), "AstrBot", font=F28, fill=C_ACCENT)
    aw = _tw(draw, "AstrBot", F28)
    # 小分隔点
    draw.ellipse((X0 + aw + 8, hy + 14, X0 + aw + 15, hy + 21), fill=C_MUTED)
    draw.text((X0 + aw + 22, hy + 5), "任务状态", font=F22, fill=C_WHITE)
    draw.text((X0, hy + 40), "对话  " + cid_short, font=F18, fill=C_MUTED)

    # 状态徽章
    badge_txt = f"  {s_lbl}  "
    bw = _tw(draw, badge_txt, F20) + 4
    bh = 30
    bx = X1 - bw
    by = HDR_T + (H_HDR - bh) // 2
    draw.rounded_rectangle(
        (bx, by, bx + bw, by + bh), radius=8, fill=S_BG, outline=S_BD, width=1
    )
    draw.text(
        (
            bx + (_tw(draw, badge_txt, F20) - _tw(draw, s_lbl.strip(), F20)) // 2 + 2,
            by + (bh - 20) // 2,
        ),
        s_lbl.strip(),
        font=F20,
        fill=S_FG,
    )

    # ── 内容区 ───────────────────────────────────────────────
    y = HDR_B + H_PAD_T

    # "TASK LOG" 标签
    tl = "TASK  LOG"
    draw.text((X0, y + 4), tl, font=F16, fill=C_MUTED)
    tlw = _tw(draw, tl, F16)
    draw.line((X0 + tlw + 10, y + 11, X1, y + 11), fill=C_SEP, width=1)
    y += H_LSEP

    SPINE_X = X0 + 11  # 时间轴纵线 x

    if not rows:
        draw.text(
            (X0 + 24, y + (H_EMPTY - 22) // 2),
            "暂无工具调用记录",
            font=F22,
            fill=C_MUTED,
        )
        y += H_EMPTY
    else:
        # 计算时间轴纵线的 y 范围（第一行中心 → 最后一行中心）
        spine_ys = []
        _y_tmp = y
        for rtype, _ in rows:
            rh = ROW_H_MAP.get(rtype, H_TOOL)
            if rtype in ("sep", "tool", "inj"):
                spine_ys.append(_y_tmp + rh // 2)
            _y_tmp += rh
        if len(spine_ys) >= 2:
            draw.line(
                (SPINE_X, spine_ys[0], SPINE_X, spine_ys[-1]), fill=C_SEP, width=2
            )

        RX = X0 + 26  # 行内容起始 x（时间轴右侧）

        for rtype, data in rows:
            rh = ROW_H_MAP.get(rtype, H_TOOL)
            mid_y = y + rh // 2

            if rtype == "sep":
                # ── 任务分隔符 ──────────────────────────────
                # 菱形节点
                draw.polygon(
                    [
                        (SPINE_X, mid_y - 7),
                        (SPINE_X + 7, mid_y),
                        (SPINE_X, mid_y + 7),
                        (SPINE_X - 7, mid_y),
                    ],
                    fill=C_TASK,
                )
                label_txt = data
                lx_txt = RX
                # 左短线
                draw.line((RX, mid_y, RX + 6, mid_y), fill=C_SEP, width=1)
                lx_txt += 10
                draw.text((lx_txt, mid_y - 11), label_txt, font=F18, fill=C_TASK)
                ltw = _tw(draw, label_txt, F18)
                # 右延伸线
                draw.line((lx_txt + ltw + 8, mid_y, X1, mid_y), fill=C_SEP, width=1)

            elif rtype == "trig":
                # ── 触发指令（悬挂在分隔符下，无轴节点）──
                bar_x = RX + 2
                draw.rectangle((bar_x, y + 4, bar_x + 3, y + rh - 4), fill=C_TRIG)
                trig_txt = "触发: " + _fit(draw, data, F18, CW - 44)
                draw.text(
                    (bar_x + 10, y + (rh - 18) // 2), trig_txt, font=F18, fill=C_TRIG
                )

            elif rtype == "inj":
                # ── 用户注入（箭头节点，琥珀色）───────────
                t_str, msg_txt = data
                # 箭头节点
                draw.polygon(
                    [
                        (SPINE_X - 6, mid_y - 5),
                        (SPINE_X + 6, mid_y),
                        (SPINE_X - 6, mid_y + 5),
                    ],
                    fill=C_INJ,
                )
                # 时间
                draw.text((RX, y + (rh - 18) // 2), t_str, font=F18, fill=C_MUTED)
                tw = _tw(draw, t_str, F18)
                # 标签
                tag = " [注入] "
                draw.text((RX + tw, y + (rh - 18) // 2), tag, font=F18, fill=C_INJ)
                tgw = _tw(draw, tag, F18)
                # 消息
                rest = _fit(draw, msg_txt, F18, CW - 26 - tw - tgw)
                draw.text(
                    (RX + tw + tgw, y + (rh - 18) // 2), rest, font=F18, fill=C_TEXT
                )

            elif rtype == "tool":
                # ── 工具调用（圆点节点）────────────────────
                t_str, msg_txt = data
                is_err = any(w in msg_txt for w in ("错误", "失败", "Error", "error"))
                dot_c = C_ERR if is_err else C_TOOL
                name_c = C_ERR if is_err else C_TOOL
                # 圆点
                draw.ellipse(
                    (SPINE_X - 5, mid_y - 5, SPINE_X + 5, mid_y + 5), fill=dot_c
                )
                # 时间
                draw.text((RX, y + (rh - 18) // 2), t_str, font=F18, fill=C_MUTED)
                tw = _tw(draw, t_str, F18)
                msg_x = RX + tw + 10
                avail_w = X1 - msg_x
                # 工具名 + 参数分色
                paren = msg_txt.find("(")
                if 0 < paren <= 32:
                    nm = msg_txt[:paren]
                    rest_msg = _fit(
                        draw, msg_txt[paren:], F18, avail_w - _tw(draw, nm, F18)
                    )
                    draw.text((msg_x, y + (rh - 18) // 2), nm, font=F18, fill=name_c)
                    draw.text(
                        (msg_x + _tw(draw, nm, F18), y + (rh - 18) // 2),
                        rest_msg,
                        font=F18,
                        fill=C_TEXT,
                    )
                else:
                    draw.text(
                        (msg_x, y + (rh - 18) // 2),
                        _fit(draw, msg_txt, F18, avail_w),
                        font=F18,
                        fill=name_c,
                    )

            y += rh

    # ── 附加信息（子进程）───────────────────────────────────
    if proc_count:
        draw.line((X0, y, X1, y), fill=C_SEP, width=1)
        y += 6
        draw.text(
            (X0 + 4, y + (H_EXTRA - 20) // 2),
            f"[后台]  运行中子进程  {proc_count}  个",
            font=F20,
            fill=C_INJ,
        )
        y += H_EXTRA

    # ── 页脚 ─────────────────────────────────────────────────
    FOOT_T = total_h - CB - H_FOOT
    # 页脚背景（圆角底，平顶）
    draw.rounded_rectangle((6, FOOT_T, W - 6, total_h - CB), radius=18, fill=C_FOOT_BG)
    draw.rectangle((6, FOOT_T, W - 6, FOOT_T + 18), fill=C_FOOT_BG)
    draw.line((X0, FOOT_T, X1, FOOT_T), fill=C_SEP, width=1)

    n_shown = min(len(logs), max_count)
    gen_time = time.strftime("%m-%d %H:%M:%S")
    draw.text((X0, FOOT_T + 14), f"共  {n_shown}  条记录", font=F18, fill=C_MUTED)
    gw = _tw(draw, gen_time, F18)
    draw.text((X1 - gw, FOOT_T + 14), gen_time, font=F18, fill=C_MUTED)

    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _auto_log(log_key: str, tool_name: str, tool_args: dict, trigger: str = "") -> None:
    """记录工具调用到对话日志，超过空闲间隔自动插入任务分隔符。"""
    now = time.time()
    q = _task_logs.setdefault(log_key, deque(maxlen=200))
    last = _last_tool_time.get(log_key, 0.0)

    # 新任务边界：首次调用或距上次超过 _TASK_GAP_SECS
    if now - last > _TASK_GAP_SECS:
        task_start = time.strftime("%H:%M:%S", time.localtime(now))
        q.append((now, f"──── 新任务 @ {task_start} ────"))
        if trigger:
            q.append((now, f"TRIGGER:{trigger}"))

    _last_tool_time[log_key] = now

    # 只取前 3 个参数防止日志过长
    short_args = {}
    for i, (k, v) in enumerate(tool_args.items()):
        if i >= 3:
            short_args["..."] = f"({len(tool_args) - 3} more)"
            break
        sv = str(v)
        short_args[k] = sv[:80] + "…" if len(sv) > 80 else sv
    args_str = ", ".join(f"{k}={v!r}" for k, v in short_args.items())
    q.append((now, f"{tool_name}({args_str})"))


def _register(umo: str, proc) -> None:
    _procs.setdefault(umo, []).append(proc)


def _unregister(umo: str, proc) -> None:
    lst = _procs.get(umo)
    if lst:
        try:
            lst.remove(proc)
        except ValueError:
            pass


# ── Pipeline 拦截器 ───────────────────────────────────────────────────────────


def _install_patch() -> None:
    global _original_execute_func, _patch_installed
    if _patch_installed:
        return

    import mcp
    import mcp.types

    _original_execute_func = FunctionToolExecutor.__dict__["execute"].__func__
    orig = _original_execute_func

    @classmethod
    async def _intercepted_execute(cls, tool, run_context, **tool_args):
        umo = run_context.context.event.unified_msg_origin

        # 获取当前对话 ID（/new 后 cid 改变，日志自动隔离）
        try:
            cid = await run_context.context.context.conversation_manager.get_curr_conversation_id(
                umo
            )
        except Exception:
            cid = None
        log_key = _conv_log_key(umo, cid)

        # 在新任务边界提取触发指令，传给 _auto_log 嵌入日志
        _trig = ""
        _now = time.time()
        _last = _last_tool_time.get(log_key, 0.0)
        if _now - _last > _TASK_GAP_SECS:
            try:
                _trig = (run_context.context.event.message_str or "").strip()
            except Exception:
                pass

        # 1. 自动记录工具调用（对话级隔离，trigger 嵌入日志）
        _auto_log(log_key, tool.name, tool_args, trigger=_trig)

        # 1.5. 若 Task API 任务禁用了工具，拦截所有非通信工具
        try:
            cron_payload = run_context.context.event.get_extra("cron_payload") or {}
            if (
                cron_payload.get("task_tools_disabled")
                and tool.name != "send_message_to_user"
            ):
                logger.info(
                    "[TaskControl] Interceptor: tool blocked (allow_tools=false) for %s",
                    tool.name,
                )
                yield mcp.types.CallToolResult(
                    content=[
                        mcp.types.TextContent(
                            type="text",
                            text=(
                                f"⚠ Tool `{tool.name}` is disabled for this task "
                                "(allow_tools=false in the API request). "
                                "You may only send messages. Do NOT attempt other tool calls."
                            ),
                        )
                    ]
                )
                return
        except Exception:
            pass

        # 2. 正常执行
        async for result in orig(cls, tool, run_context, **tool_args):
            yield result

    FunctionToolExecutor.execute = _intercepted_execute
    _patch_installed = True
    logger.info(
        "[TaskControl] Pipeline interceptor installed on FunctionToolExecutor.execute"
    )


def _remove_patch() -> None:
    global _original_execute_func, _patch_installed
    if not _patch_installed or _original_execute_func is None:
        return
    FunctionToolExecutor.execute = classmethod(_original_execute_func)
    _original_execute_func = None
    _patch_installed = False
    logger.info(
        "[TaskControl] Pipeline interceptor removed from FunctionToolExecutor.execute"
    )


# ── 后台长时任务（bg_run / bg_await / bg_status / bg_output / bg_kill） ────────

_BG_JOB_COUNTER = 0
_bg_notify_fn = None  # async(note, session, name) set in TaskControlPlugin.initialize()
_BG_OUTPUT_MAXLINES = 2000


def _new_job_id() -> str:
    global _BG_JOB_COUNTER
    _BG_JOB_COUNTER += 1
    return f"j{_BG_JOB_COUNTER:04d}"


def _bg_tail(job_id: str, n: int = 60) -> list:
    job = _bg_jobs.get(job_id)
    return list(job["stdout_buf"])[-n:] if job else []


def _bg_summary(job_id: str, tail_lines: int = 60) -> str:
    job = _bg_jobs.get(job_id)
    if not job:
        return json.dumps({"error": f"Unknown job_id: {job_id!r}"}, ensure_ascii=False)
    elapsed = int(time.time() - job["start_ts"])
    return json.dumps(
        {
            "job_id": job_id,
            "label": job["label"],
            "status": job["status"],
            "exit_code": job["exit_code"],
            "elapsed_secs": elapsed,
            "cmd": job["cmd"][:200],
            "output_lines_buffered": len(job["stdout_buf"]),
            "tail": _bg_tail(job_id, tail_lines),
        },
        ensure_ascii=False,
    )


async def _drain_stream(stream, buf: deque, tag: str) -> None:
    """持续读取 stream 的每一行追加到 buf，直到 EOF。"""
    try:
        async for raw in stream:
            line = raw.decode(errors="replace").rstrip("\n\r")
            buf.append(f"[{tag}] {line}")
    except Exception:
        pass


async def _bg_monitor(job_id: str) -> None:
    """监控后台 job：drain 输出、强制超时、完成后通知 agent。"""
    job = _bg_jobs.get(job_id)
    if not job:
        return

    proc = job["proc"]
    buf: deque = job["stdout_buf"]
    timeout = job["timeout_secs"]
    done_event: asyncio.Event = job["_done_event"]

    drain_tasks = []
    if proc.stdout:
        drain_tasks.append(asyncio.create_task(_drain_stream(proc.stdout, buf, "OUT")))
    if proc.stderr:
        drain_tasks.append(asyncio.create_task(_drain_stream(proc.stderr, buf, "ERR")))

    try:
        if timeout:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        else:
            await proc.wait()
        # 等 drain 任务最多 5s 收尾
        if drain_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*drain_tasks, return_exceptions=True), timeout=5
                )
            except asyncio.TimeoutError:
                for t in drain_tasks:
                    t.cancel()
        ec = proc.returncode
        job["exit_code"] = ec
        job["status"] = "done" if ec == 0 else "failed"

    except asyncio.TimeoutError:
        for t in drain_tasks:
            t.cancel()
        try:
            proc.terminate()
        except Exception:
            pass
        await asyncio.sleep(2)
        if proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
        job["status"] = "timeout"
        job["exit_code"] = -1
        buf.append(f"[SYS] Job timed out after {timeout}s. Process killed.")

    except asyncio.CancelledError:
        for t in drain_tasks:
            t.cancel()
        raise

    finally:
        job["end_ts"] = time.time()
        _unregister(job["umo"], proc)
        done_event.set()

    # 完成后通过 cron_manager 注入新对话回合通知 agent
    if job.get("notify") and _bg_notify_fn:
        elapsed = int(job.get("end_ts", time.time()) - job["start_ts"])
        tail_text = "\n".join(_bg_tail(job_id, 30))
        stat_cn = {
            "done": "完成",
            "failed": "失败",
            "timeout": "超时",
            "killed": "已终止",
        }.get(job["status"], job["status"])
        note = (
            f"[后台任务通知] {job_id} ({job['label']}) 已{stat_cn}\n"
            f"退出码: {job['exit_code']}  耗时: {elapsed}s\n"
            f"命令: {job['cmd'][:300]}\n"
            f"--- 最后输出 ---\n{tail_text}"
        )
        try:
            await _bg_notify_fn(
                note=note, session=job["session"], name=f"bg_job_{job_id}"
            )
        except Exception as exc:
            logger.error("[BgJob] Notify failed for %s: %s", job_id, exc)

    logger.info("[BgJob] %s → %s (exit=%s)", job_id, job["status"], job["exit_code"])


def _check_bg_admin(context: ContextWrapper) -> str | None:
    try:
        cfg = context.context.context.get_config(
            umo=context.context.event.unified_msg_origin
        )
        settings = cfg.get("provider_settings", {})
        if settings.get("computer_use_require_admin", True):
            if context.context.event.role != "admin":
                uid = context.context.event.get_sender_id()
                return f"error: Permission denied. Requires admin role. User ID: {uid}."
    except Exception:
        pass
    return None


class BgRunTool(FunctionTool):
    """bg_run: 非阻塞启动后台 shell 任务，立即返回 job_id。"""

    def __init__(self):
        super().__init__(
            name="bg_run",
            description=(
                "Start a shell command in the BACKGROUND and return immediately with a job_id.\n"
                "Use for long-running tasks (model training, builds, downloads, servers) "
                "that would block the agent if run synchronously.\n\n"
                "Workflow:\n"
                "  1. bg_run(cmd, label='task-name')       → returns job_id instantly\n"
                "  2. (optional) bg_await(job_id, max_secs=60) → wait up to N seconds\n"
                "  3. bg_status(job_id)                    → poll progress any time\n"
                "  4. bg_output(job_id)                    → read accumulated stdout\n"
                "  5. Automatic notification when done     → new agent turn with results\n\n"
                "Set notify=true (default) for auto-notification on completion/timeout.\n"
                "Set timeout_secs for safety on bounded tasks."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Bash command to run.",
                    },
                    "label": {
                        "type": "string",
                        "description": "Short human-readable label (e.g. 'model-training', 'data-download').",
                    },
                    "timeout_secs": {
                        "type": "number",
                        "description": "Auto-kill timeout in seconds. 0 or omitted = no timeout.",
                    },
                    "notify": {
                        "type": "boolean",
                        "description": "Send auto-notification to this session when job finishes. Default true.",
                        "default": True,
                    },
                    "env": {
                        "type": "object",
                        "description": "Extra environment variables.",
                        "additionalProperties": {"type": "string"},
                    },
                },
                "required": ["command"],
            },
        )

    async def call(
        self,
        context: ContextWrapper[AstrAgentContext],
        command: str,
        label: str = "",
        timeout_secs: float | None = None,
        notify: bool = True,
        env: dict | None = None,
        **_,
    ) -> ToolExecResult:
        if err := _check_bg_admin(context):
            return err

        umo = context.context.event.unified_msg_origin
        run_env = os.environ.copy()
        if env:
            run_env.update({str(k): str(v) for k, v in env.items()})

        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=run_env,
        )

        job_id = _new_job_id()
        timeout = float(timeout_secs) if timeout_secs and timeout_secs > 0 else None
        buf: deque = deque(maxlen=_BG_OUTPUT_MAXLINES)
        _bg_jobs[job_id] = {
            "proc": proc,
            "cmd": command,
            "label": label or command[:60],
            "umo": umo,
            "session": umo,
            "start_ts": time.time(),
            "end_ts": None,
            "timeout_secs": timeout,
            "status": "running",
            "exit_code": None,
            "stdout_buf": buf,
            "notify": notify,
            "monitor_task": None,
            "_done_event": asyncio.Event(),
        }
        _register(umo, proc)
        monitor = asyncio.create_task(_bg_monitor(job_id))
        _bg_jobs[job_id]["monitor_task"] = monitor

        return json.dumps(
            {
                "job_id": job_id,
                "pid": proc.pid,
                "label": _bg_jobs[job_id]["label"],
                "status": "running",
                "started_at": time.strftime("%H:%M:%S"),
                "timeout_secs": timeout,
                "notify_on_done": notify,
                "hint": (
                    f"Job started in background. "
                    f"{'Will notify this session on completion. ' if notify else ''}"
                    f"Use bg_await('{job_id}', max_secs=60) to wait, "
                    f"bg_status('{job_id}') to poll, or bg_output('{job_id}') to read output."
                ),
            },
            ensure_ascii=False,
        )


class BgAwaitTool(FunctionTool):
    """bg_await: 等待后台任务完成（有上限），可分多次调用。"""

    def __init__(self):
        super().__init__(
            name="bg_await",
            description=(
                "Wait for a background job to finish, but only for up to `max_secs` seconds.\n"
                "- If the job finishes within max_secs: returns final status + output tail.\n"
                "- If still running after max_secs: returns 'still_running' + current output.\n"
                "  You can call bg_await again, or let the auto-notification handle it.\n"
                "- Safe to call multiple times on the same job_id.\n"
                "Use max_secs < provider tool_call_timeout to avoid being forcibly cancelled."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID from bg_run."},
                    "max_secs": {
                        "type": "number",
                        "description": "Max seconds to wait. Default 60. Keep below tool_call_timeout.",
                        "default": 60,
                    },
                    "tail_lines": {
                        "type": "integer",
                        "description": "How many output lines to include in result. Default 80.",
                        "default": 80,
                    },
                },
                "required": ["job_id"],
            },
        )

    async def call(
        self,
        context: ContextWrapper[AstrAgentContext],
        job_id: str,
        max_secs: float = 60,
        tail_lines: int = 80,
        **_,
    ) -> ToolExecResult:
        job = _bg_jobs.get(job_id)
        if not job:
            return json.dumps(
                {"error": f"Unknown job_id: {job_id!r}"}, ensure_ascii=False
            )

        if job["status"] != "running":
            return _bg_summary(job_id, tail_lines)

        done_event: asyncio.Event = job["_done_event"]
        try:
            await asyncio.wait_for(
                asyncio.shield(done_event.wait()), timeout=float(max_secs)
            )
        except asyncio.TimeoutError:
            pass  # still running, return current state

        elapsed = int(time.time() - job["start_ts"])
        return json.dumps(
            {
                "job_id": job_id,
                "label": job["label"],
                "status": job["status"],
                "exit_code": job["exit_code"],
                "elapsed_secs": elapsed,
                "output_lines_buffered": len(job["stdout_buf"]),
                "tail": _bg_tail(job_id, tail_lines),
                "hint": (
                    "Job still running. Call bg_await again or wait for auto-notification."
                    if job["status"] == "running"
                    else ""
                ),
            },
            ensure_ascii=False,
        )


class BgStatusTool(FunctionTool):
    """bg_status / bg_list_jobs: 快速查询，不阻塞。"""

    def __init__(self):
        super().__init__(
            name="bg_status",
            description=(
                "Get the status of a background job (non-blocking), or list all jobs.\n"
                "- Provide job_id to query one specific job.\n"
                "- Omit job_id (or pass 'list') to list all active/recent jobs.\n"
                "Returns status, elapsed time, and last few output lines."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Job ID to query. Omit or pass 'list' to list all jobs.",
                    },
                    "tail_lines": {
                        "type": "integer",
                        "description": "Output lines to include. Default 20.",
                        "default": 20,
                    },
                },
                "required": [],
            },
        )

    async def call(
        self,
        context: ContextWrapper[AstrAgentContext],
        job_id: str = "",
        tail_lines: int = 20,
        **_,
    ) -> ToolExecResult:
        if not job_id or job_id == "list":
            if not _bg_jobs:
                return json.dumps(
                    {"jobs": [], "hint": "No background jobs."}, ensure_ascii=False
                )
            jobs_summary = []
            for jid, job in _bg_jobs.items():
                elapsed = int(time.time() - job["start_ts"])
                jobs_summary.append(
                    {
                        "job_id": jid,
                        "label": job["label"],
                        "status": job["status"],
                        "exit_code": job["exit_code"],
                        "elapsed_secs": elapsed,
                        "pid": job["proc"].pid if job["proc"] else None,
                    }
                )
            return json.dumps({"jobs": jobs_summary}, ensure_ascii=False)
        return _bg_summary(job_id, tail_lines)


class BgOutputTool(FunctionTool):
    """bg_output: 读取后台任务的累积输出（支持分页）。"""

    def __init__(self):
        super().__init__(
            name="bg_output",
            description=(
                "Read accumulated stdout/stderr from a background job.\n"
                "- lines: how many lines to return (default 100).\n"
                "- offset: 0=oldest, -N=last N lines (default -100 = last 100 lines).\n"
                "Output is buffered in memory (max 2000 lines per job)."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID from bg_run."},
                    "lines": {
                        "type": "integer",
                        "description": "Number of lines to return. Default 100.",
                        "default": 100,
                    },
                    "offset": {
                        "type": "integer",
                        "description": (
                            "Start index into the buffer. "
                            "Negative = from end (e.g. -100 = last 100 lines). Default -100."
                        ),
                        "default": -100,
                    },
                },
                "required": ["job_id"],
            },
        )

    async def call(
        self,
        context: ContextWrapper[AstrAgentContext],
        job_id: str,
        lines: int = 100,
        offset: int = -100,
        **_,
    ) -> ToolExecResult:
        job = _bg_jobs.get(job_id)
        if not job:
            return json.dumps(
                {"error": f"Unknown job_id: {job_id!r}"}, ensure_ascii=False
            )
        buf = list(job["stdout_buf"])
        total = len(buf)
        if offset < 0:
            start = max(0, total + offset)
        else:
            start = min(offset, total)
        chunk = buf[start : start + lines]
        return json.dumps(
            {
                "job_id": job_id,
                "label": job["label"],
                "status": job["status"],
                "total_lines_buffered": total,
                "returned_range": [start, start + len(chunk)],
                "output": chunk,
            },
            ensure_ascii=False,
        )


class BgKillTool(FunctionTool):
    """bg_kill: 终止后台任务。"""

    def __init__(self):
        super().__init__(
            name="bg_kill",
            description=(
                "Terminate a running background job.\n"
                "Sends SIGTERM first, then SIGKILL after 2 seconds if still alive."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string", "description": "Job ID to kill."},
                },
                "required": ["job_id"],
            },
        )

    async def call(
        self,
        context: ContextWrapper[AstrAgentContext],
        job_id: str,
        **_,
    ) -> ToolExecResult:
        if err := _check_bg_admin(context):
            return err
        job = _bg_jobs.get(job_id)
        if not job:
            return json.dumps(
                {"error": f"Unknown job_id: {job_id!r}"}, ensure_ascii=False
            )
        if job["status"] != "running":
            return json.dumps(
                {
                    "job_id": job_id,
                    "status": job["status"],
                    "message": "Job already finished.",
                },
                ensure_ascii=False,
            )
        proc = job["proc"]
        try:
            proc.terminate()
        except Exception:
            pass
        await asyncio.sleep(2)
        if proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
        job["status"] = "killed"
        job["exit_code"] = proc.returncode if proc.returncode is not None else -9
        job["stdout_buf"].append("[SYS] Job killed by agent.")
        job["_done_event"].set()
        return json.dumps(
            {
                "job_id": job_id,
                "status": "killed",
                "message": "Job terminated.",
            },
            ensure_ascii=False,
        )


# ── bg tool singletons ────────────────────────────────────────────────────────
_bg_run_tool = BgRunTool()
_bg_await_tool = BgAwaitTool()
_bg_status_tool = BgStatusTool()
_bg_output_tool = BgOutputTool()
_bg_kill_tool = BgKillTool()

# ── Task API HTTP 服务器 ───────────────────────────────────────────────────────


# 前端管理页面 HTML（在启动时从文件读取，编译为字符串）
def _load_frontend_html() -> str:
    p = _PLUGIN_DIR / "frontend.html"
    if p.exists():
        return p.read_text(encoding="utf-8")
    return "<html><body><h1>frontend.html not found</h1></body></html>"


class TaskApiServer:
    """独立 aiohttp HTTP 服务器，外部程序可直接 POST 派发 LLM 任务或主动发送消息。"""

    def __init__(
        self, cron_manager, cfg: dict, plugin_context, listener_manager=None
    ) -> None:
        self.cron_manager = cron_manager
        self.plugin_context = plugin_context
        self.listener_manager = listener_manager
        self.host: str = cfg.get("task_api_host", "0.0.0.0")
        self.port: int = int(cfg.get("task_api_port", 15700))
        self.token_enabled: bool = bool(cfg.get("task_api_token_enabled", True))
        self.token: str = str(cfg.get("task_api_token", "")).strip()
        self.default_session: str = str(cfg.get("task_api_default_session", "")).strip()

        if self.token_enabled and not self.token:
            self.token = secrets.token_urlsafe(32)
            logger.warning(
                "[TaskAPI] 未配置 Token，已自动生成：%s\n"
                "          请在插件配置页面 task_api_token 字段保存此 Token。",
                self.token,
            )

        self._runner = None
        self._site = None

    def _check_auth(self, request) -> bool:
        if not self.token_enabled:
            return True
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:].strip() == self.token
        api_key = request.headers.get("X-API-Key", "")
        return api_key == self.token

    async def _handle_health(self, request):
        from aiohttp.web import Response

        return Response(
            text=json.dumps(
                {"status": "ok", "service": "task-api"}, ensure_ascii=False
            ),
            content_type="application/json",
        )

    async def _handle_task(self, request):
        from aiohttp.web import Response

        def _resp(
            status: str, message: str, data: dict | None = None, http_status: int = 200
        ):
            body = {"status": status, "message": message}
            if data:
                body["data"] = data
            return Response(
                text=json.dumps(body, ensure_ascii=False),
                content_type="application/json",
                status=http_status,
            )

        if not self._check_auth(request):
            return _resp("error", "Unauthorized", http_status=401)

        try:
            body = await request.json()
        except Exception:
            return _resp("error", "Invalid JSON body", http_status=400)

        # ── 公共字段 ─────────────────────────────────────────────────────────
        action = str(body.get("action", "task")).strip().lower()
        if action not in ("task", "message"):
            return _resp("error", "action must be 'task' or 'message'", http_status=400)

        session = str(body.get("session", "") or self.default_session).strip()
        if not session:
            return _resp(
                "error",
                "session is required (no default_session configured)",
                http_status=400,
            )

        # ── action=message：直接向指定会话发送文本，不经过 LLM ─────────────────
        if action == "message":
            text = str(body.get("text", "")).strip()
            if not text:
                return _resp(
                    "error", "text is required for action=message", http_status=400
                )
            try:
                ok = await self.plugin_context.send_message(
                    session, MessageChain().message(text)
                )
                if not ok:
                    return _resp(
                        "error",
                        f"No platform adapter found for session '{session}'",
                        http_status=404,
                    )
            except Exception as e:
                logger.exception("[TaskAPI] Failed to send message")
                return _resp("error", str(e), http_status=500)
            logger.info("[TaskAPI] Message sent to session=%s", session)
            return _resp("ok", "Message sent", data={"session": session})

        # ── action=task：通过 CronJobManager 派发 LLM 任务 ────────────────────
        note = str(body.get("note", "")).strip()
        if not note:
            return _resp("error", "note is required for action=task", http_status=400)

        sender_id = str(body.get("sender_id", "")).strip() or None
        allow_tools_req = bool(body.get("allow_tools", False))

        # allow_tools=true 仅在 sender_id 具有管理员权限时生效，防止外部提权
        allow_tools = False
        if allow_tools_req and sender_id:
            try:
                cfg = self.plugin_context.get_config(umo=session)
                admin_ids = cfg.get("admins_id", [])
                allow_tools = str(sender_id) in [str(a) for a in admin_ids]
            except Exception:
                allow_tools = False

        name = str(body.get("name", "") or "task_api").strip()
        run_at_str = body.get("run_at")
        cron_expression = body.get("cron_expression")
        run_once = bool(body.get("run_once", True))

        run_at_dt = None
        if run_at_str:
            try:
                run_at_dt = datetime.fromisoformat(str(run_at_str))
            except Exception:
                return _resp(
                    "error",
                    "run_at must be ISO datetime, e.g. 2026-01-01T09:00:00+08:00",
                    http_status=400,
                )
        elif not cron_expression:
            run_at_dt = datetime.now(timezone.utc) + timedelta(seconds=2)
            run_once = True

        payload = {
            "session": session,
            "note": note,
            # sender_id 决定 role（是否 admin），无 sender_id 则以 member 身份运行
            **({"sender_id": sender_id} if sender_id else {}),
            # task_tools_disabled 由拦截器读取，allow_tools=false 时阻断所有非通信工具
            **({"task_tools_disabled": True} if not allow_tools else {}),
        }

        try:
            job = await self.cron_manager.add_active_job(
                name=name,
                cron_expression=str(cron_expression) if cron_expression else None,
                payload=payload,
                description=note[:200],
                run_once=run_once,
                run_at=run_at_dt,
            )
        except Exception as e:
            logger.exception("[TaskAPI] Failed to create job")
            return _resp("error", str(e), http_status=500)

        logger.info(
            "[TaskAPI] Job dispatched: %s (%s) → session=%s allow_tools=%s",
            job.job_id,
            job.name,
            session,
            allow_tools,
        )
        return _resp(
            "ok",
            "Task dispatched",
            data={
                "job_id": job.job_id,
                "name": job.name,
                "session": session,
                "allow_tools": allow_tools,
                "run_at": job.next_run_time.isoformat() if job.next_run_time else None,
            },
        )

    # ── Listener API 路由 ─────────────────────────────────────────────────────

    async def _handle_frontend(self, request):
        from aiohttp.web import Response

        return Response(text=_load_frontend_html(), content_type="text/html")

    async def _handle_list_listeners(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "ok", "data": []}),
                content_type="application/json",
            )
        listeners = self.listener_manager.list_listeners()
        # 附加最近事件
        for l in listeners:
            l["recent_events"] = self.listener_manager.get_events(l["name"], limit=20)
            l["raw_config"] = self.listener_manager.get_raw_config(l["name"])
        return Response(
            text=json.dumps({"status": "ok", "data": listeners}, ensure_ascii=False),
            content_type="application/json",
        )

    async def _handle_listener_start(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "error", "message": "No listener manager"}),
                content_type="application/json",
                status=503,
            )
        try:
            await self.listener_manager.start_listener(name)
            return Response(
                text=json.dumps(
                    {"status": "ok", "message": f"Listener '{name}' started"}
                ),
                content_type="application/json",
            )
        except Exception as e:
            return Response(
                text=json.dumps({"status": "error", "message": str(e)}),
                content_type="application/json",
                status=400,
            )

    async def _handle_listener_stop(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "error", "message": "No listener manager"}),
                content_type="application/json",
                status=503,
            )
        await self.listener_manager.stop_listener(name)
        return Response(
            text=json.dumps({"status": "ok", "message": f"Listener '{name}' stopped"}),
            content_type="application/json",
        )

    async def _handle_get_config(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "ok", "data": {}}),
                content_type="application/json",
            )
        cfg = self.listener_manager.get_config(name)
        return Response(
            text=json.dumps({"status": "ok", "data": cfg}, ensure_ascii=False),
            content_type="application/json",
        )

    async def _handle_get_config_raw(self, request):
        """返回未脱敏的原始配置（用于前端回显编辑，同样需要 auth）。"""
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "ok", "data": {}}),
                content_type="application/json",
            )
        cfg = self.listener_manager.get_raw_config(name)
        return Response(
            text=json.dumps({"status": "ok", "data": cfg}, ensure_ascii=False),
            content_type="application/json",
        )

    async def _handle_update_config(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "error", "message": "No listener manager"}),
                content_type="application/json",
                status=503,
            )
        try:
            body = await request.json()
        except Exception:
            return Response(
                text=json.dumps({"status": "error", "message": "Invalid JSON"}),
                content_type="application/json",
                status=400,
            )
        self.listener_manager.update_config(name, body)
        return Response(
            text=json.dumps({"status": "ok", "message": "Config updated"}),
            content_type="application/json",
        )

    async def _handle_get_events(self, request):
        from aiohttp.web import Response

        if not self._check_auth(request):
            return Response(
                text=json.dumps({"status": "error", "message": "Unauthorized"}),
                content_type="application/json",
                status=401,
            )
        name = request.match_info.get("name", "")
        limit = int(request.rel_url.query.get("limit", "50"))
        if not self.listener_manager:
            return Response(
                text=json.dumps({"status": "ok", "data": []}),
                content_type="application/json",
            )
        events = self.listener_manager.get_events(name, limit=limit)
        return Response(
            text=json.dumps({"status": "ok", "data": events}, ensure_ascii=False),
            content_type="application/json",
        )

    async def start(self) -> None:
        from aiohttp import web

        app = web.Application()
        # Task API
        app.router.add_get("/health", self._handle_health)
        app.router.add_post("/task", self._handle_task)
        # Frontend
        app.router.add_get("/", self._handle_frontend)
        # Listener management API
        app.router.add_get("/api/listeners", self._handle_list_listeners)
        app.router.add_post("/api/listeners/{name}/start", self._handle_listener_start)
        app.router.add_post("/api/listeners/{name}/stop", self._handle_listener_stop)
        app.router.add_get("/api/listeners/{name}/config", self._handle_get_config)
        app.router.add_get(
            "/api/listeners/{name}/config/raw", self._handle_get_config_raw
        )
        app.router.add_post("/api/listeners/{name}/config", self._handle_update_config)
        app.router.add_get("/api/listeners/{name}/events", self._handle_get_events)

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        logger.info(
            "[TaskAPI] Listening on http://%s:%d  (token_enabled=%s)  — UI: http://%s:%d/",
            self.host,
            self.port,
            self.token_enabled,
            self.host,
            self.port,
        )

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            logger.info("[TaskAPI] Server stopped.")


# ── Plugin class ──────────────────────────────────────────────────────────────


class TaskControlPlugin(Star):
    def __init__(self, context: Context, config=None) -> None:
        super().__init__(context, config)
        self._config = config
        self._task_api: TaskApiServer | None = None
        self._listener_manager = None

        # Remove stale tools from previous plugin versions
        for stale in (
            "astrbot_execute_shell",
            "task_log",
            "check_commands",
            "shell_run",
        ):
            try:
                context.provider_manager.llm_tools.remove_func(stale)
                logger.info("[TaskControl] Removed stale tool: %s", stale)
            except Exception:
                pass

        # Register bg tools
        for tool in (
            _bg_run_tool,
            _bg_await_tool,
            _bg_status_tool,
            _bg_output_tool,
            _bg_kill_tool,
        ):
            context.add_llm_tools(tool)

        # Install the pipeline interceptor
        _install_patch()

        logger.info(
            "[TaskControl] Ready. bg_run/bg_await/bg_status/bg_output/bg_kill registered."
        )

    async def initialize(self) -> None:
        """AstrBot 插件激活时调用，此时 cron_manager 已就绪。"""
        global _bg_notify_fn
        cfg = self._config or {}

        # 设置后台任务完成通知函数（复用 cron_manager 注入机制）
        cron_mgr = self.context.cron_manager
        if cron_mgr is not None:

            async def _dispatch_bg(note: str, session: str, name: str = "bg_job"):
                run_at = datetime.now(timezone.utc) + timedelta(seconds=2)
                payload = {"session": session, "note": note}
                await cron_mgr.add_active_job(
                    name=name,
                    cron_expression=None,
                    payload=payload,
                    description=note[:200],
                    run_once=True,
                    run_at=run_at,
                )

            _bg_notify_fn = _dispatch_bg
            logger.info("[TaskControl] bg_notify_fn configured via cron_manager.")
        else:
            logger.warning(
                "[TaskControl] cron_manager not available; bg_run notify disabled."
            )

        if not cfg.get("task_api_enabled", True):
            logger.info("[TaskAPI] Disabled by config.")
            return
        if cron_mgr is None:
            logger.warning(
                "[TaskAPI] cron_manager not available, Task API will not start."
            )
            return

        # 构建 dispatch 函数（供 Listener 调用）
        async def _dispatch(
            note: str, session: str, allow_tools: bool = False, name: str = "listener"
        ):
            run_at = datetime.now(timezone.utc) + timedelta(seconds=2)
            payload = {
                "session": session,
                "note": note,
                **({"task_tools_disabled": True} if not allow_tools else {}),
            }
            await cron_mgr.add_active_job(
                name=name,
                cron_expression=None,
                payload=payload,
                description=note[:200],
                run_once=True,
                run_at=run_at,
            )

        # 初始化并注册所有 Listener
        import importlib.util as _ilu

        _lspec = _ilu.spec_from_file_location(
            "tc_listeners", _PLUGIN_DIR / "listeners.py"
        )
        _lmod = _ilu.module_from_spec(_lspec)
        _lspec.loader.exec_module(_lmod)
        EventListenerManager = _lmod.EventListenerManager
        EmailListener = _lmod.EmailListener

        self._listener_manager = EventListenerManager(str(_PLUGIN_DIR), _dispatch)
        self._listener_manager.register(EmailListener)

        self._task_api = TaskApiServer(
            cron_mgr, cfg, self.context, self._listener_manager
        )
        await self._task_api.start()

    async def terminate(self) -> None:
        """插件卸载/重载时停止 HTTP 服务器和所有 Listener，取消后台任务监控协程。"""
        global _bg_notify_fn
        _bg_notify_fn = None
        # 取消所有仍在运行的 bg_monitor 协程（不 kill 进程，让它们继续跑）
        for job_id, job in list(_bg_jobs.items()):
            mt = job.get("monitor_task")
            if mt and not mt.done():
                mt.cancel()
        if self._listener_manager:
            await self._listener_manager.stop_all()
            self._listener_manager = None
        if self._task_api:
            await self._task_api.stop()
            self._task_api = None
        _remove_patch()

    # ── /stop ──────────────────────────────────────────────────────────────────

    @filter.platform_adapter_type(filter.PlatformAdapterType.ALL)
    @filter.regex(r".*", priority=100)
    async def on_passive_message_capture(self, event: AstrMessageEvent):
        """在 agent loop 运行时，仅记录是否有中途插入指令，不拦截事件，交由系统正常处理。"""
        umo = event.unified_msg_origin

        # 仅在 agent loop 持有会话锁时检测
        lock = session_lock_manager._locks.get(umo)
        if not (lock and lock.locked()):
            return

        msg = event.message_str.strip()
        # 跳过空消息和命令消息
        # 注意：AstrBot 的 waking_check 阶段会将唤醒前缀（如"/"）从 message_str 中剥离，
        # 所以 "/status" 到达此处时已变成 "status"，需单独过滤本插件的命令。
        if not msg or msg.startswith("/"):
            return
        _first_token = msg.split()[0].lower()
        if _first_token in _PLUGIN_OWN_COMMANDS:
            return
        # Skip messages that are handled by command filters — these are slash commands
        # like /status, /cmd, etc. that won't follow up to LLM tool calls.
        activated_handlers = event.get_extra("activated_handlers", default=[])
        for handler in activated_handlers:
            for f in handler.event_filters:
                if isinstance(f, (CommandFilter, CommandGroupFilter)):
                    return

        # 记录有中途插入指令；同时嵌入到当前活跃日志时间轴中
        _had_injection[umo] = True
        log_key = _get_active_log_key(umo)
        if log_key and log_key in _task_logs:
            _task_logs[log_key].append((time.time(), f"INJECT:{msg}"))
        logger.info("[TaskControl] Mid-run message detected for %s: %.80s", umo, msg)

    # ── /status ────────────────────────────────────────────────────────────────

    @filter.command("status")
    async def cmd_status(self, event: AstrMessageEvent):
        """立即返回工具调用日志（不走 LLM，并发响应；按任务分组，仅属于本会话）"""
        umo = event.unified_msg_origin

        # 获取当前对话 ID，与拦截器保持一致
        try:
            cid = await self.context.conversation_manager.get_curr_conversation_id(umo)
        except Exception:
            cid = None
        log_key = _conv_log_key(umo, cid)

        logs = list(_task_logs.get(log_key, []))
        proc_count = len(_procs.get(umo, []))
        last_t = _last_tool_time.get(log_key)
        max_count = int((self._config or {}).get("status_log_count", 100))

        cid_short = cid[:8] if cid else "unknown"
        idle_secs = int(time.time() - last_t) if last_t else None
        if not logs:
            state = "无记录"
        elif idle_secs is not None:
            state = (
                "进行中" if idle_secs < _TASK_GAP_SECS else f"已完成（{idle_secs}s 前）"
            )
        else:
            state = "无记录"

        try:
            img_bytes = await _render_status_image(
                logs=logs,
                state=state,
                cid_short=cid_short,
                max_count=max_count,
                proc_count=proc_count,
            )
            b64 = base64.b64encode(img_bytes).decode()
            yield event.make_result().base64_image(b64)
        except Exception as e:
            logger.error("[TaskControl] /status render failed: %s", e, exc_info=True)
            # 降级纯文本
            if not logs:
                text = f"工具调用日志为空（对话 {cid_short}）"
            else:
                lines = [f"{state}  对话 {cid_short}，最近 {max_count} 条："]
                for ts, msg in logs[-max_count:]:
                    t = time.strftime("%H:%M:%S", time.localtime(ts))
                    if msg.startswith("────"):
                        lines.append(f"\n{msg}")
                    elif msg.startswith("TRIGGER:"):
                        lines.append(f"  触发: {msg[8:]}")
                    elif msg.startswith("INJECT:"):
                        lines.append(f"  [{t}] [注入] {msg[7:]}")
                    else:
                        lines.append(f"  [{t}] {msg}")
                text = "\n".join(lines)
            if proc_count:
                text += f"\n[后台] 运行中子进程 {proc_count} 个"
            yield event.plain_result(text)


def get_plugin_instance(context: Context) -> Star:
    return TaskControlPlugin(context)
