"""
astrbot_plugin_task_control — Event Listener Manager
=====================================================
提供一个可扩展的外部事件监听框架：
  - EventListenerBase   : 所有 Listener 继承的抽象基类
  - EventListenerManager: 注册 / 启动 / 停止 / 持久化配置
  - EmailListener       : IMAP 邮件监听示例（子插件）
"""

from __future__ import annotations

import asyncio
import email
import email.header
import imaplib
import json
import os
import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from astrbot.api import logger


# ── 事件记录 ─────────────────────────────────────────────────────────────────

class ListenerEvent:
    __slots__ = ("ts", "source", "summary", "dispatched")

    def __init__(self, source: str, summary: str, dispatched: bool = False):
        self.ts = time.time()
        self.source = source
        self.summary = summary
        self.dispatched = dispatched

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "time": datetime.fromtimestamp(self.ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "source": self.source,
            "summary": self.summary,
            "dispatched": self.dispatched,
        }


# ── 抽象基类 ──────────────────────────────────────────────────────────────────

class EventListenerBase(ABC):
    """所有事件监听器的基类。子类只需实现 _run_loop() 即可。"""

    # 子类覆盖这些类属性
    name: str = "unnamed"
    display_name: str = "Unnamed Listener"
    description: str = ""
    # config_schema 格式：[{key, label, type("string"|"number"|"bool"|"password"|"text"|"list"), default, required, description}]
    config_schema: list[dict] = []

    def __init__(
        self,
        config: dict,
        dispatch_fn: Callable[..., Coroutine],
    ):
        self.config = config
        self._dispatch = dispatch_fn  # async (note, session, allow_tools, name) -> None
        self._running = False
        self._task: asyncio.Task | None = None
        self._events: deque[ListenerEvent] = deque(maxlen=100)
        self._last_error: str | None = None

    @property
    def status(self) -> str:
        if self._task is None:
            return "stopped"
        if self._task.done():
            exc = self._task.exception() if not self._task.cancelled() else None
            return f"error: {exc}" if exc else "stopped"
        return "running"

    def _record_event(self, source: str, summary: str, dispatched: bool = False):
        self._events.append(ListenerEvent(source, summary, dispatched))

    async def _do_dispatch(self, note: str, session: str, allow_tools: bool = False):
        try:
            await self._dispatch(
                note=note,
                session=session,
                allow_tools=allow_tools,
                name=f"listener_{self.name}",
            )
        except Exception as e:
            logger.exception("[Listener:%s] dispatch failed: %s", self.name, e)

    @abstractmethod
    async def _run_loop(self): ...

    async def start(self):
        if self._task and not self._task.done():
            return
        self._running = True
        self._last_error = None
        self._task = asyncio.create_task(self._safe_run(), name=f"listener_{self.name}")
        logger.info("[Listener:%s] Started.", self.name)

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=5)
            except Exception:
                pass
        logger.info("[Listener:%s] Stopped.", self.name)

    async def _safe_run(self):
        try:
            await self._run_loop()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._last_error = str(e)
            logger.exception("[Listener:%s] Unhandled error: %s", self.name, e)


# ── EventListenerManager ─────────────────────────────────────────────────────

class EventListenerManager:
    """管理所有已注册 Listener 的生命周期与配置持久化。"""

    CONFIG_FILENAME = "listeners_config.json"

    def __init__(self, plugin_dir: str, dispatch_fn: Callable):
        self._plugin_dir = plugin_dir
        self._dispatch_fn = dispatch_fn
        self._registry: dict[str, type[EventListenerBase]] = {}   # name → class
        self._instances: dict[str, EventListenerBase] = {}        # name → instance
        self._configs: dict[str, dict] = self._load_configs()

    # ── 注册 ─────────────────────────────────────────────────────────────────

    def register(self, cls: type[EventListenerBase]):
        self._registry[cls.name] = cls
        if cls.name not in self._configs:
            # 用 schema 默认值初始化配置
            self._configs[cls.name] = {
                s["key"]: s.get("default", None)
                for s in cls.config_schema
            }
        logger.info("[ListenerManager] Registered listener: %s", cls.name)

    # ── 生命周期 ─────────────────────────────────────────────────────────────

    async def start_listener(self, name: str):
        cls = self._registry.get(name)
        if not cls:
            raise KeyError(f"Listener '{name}' not registered")
        cfg = self._configs.get(name, {})
        instance = cls(cfg, self._dispatch_fn)
        self._instances[name] = instance
        await instance.start()

    async def stop_listener(self, name: str):
        inst = self._instances.get(name)
        if inst:
            await inst.stop()

    async def stop_all(self):
        for name in list(self._instances):
            await self.stop_listener(name)

    # ── 状态查询 ─────────────────────────────────────────────────────────────

    def list_listeners(self) -> list[dict]:
        result = []
        for name, cls in self._registry.items():
            inst = self._instances.get(name)
            status = inst.status if inst else "stopped"
            last_event = None
            if inst and inst._events:
                last_event = inst._events[-1].to_dict()
            result.append({
                "name": name,
                "display_name": cls.display_name,
                "description": cls.description,
                "status": status,
                "config_schema": cls.config_schema,
                "config": self._safe_config(name),
                "last_event": last_event,
                "event_count": len(inst._events) if inst else 0,
            })
        return result

    def get_events(self, name: str, limit: int = 50) -> list[dict]:
        inst = self._instances.get(name)
        if not inst:
            return []
        return [e.to_dict() for e in list(inst._events)[-limit:]]

    # ── 配置 CRUD ────────────────────────────────────────────────────────────

    def get_config(self, name: str) -> dict:
        return self._safe_config(name)

    def update_config(self, name: str, new_cfg: dict):
        self._configs[name] = new_cfg
        self._save_configs()
        # 若已在运行，更新实例的 config（下次循环迭代时生效）
        inst = self._instances.get(name)
        if inst:
            inst.config = new_cfg

    def _safe_config(self, name: str) -> dict:
        """返回配置副本，password 字段脱敏（仅前端显示用）。"""
        cfg = dict(self._configs.get(name, {}))
        cls = self._registry.get(name)
        if cls:
            for s in cls.config_schema:
                if s.get("type") == "password" and cfg.get(s["key"]):
                    cfg[s["key"]] = "••••••••"
        return cfg

    def get_raw_config(self, name: str) -> dict:
        return dict(self._configs.get(name, {}))

    # ── 持久化 ───────────────────────────────────────────────────────────────

    def _config_path(self) -> str:
        return os.path.join(self._plugin_dir, self.CONFIG_FILENAME)

    def _load_configs(self) -> dict:
        try:
            with open(self._config_path(), encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}
        except Exception as e:
            logger.warning("[ListenerManager] Failed to load configs: %s", e)
            return {}

    def _save_configs(self):
        try:
            with open(self._config_path(), "w", encoding="utf-8") as f:
                json.dump(self._configs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("[ListenerManager] Failed to save configs: %s", e)


# ── EmailListener ─────────────────────────────────────────────────────────────

def _decode_header_str(header_str: str) -> str:
    parts = email.header.decode_header(header_str or "")
    decoded = []
    for byt, enc in parts:
        if isinstance(byt, bytes):
            decoded.append(byt.decode(enc or "utf-8", errors="replace"))
        else:
            decoded.append(byt)
    return "".join(decoded)


def _extract_text_body(msg: email.message.Message, max_chars: int = 2000) -> str:
    """递归提取 text/plain 正文，截断到 max_chars。"""
    text_parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                try:
                    text_parts.append(part.get_payload(decode=True).decode(charset, errors="replace"))
                except Exception:
                    pass
    else:
        if msg.get_content_type() == "text/plain":
            charset = msg.get_content_charset() or "utf-8"
            try:
                text_parts.append(msg.get_payload(decode=True).decode(charset, errors="replace"))
            except Exception:
                pass

    body = "\n".join(text_parts).strip()
    if len(body) > max_chars:
        body = body[:max_chars] + f"\n…（正文已截断，超过 {max_chars} 字符）"
    return body


def _imap_fetch_new(
    host: str, port: int, use_ssl: bool,
    user: str, password: str, folder: str,
    since_uid: int,
) -> tuple[int, list[dict]]:
    """同步 IMAP 拉取（在 asyncio.to_thread 中运行）。返回 (new_max_uid, [email_dict])"""
    try:
        if use_ssl:
            M = imaplib.IMAP4_SSL(host, port)
        else:
            M = imaplib.IMAP4(host, port)
        M.login(user, password)
        M.select(folder, readonly=True)

        # 搜索 UID > since_uid（首次 since_uid=0 则取最新 5 封）
        if since_uid > 0:
            typ, data = M.uid("SEARCH", None, f"UID {since_uid + 1}:*")
        else:
            # 第一次连接：只取最新 5 封，避免洪水推送
            typ, data = M.uid("SEARCH", None, "ALL")

        uids: list[int] = []
        if typ == "OK" and data and data[0]:
            raw_uids = data[0].decode().split()
            uids = [int(u) for u in raw_uids if u.isdigit()]
            if since_uid == 0:
                uids = uids[-5:]  # 首次只取最新 5

        if not uids:
            M.logout()
            return since_uid, []

        new_max_uid = max(uids)
        results = []
        for uid in uids:
            typ, msg_data = M.uid("FETCH", str(uid), "(RFC822)")
            if typ != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            if not isinstance(raw, bytes):
                continue
            parsed = email.message_from_bytes(raw)
            results.append({
                "uid": uid,
                "from": _decode_header_str(parsed.get("From", "")),
                "subject": _decode_header_str(parsed.get("Subject", "(无主题)")),
                "date": parsed.get("Date", ""),
                "body": _extract_text_body(parsed),
            })

        M.logout()
        return new_max_uid, results

    except Exception as e:
        raise RuntimeError(f"IMAP error: {e}") from e


class EmailListener(EventListenerBase):
    name = "email_imap"
    display_name = "📧 邮件监听 (IMAP)"
    description = "通过 IMAP 协议轮询指定邮箱，新邮件自动推送给 LLM 处理并通知用户。"
    config_schema = [
        {"key": "imap_host",   "label": "IMAP 服务器",   "type": "string",   "required": True,  "default": "",    "description": "如 imap.gmail.com / imap.qq.com"},
        {"key": "imap_port",   "label": "IMAP 端口",     "type": "number",   "required": False, "default": 993,   "description": "SSL 通常 993，STARTTLS 通常 143"},
        {"key": "imap_ssl",    "label": "使用 SSL",      "type": "bool",     "required": False, "default": True,  "description": ""},
        {"key": "email",       "label": "邮箱地址",      "type": "string",   "required": True,  "default": "",    "description": "登录用的邮箱账号"},
        {"key": "password",    "label": "密码 / 授权码", "type": "password", "required": True,  "default": "",    "description": "Gmail 需使用应用专用密码；QQ 邮箱需开启 IMAP 并使用授权码"},
        {"key": "folder",      "label": "邮件文件夹",    "type": "string",   "required": False, "default": "INBOX","description": ""},
        {"key": "interval",    "label": "检查间隔（秒）","type": "number",   "required": False, "default": 60,    "description": "建议不低于 30 秒，避免被服务商限流"},
        {"key": "target_session","label":"目标会话 UMO", "type": "string",   "required": True,  "default": "",    "description": "如 default:FriendMessage:123456"},
        {"key": "allow_tools", "label": "允许 LLM 使用工具","type": "bool",  "required": False, "default": False, "description": "开启后 LLM 可调用 shell_run 等工具（需 sender_id 为管理员）"},
        {"key": "sender_whitelist","label":"发件人白名单","type": "list",    "required": False, "default": [],    "description": "只处理这些发件人的邮件，列表为空则不过滤（每行一个地址/关键词）"},
        {"key": "sender_blacklist","label":"发件人黑名单","type": "list",    "required": False, "default": [],    "description": "忽略这些发件人的邮件"},
        {"key": "subject_keywords","label":"主题关键词",  "type": "list",    "required": False, "default": [],    "description": "主题包含任意关键词才处理，列表为空则不过滤"},
        {"key": "max_body_chars","label":"最大正文长度",  "type": "number",  "required": False, "default": 2000,  "description": "超出部分截断，避免 prompt 过长"},
        {"key": "llm_prompt_template","label":"LLM Prompt 模板","type":"text","required":False,"default":
            "你收到了一封新邮件，请提取关键信息并用简洁友好的方式通知用户。\n\n{email_content}",
            "description": "可用占位符：{email_content}"},
    ]

    def __init__(self, config: dict, dispatch_fn):
        super().__init__(config, dispatch_fn)
        self._last_uid: int = 0  # 已处理的最大 UID，避免重复推送

    def _filter_email(self, mail: dict) -> bool:
        """返回 True 表示应该处理该邮件。"""
        sender = mail.get("from", "").lower()
        subject = mail.get("subject", "").lower()

        whitelist = [s.lower() for s in (self.config.get("sender_whitelist") or []) if s]
        if whitelist and not any(w in sender for w in whitelist):
            return False

        blacklist = [s.lower() for s in (self.config.get("sender_blacklist") or []) if s]
        if blacklist and any(b in sender for b in blacklist):
            return False

        keywords = [k.lower() for k in (self.config.get("subject_keywords") or []) if k]
        if keywords and not any(k in subject for k in keywords):
            return False

        return True

    async def _run_loop(self):
        host = self.config.get("imap_host", "")
        port = int(self.config.get("imap_port") or 993)
        use_ssl = bool(self.config.get("imap_ssl", True))
        user = self.config.get("email", "")
        password = self.config.get("password", "")
        folder = self.config.get("folder") or "INBOX"
        interval = max(10, int(self.config.get("interval") or 60))
        session = self.config.get("target_session", "")
        allow_tools = bool(self.config.get("allow_tools", False))
        max_chars = int(self.config.get("max_body_chars") or 2000)
        template = self.config.get("llm_prompt_template") or \
            "你收到了一封新邮件，请提取关键信息并用简洁友好的方式通知用户。\n\n{email_content}"

        if not host or not user or not password or not session:
            logger.error("[Listener:email_imap] Missing required config (host/email/password/session).")
            self._record_event("config", "缺少必要配置，listener 停止", dispatched=False)
            return

        logger.info("[Listener:email_imap] Starting IMAP loop → %s:%d (interval=%ds)", host, port, interval)
        self._record_event("system", f"Listener 启动，连接 {host}:{port}", dispatched=False)

        while self._running:
            try:
                # 同步 IMAP 调用放到线程池，不阻塞事件循环
                new_uid, mails = await asyncio.to_thread(
                    _imap_fetch_new, host, port, use_ssl, user, password, folder, self._last_uid
                )
                if new_uid > self._last_uid:
                    self._last_uid = new_uid

                for mail in mails:
                    if not self._filter_email(mail):
                        logger.debug("[Listener:email_imap] Filtered: %s / %s", mail["from"], mail["subject"])
                        self._record_event(mail["from"], f"[过滤] {mail['subject']}", dispatched=False)
                        continue

                    body_preview = mail["body"]
                    if len(body_preview) > max_chars:
                        body_preview = body_preview[:max_chars] + "…（已截断）"

                    email_content = (
                        f"From: {mail['from']}\n"
                        f"Subject: {mail['subject']}\n"
                        f"Date: {mail['date']}\n"
                        f"---\n{body_preview}"
                    )
                    note = template.format(email_content=email_content)

                    await self._do_dispatch(note=note, session=session, allow_tools=allow_tools)
                    summary = f"📨 {mail['subject']} | from {mail['from']}"
                    self._record_event(mail["from"], summary, dispatched=True)
                    logger.info("[Listener:email_imap] Dispatched: %s", summary)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("[Listener:email_imap] Error in loop: %s", e)
                self._record_event("error", str(e), dispatched=False)

            await asyncio.sleep(interval)
