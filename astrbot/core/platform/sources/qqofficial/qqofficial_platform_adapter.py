from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import cast

import botpy
import botpy.message
from botpy import Client
from botpy.http import Route
from botpy.types.message import Media

from astrbot import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, File, Image, Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
)
from astrbot.core.message.components import BaseMessageComponent
from astrbot.core.platform.astr_message_event import MessageSesion

from ...register import register_platform_adapter
from .qqofficial_message_event import QQOfficialMessageEvent

# remove logger handler
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


# QQ 机器人官方框架
class botClient(Client):
    def set_platform(self, platform: QQOfficialPlatformAdapter) -> None:
        self.platform = platform

    # 收到群消息
    async def on_group_at_message_create(
        self, message: botpy.message.GroupMessage
    ) -> None:
        abm = QQOfficialPlatformAdapter._parse_from_qqofficial(
            message,
            MessageType.GROUP_MESSAGE,
        )
        abm.group_id = cast(str, message.group_openid)
        abm.session_id = abm.group_id
        self._commit(abm)

    # 收到频道消息
    async def on_at_message_create(self, message: botpy.message.Message) -> None:
        abm = QQOfficialPlatformAdapter._parse_from_qqofficial(
            message,
            MessageType.GROUP_MESSAGE,
        )
        abm.group_id = message.channel_id
        abm.session_id = abm.group_id
        self._commit(abm)

    # 收到私聊消息
    async def on_direct_message_create(
        self, message: botpy.message.DirectMessage
    ) -> None:
        abm = QQOfficialPlatformAdapter._parse_from_qqofficial(
            message,
            MessageType.FRIEND_MESSAGE,
        )
        abm.session_id = abm.sender.user_id
        self._commit(abm)

    # 收到 C2C 消息
    async def on_c2c_message_create(self, message: botpy.message.C2CMessage) -> None:
        abm = QQOfficialPlatformAdapter._parse_from_qqofficial(
            message,
            MessageType.FRIEND_MESSAGE,
        )
        abm.session_id = abm.sender.user_id
        self._commit(abm)

    def _commit(self, abm: AstrBotMessage) -> None:
        self.platform.commit_event(
            QQOfficialMessageEvent(
                abm.message_str,
                abm,
                self.platform.meta(),
                abm.session_id,
                self.platform.client,
            ),
        )


try:
    from botpy.types.markdown import Markdown as MarkdownPayload
except ImportError:
    class MarkdownPayload:
        def __init__(self, content: str):
            self.content = content


@register_platform_adapter("qq_official", "QQ 机器人官方 API 适配器")
class QQOfficialPlatformAdapter(Platform):
    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)

        self.appid = platform_config["appid"]
        self.secret = platform_config["secret"]
        qq_group = platform_config["enable_group_c2c"]
        guild_dm = platform_config["enable_guild_direct_message"]

        if qq_group:
            self.intents = botpy.Intents(
                public_messages=True,
                public_guild_messages=True,
                direct_message=guild_dm,
            )
        else:
            self.intents = botpy.Intents(
                public_guild_messages=True,
                direct_message=guild_dm,
            )
        self.client = botClient(
            intents=self.intents,
            bot_log=False,
            timeout=20,
        )

        self.client.set_platform(self)

        self.test_mode = os.environ.get("TEST_MODE", "off") == "on"

    async def send_by_session(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        """主动向 C2C 单聊用户发送消息。

        session.session_id 即为目标用户的 user_openid。
        支持：纯文本、图片、视频（file_type=2）、文件（file_type=4）。
        仅支持 FRIEND_MESSAGE（C2C）类型，群聊暂不支持主动发送。
        """
        from astrbot.core.utils.io import file_to_base64, download_image_by_url
        from astrbot.api.message_components import (
            Image as ImageComp,
            Plain as PlainComp,
            Video as VideoComp,
        )

        if session.message_type.value != "FriendMessage":
            logger.warning(
                f"[QQOfficial] send_by_session 目前仅支持 C2C 单聊（FriendMessage），"
                f"收到 {session.message_type.value}，跳过。"
            )
            return

        openid = session.session_id  # C2C 场景下 session_id == user_openid
        plain_text = ""
        image_base64 = None

        file_path = None        # 本地文件路径（用于 File / Video 组件）
        file_name = "file"
        file_type_override = 4  # 默认 file_type=4（普通文件），视频时改为 2

        for comp in message_chain.chain:
            if isinstance(comp, PlainComp):
                plain_text += comp.text
            elif isinstance(comp, ImageComp) and image_base64 is None:
                try:
                    if comp.file and comp.file.startswith("file:///"):
                        image_base64 = file_to_base64(comp.file[8:])
                    elif comp.file and comp.file.startswith("http"):
                        path = await download_image_by_url(comp.file)
                        image_base64 = file_to_base64(path)
                    elif comp.file and comp.file.startswith("base64://"):
                        image_base64 = comp.file[9:]
                    elif comp.file:
                        image_base64 = file_to_base64(comp.file)
                    if image_base64:
                        image_base64 = image_base64.removeprefix("base64://")
                except Exception as e:
                    logger.error(f"[QQOfficial] send_by_session 图片处理失败: {e}")
            elif isinstance(comp, VideoComp) and file_path is None:
                # Video 组件：读取本地视频文件路径，以 file_type=2 上传
                video_src = getattr(comp, "file", None) or getattr(comp, "file_", None) or ""
                if video_src and video_src.startswith("file:///"):
                    video_src = video_src[8:]
                if video_src and os.path.exists(video_src):
                    file_path = os.path.abspath(video_src)
                    file_name = os.path.basename(video_src)
                    file_type_override = 2  # file_type=2 → 视频
                elif video_src and video_src.startswith("http"):
                    try:
                        file_path = await download_image_by_url(video_src)
                        file_name = os.path.basename(file_path)
                        file_type_override = 2
                    except Exception as e:
                        logger.error(f"[QQOfficial] send_by_session 视频下载失败: {e}")
                else:
                    logger.warning(f"[QQOfficial] send_by_session: Video 组件无有效路径: {video_src}")
            elif hasattr(comp, "file_") and file_path is None:
                # File 组件：直接用本地路径
                local = getattr(comp, "file_", None) or ""
                url_val = getattr(comp, "url", None) or ""
                file_name = getattr(comp, "name", None) or "file"
                if local and os.path.exists(local):
                    file_path = os.path.abspath(local)
                elif url_val:
                    try:
                        file_path = await download_image_by_url(url_val)
                    except Exception as e:
                        logger.error(f"[QQOfficial] send_by_session 文件下载失败: {e}")
                else:
                    logger.warning("[QQOfficial] send_by_session: File 组件无有效路径。")

        if not plain_text and not image_base64 and not file_path:
            logger.warning("[QQOfficial] send_by_session: 消息内容为空，跳过发送。")
            return

        try:
            payload: dict = {
                "openid": openid,
                "msg_type": 0,
                "msg_seq": random.randint(1, 10000),
            }

            if image_base64:
                # 先上传图片获取 media
                upload_payload = {
                    "file_data": image_base64,
                    "file_type": 1,
                    "srv_send_msg": False,
                    "openid": openid,
                }
                route = Route("POST", "/v2/users/{openid}/files", openid=openid)
                result = await self.client.api._http.request(route, json=upload_payload)
                if isinstance(result, dict):
                    media = Media(
                        file_uuid=result["file_uuid"],
                        file_info=result["file_info"],
                        ttl=result.get("ttl", 0),
                    )
                    payload["media"] = media
                    payload["msg_type"] = 7  # 富媒体类型
                    if plain_text:
                        payload["content"] = plain_text
                else:
                    logger.error(f"[QQOfficial] send_by_session 图片上传失败: {result}")
                    if plain_text:
                        payload["content"] = plain_text
                        payload["msg_type"] = 0

            elif file_path:
                # 文件/视频类型：读取本地文件，以 base64 上传
                import aiofiles
                import base64 as _base64
                async with aiofiles.open(file_path, "rb") as f:
                    file_data = await f.read()
                file_b64 = _base64.b64encode(file_data).decode("utf-8")
                upload_payload = {
                    "file_data": file_b64,
                    "file_type": file_type_override,  # 2=视频, 4=普通文件
                    "srv_send_msg": False,
                    "openid": openid,
                }
                route = Route("POST", "/v2/users/{openid}/files", openid=openid)
                result = await self.client.api._http.request(route, json=upload_payload)
                if isinstance(result, dict):
                    media = Media(
                        file_uuid=result["file_uuid"],
                        file_info=result["file_info"],
                        ttl=result.get("ttl", 0),
                    )
                    payload["media"] = media
                    payload["msg_type"] = 7
                    if plain_text:
                        payload["content"] = plain_text
                else:
                    logger.error(f"[QQOfficial] send_by_session 文件上传失败: {result}")
                    return

            else:
                payload["markdown"] = {"content": plain_text}
                payload["msg_type"] = 2

            route = Route("POST", "/v2/users/{openid}/messages", openid=openid)
            ret = await self.client.api._http.request(route, json=payload)
            logger.info(f"[QQOfficial] send_by_session C2C 发送成功: openid={openid}, ret={ret}")

        except Exception as e:
            logger.error(f"[QQOfficial] send_by_session 发送失败: {e}", exc_info=True)

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            name="qq_official",
            description="QQ 机器人官方 API 适配器",
            id=cast(str, self.config.get("id")),
            support_proactive_message=True,
        )

    @staticmethod
    def _normalize_attachment_url(url: str | None) -> str:
        if not url:
            return ""
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"https://{url}"

    @staticmethod
    def _append_attachments(
        msg: list[BaseMessageComponent],
        attachments: list | None,
    ) -> None:
        if not attachments:
            return

        for attachment in attachments:
            content_type = cast(str, getattr(attachment, "content_type", "") or "")
            url = QQOfficialPlatformAdapter._normalize_attachment_url(
                cast(str | None, getattr(attachment, "url", None))
            )
            if not url:
                continue

            if content_type.startswith("image"):
                msg.append(Image.fromURL(url))
            else:
                filename = cast(
                    str,
                    getattr(attachment, "filename", None)
                    or getattr(attachment, "name", None)
                    or "attachment",
                )
                msg.append(File(name=filename, file=url, url=url))

    @staticmethod
    def _parse_from_qqofficial(
        message: botpy.message.Message
        | botpy.message.GroupMessage
        | botpy.message.DirectMessage
        | botpy.message.C2CMessage,
        message_type: MessageType,
    ):
        abm = AstrBotMessage()
        abm.type = message_type
        abm.timestamp = int(time.time())
        abm.raw_message = message
        abm.message_id = message.id
        # abm.tag = "qq_official"
        msg: list[BaseMessageComponent] = []

        if isinstance(message, botpy.message.GroupMessage) or isinstance(
            message,
            botpy.message.C2CMessage,
        ):
            if isinstance(message, botpy.message.GroupMessage):
                abm.sender = MessageMember(message.author.member_openid, "")
                abm.group_id = message.group_openid
            else:
                abm.sender = MessageMember(message.author.user_openid, "")
            abm.message_str = message.content.strip()
            abm.self_id = "unknown_selfid"
            msg.append(At(qq="qq_official"))
            msg.append(Plain(abm.message_str))
            QQOfficialPlatformAdapter._append_attachments(msg, message.attachments)
            abm.message = msg

        elif isinstance(message, botpy.message.Message) or isinstance(
            message,
            botpy.message.DirectMessage,
        ):
            if isinstance(message, botpy.message.Message):
                abm.self_id = str(message.mentions[0].id)
            else:
                abm.self_id = ""

            plain_content = message.content.replace(
                "<@!" + str(abm.self_id) + ">",
                "",
            ).strip()

            QQOfficialPlatformAdapter._append_attachments(msg, message.attachments)
            abm.message = msg
            abm.message_str = plain_content
            abm.sender = MessageMember(
                str(message.author.id),
                str(message.author.username),
            )
            msg.append(At(qq="qq_official"))
            msg.append(Plain(plain_content))

            if isinstance(message, botpy.message.Message):
                abm.group_id = message.channel_id
        else:
            raise ValueError(f"Unknown message type: {message_type}")
        abm.self_id = "qq_official"
        return abm

    def run(self):
        return self.client.start(appid=self.appid, secret=self.secret)

    def get_client(self) -> botClient:
        return self.client

    async def terminate(self) -> None:
        await self.client.close()
        logger.info("QQ 官方机器人接口 适配器已被优雅地关闭")
