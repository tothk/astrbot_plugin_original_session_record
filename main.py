import sys

import aiosqlite
from astrbot.api.provider import ProviderRequest
from dashscope.applications.agentstudio import user_message

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.core.db import Persona
from astrbot.core.platform import MessageType
from astrbot.core.provider.entities import LLMResponse
import astrbot.api.message_components as Comp

from astrbot.core.utils.astrbot_path import get_astrbot_data_path
from pathlib import Path

from .sqlite3l.SqliteManager import Sqlite3Manager, prepare_raw_message


@register("helloworld", "YourName", "一个简单的 Hello World 插件", "1.0.0")
class OriginalSessionRecord(Star):
    def __init__(self, context: Context):
        super().__init__(context)

        self.PLUGIN_DATA_PATH = Path(get_astrbot_data_path()) / "plugin_data" / self.name

        self._sqlite3_manager = Sqlite3Manager(self.PLUGIN_DATA_PATH)
        self.conn: aiosqlite.Connection | None = None

    async def initialize(self):
        """可选择实现异步的插件初始化方法，当实例化该插件类之后会自动调用该方法。"""
        self.conn = await self._sqlite3_manager.get_connection()

    async def terminate(self):
        """可选择实现异步的插件销毁方法，当插件被卸载/停用时会调用。"""

        # 关闭数据库链接
        await self._sqlite3_manager.close()

    @filter.command("get")
    async def get_instance_id(self, event: AstrMessageEvent):
        # 直接获取平台实例ID (Platform ID)
        instance_id = event.get_platform_id()
        yield event.plain_result(f"当前平台实例ID: {instance_id}")

    @filter.on_llm_response()
    async def on_llm_resp(
            self, event: AstrMessageEvent, resp: LLMResponse
    ):  # 请注意有三个参数
        message_type = event.get_message_type()
        if message_type != MessageType.FRIEND_MESSAGE:
            return

        user_message = event.get_message_str()
        llm_message = resp.completion_text
        await event.send(event.plain_result("🤔 正在等待请求..."))
        bot_id = event.message_obj.self_id
        if not bot_id or bot_id == "unknown_selfid":
            bot_id = event.get_platform_id()

        persona_id, persona_obj, force_applied_persona_id, use_webchat_special_default = (
            await self.context.persona_manager.resolve_selected_persona(
                umo=event.unified_msg_origin,
                conversation_persona_id=getattr(ProviderRequest.conversation, "persona_id", None),
                platform_name=event.get_platform_name(),
                provider_settings=self.context.get_config(event.unified_msg_origin).get(
                    "provider_settings", {}
                ),
            )
        )
        record = prepare_raw_message(
            sender_id=event.get_sender_id(),
            bot_id=bot_id,
            persona_id=persona_id or "default",  # NOT NULL，务必兜底
            user_message=user_message,  # 多条用户消息在此合并
            llm_message=llm_message,  # 原始生成，未经装饰
        )
        await self._sqlite3_manager.insert_raw_message(self.conn, record)
        logger.info("成功记录一条原始会话记录！")

