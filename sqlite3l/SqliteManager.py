import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import asyncio

from astrbot import logger

# ---------------------------------------------------------------- DDL BEGIN
# fmt: off
_DDL = """
       CREATE TABLE IF NOT EXISTS raw_message
       (
           id
           INTEGER
           NOT
           NULL
           CONSTRAINT
           raw_message_pk
           PRIMARY
           KEY
           AUTOINCREMENT,
           create_time
           TEXT
           NOT
           NULL,
           document_id
           TEXT
           NOT
           NULL,
           sender_id
           TEXT
           NOT
           NULL,
           bot_id
           TEXT
           NOT
           NULL,
           persona_id
           TEXT
           NOT
           NULL,
           user_message
           TEXT
           NOT
           NULL,
           llm_message
           TEXT
           NOT
           NULL,
           hash
           TEXT
           NOT
           NULL,
           flushed
           INTEGER
           NOT
           NULL
           DEFAULT
           0,
           CONSTRAINT
           chk_flushed
           CHECK (
           flushed
           IN
       (
           0,
           1
       ))
           );

       CREATE INDEX IF NOT EXISTS raw_message_document_id_flushed_index
           ON raw_message (document_id, flushed);

       CREATE UNIQUE INDEX IF NOT EXISTS raw_message_hash_uindex
           ON raw_message (hash); \
       """

_INSERT_SQL = """
              INSERT
              OR IGNORE INTO raw_message
    (create_time, document_id, sender_id, bot_id, persona_id,
     user_message, llm_message, hash, flushed)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0) \
              """

_HASH_SEP = "\x1f"  # 不可见分隔符，避免字段拼接歧义


# fmt: on
# ---------------------------------------------------------------- DDL END
def now_iso() -> str:
    """ISO 8601 带时区，供 Hindsight 解析时间指代。"""
    # 2026-11-03T14:30:00+08:00
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def build_document_id(sender_id: str, bot_id: str, persona_id: str) -> str:
    """同一用户 × 同一 bot × 同一人格 → 稳定 document_id。"""
    local_date = datetime.now(timezone.utc).astimezone().date().isoformat()
    return f"{local_date}:{sender_id}:{bot_id}:{persona_id}"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def prepare_raw_message(
        *,
        sender_id: str,
        bot_id: str,
        persona_id: str,
        user_message: str,
        llm_message: str
) -> dict[str, Any]:
    """
    构建一条待插入记录。

    hash_salt: 可选去重盐。若平台事件带 message_id，传进来可避免
               “用户重复说同一句话”被误判为重复而丢弃。
    """
    document_id = build_document_id(sender_id, bot_id, persona_id)
    ts = now_iso()

    payload = f"{document_id}_{user_message}_{llm_message}_{_HASH_SEP}"
    raw_hash = _sha256(payload)

    return {
        "create_time": ts,
        "document_id": document_id,
        "sender_id": sender_id,
        "bot_id": bot_id,
        "persona_id": persona_id,
        "user_message": user_message,
        "llm_message": llm_message,
        "hash": raw_hash,
    }


# ---------------------------------------------------------------- 3. 插入


class Sqlite3Manager:
    def __init__(self, data_path):
        self.database_name = "raw_message.db"
        self.database_path: Path = Path(data_path).joinpath(self.database_name)
        self.connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    # ---------------------------------------------------------------- 1. 初始化
    async def _init_db(self) -> aiosqlite.Connection:
        """确保 db 文件与表/索引存在，返回长期持有的连接。"""
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

        conn = await aiosqlite.connect(self.database_path)
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.executescript(_DDL)
        await conn.commit()
        logger.info("数据库初始化完毕！")
        return conn

    async def get_connection(self) -> aiosqlite.Connection:
        conn = self.connection
        if conn is not None:
            return conn
        async with self._lock:
            if self.connection is None:
                self.connection = await self._init_db()  # 显式赋值
                logger.info("获取数据库连接成功！")
            assert self.connection is not None
            return self.connection

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()
            self.connection = None

    async def insert_raw_message(
            self,
            conn: aiosqlite.Connection,
            record: dict[str, Any],
    ) -> bool:
        """单次插入完整一轮；唯一冲突时静默忽略。返回是否真的写入。"""
        cur = await self.connection.execute(_INSERT_SQL, (
            record["create_time"],
            record["document_id"],
            record["sender_id"],
            record["bot_id"],
            record["persona_id"],
            record["user_message"],
            record["llm_message"],
            record["hash"],
        ))
        await conn.commit()
        inserted = cur.rowcount == 1
        await cur.close()
        return inserted
