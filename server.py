# ============================================================
#  BLAZE MESSENGER v2.0 — SERVER
#  Полная версия: чаты, группы, голосовые, кружки, звонки,
#  профили, поиск, 2 языка, push-уведомления
# ============================================================

import asyncio
import websockets
import json
import datetime
import os
import http
import psycopg
from psycopg.rows import dict_row
import bcrypt
import jwt
import base64
import uuid
import secrets

# ============ КОНФИГ ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGO = "HS256"

MAX_HISTORY = 200
MAX_FILE_SIZE = 10 * 1024 * 1024        # 10 МБ для файлов
MAX_VOICE_SIZE = 2 * 1024 * 1024        # 2 МБ для голосовых
MAX_AVATAR_SIZE = 1 * 1024 * 1024       # 1 МБ для аватарок

# ============ БАЗА ДАННЫХ ============
def db_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


async def init_db():
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL не задан — работаю без истории")
        return

    def _init():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Пользователи
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        display_name TEXT,
                        bio TEXT DEFAULT '',
                        avatar_color TEXT DEFAULT '#ff6b00',
                        avatar_data TEXT,
                        language TEXT DEFAULT 'ru',
                        theme TEXT DEFAULT 'dark',
                        last_seen TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # Чаты (private + group)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS chats (
                        id SERIAL PRIMARY KEY,
                        type TEXT DEFAULT 'private',
                        title TEXT,
                        description TEXT DEFAULT '',
                        avatar_data TEXT,
                        avatar_color TEXT DEFAULT '#ff6b00',
                        created_by INTEGER REFERENCES users(id),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # Участники чатов
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS chat_members (
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        role TEXT DEFAULT 'member',
                        last_read_id INTEGER DEFAULT 0,
                        joined_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (chat_id, user_id)
                    );
                """)

                # Сообщения
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        sender_id INTEGER REFERENCES users(id),
                        sender_name TEXT NOT NULL,
                        msg_type TEXT DEFAULT 'text',
                        text TEXT,
                        file_data TEXT,
                        file_name TEXT,
                        file_type TEXT,
                        duration INTEGER,
                        reply_to INTEGER,
                        forwarded_from TEXT,
                        edited BOOLEAN DEFAULT FALSE,
                        deleted BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # Реакции
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reactions (
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        emoji TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (message_id, user_id)
                    );
                """)

                # Push-подписки
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS push_subscriptions (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        endpoint TEXT UNIQUE NOT NULL,
                        p256dh TEXT,
                        auth TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # Общий чат по умолчанию (для теста)
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                if not cur.fetchone():
                    cur.execute("INSERT INTO chats (type, title, description) VALUES ('global', 'Общий чат', 'Все пользователи Blaze')")

            conn.commit()

    try:
        await asyncio.to_thread(_init)
        print("✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")


# ============ USERS ============
async def db_create_user(username, password):
    if not DATABASE_URL:
        return None
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def _q():
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    colors = ["#ff6b00", "#e53935", "#ff8c1a", "#d32f2f",
                              "#ff5722", "#f4511e", "#bf360c", "#e64a19"]
                    color = colors[sum(ord(c) for c in username) % len(colors)]
                    cur.execute(
                        "INSERT INTO users (username, display_name, password_hash, avatar_color) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id, username, display_name, avatar_color, bio, avatar_data, language, theme",
                        (username, username, pw_hash, color)
                    )
                    row = cur.fetchone()
                conn.commit()
                return row
        except psycopg.errors.UniqueViolation:
            return None

    return await asyncio.to_thread(_q)


async def db_get_user(username):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, display_name, password_hash, avatar_color, bio, "
                    "avatar_data, language, theme FROM users WHERE username=%s",
                    (username,)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_user_by_id(user_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, display_name, avatar_color, bio, avatar_data, last_seen "
                    "FROM users WHERE id=%s",
                    (user_id,)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_all_users(exclude_id=None):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                if exclude_id:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, avatar_data, last_seen "
                        "FROM users WHERE id != %s ORDER BY display_name",
                        (exclude_id,)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, avatar_data, last_seen "
                        "FROM users ORDER BY display_name"
                    )
                return cur.fetchall()

    return await asyncio.to_thread(_q)


async def db_update_user(user_id, display_name=None, bio=None,
                         avatar_data=None, language=None, theme=None):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                updates = []
                params = []
                if display_name is not None:
                    updates.append("display_name=%s")
                    params.append(display_name)
                if bio is not None:
                    updates.append("bio=%s")
                    params.append(bio)
                if avatar_data is not None:
                    updates.append("avatar_data=%s")
                    params.append(avatar_data)
                if language is not None:
                    updates.append("language=%s")
                    params.append(language)
                if theme is not None:
                    updates.append("theme=%s")
                    params.append(theme)
                if not updates:
                    return
                params.append(user_id)
                cur.execute(
                    f"UPDATE users SET {', '.join(updates)} WHERE id=%s",
                    params
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_last_seen(user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET last_seen=NOW() WHERE id=%s", (user_id,))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_search_users(query, exclude_id=None):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                q = f"%{query}%"
                if exclude_id:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, avatar_data, last_seen "
                        "FROM users WHERE (username ILIKE %s OR display_name ILIKE %s) AND id != %s "
                        "ORDER BY display_name LIMIT 50",
                        (q, q, exclude_id)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, avatar_data, last_seen "
                        "FROM users WHERE username ILIKE %s OR display_name ILIKE %s "
                        "ORDER BY display_name LIMIT 50",
                        (q, q)
                    )
                return cur.fetchall()

    return await asyncio.to_thread(_q)
    # ============ CHATS ============
async def db_get_global_chat():
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_or_create_private_chat(user1_id, user2_id):
    if not DATABASE_URL:
        return None
    a, b = sorted([user1_id, user2_id])

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id FROM chats c
                    JOIN chat_members m1 ON m1.chat_id=c.id AND m1.user_id=%s
                    JOIN chat_members m2 ON m2.chat_id=c.id AND m2.user_id=%s
                    WHERE c.type='private' LIMIT 1
                """, (a, b))
                row = cur.fetchone()
                if row:
                    return row
                cur.execute("INSERT INTO chats (type) VALUES ('private') RETURNING id")
                chat_id = cur.fetchone()["id"]
                cur.execute("INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)", (chat_id, a))
                cur.execute("INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)", (chat_id, b))
            conn.commit()
            return {"id": chat_id}

    return await asyncio.to_thread(_q)


async def db_create_group(creator_id, title, member_ids=None):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO chats (type, title, created_by) VALUES ('group', %s, %s) RETURNING id",
                    (title, creator_id)
                )
                chat_id = cur.fetchone()["id"]
                # Создатель — админ
                cur.execute(
                    "INSERT INTO chat_members (chat_id, user_id, role) VALUES (%s, %s, 'admin')",
                    (chat_id, creator_id)
                )
                # Остальные — участники
                if member_ids:
                    for uid in member_ids:
                        if uid != creator_id:
                            cur.execute(
                                "INSERT INTO chat_members (chat_id, user_id, role) "
                                "VALUES (%s, %s, 'member') ON CONFLICT DO NOTHING",
                                (chat_id, uid)
                            )
            conn.commit()
            return {"id": chat_id}

    return await asyncio.to_thread(_q)


async def db_get_chat_info(chat_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, type, title, description, avatar_data, avatar_color,
                           created_by, created_at
                    FROM chats WHERE id=%s
                """, (chat_id,))
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_chat_members(chat_id):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.id, u.username, u.display_name, u.avatar_color,
                           u.avatar_data, u.last_seen, m.role, m.joined_at
                    FROM chat_members m
                    JOIN users u ON u.id = m.user_id
                    WHERE m.chat_id=%s
                    ORDER BY m.role DESC, u.display_name
                """, (chat_id,))
                rows = cur.fetchall()
                for r in rows:
                    if r["last_seen"]:
                        r["last_seen"] = r["last_seen"].isoformat()
                return rows

    return await asyncio.to_thread(_q)


async def db_get_chat_member_ids(chat_id):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM chat_members WHERE chat_id=%s", (chat_id,))
                return [r["user_id"] for r in cur.fetchall()]

    return await asyncio.to_thread(_q)


async def db_check_member(chat_id, user_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_add_members(chat_id, user_ids):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                for uid in user_ids:
                    cur.execute(
                        "INSERT INTO chat_members (chat_id, user_id, role) "
                        "VALUES (%s, %s, 'member') ON CONFLICT DO NOTHING",
                        (chat_id, uid)
                    )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_remove_member(chat_id, user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_set_member_role(chat_id, user_id, role):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE chat_members SET role=%s WHERE chat_id=%s AND user_id=%s",
                    (role, chat_id, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_group(chat_id, title=None, description=None,
                          avatar_data=None, avatar_color=None):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                updates = []
                params = []
                if title is not None:
                    updates.append("title=%s")
                    params.append(title)
                if description is not None:
                    updates.append("description=%s")
                    params.append(description)
                if avatar_data is not None:
                    updates.append("avatar_data=%s")
                    params.append(avatar_data)
                if avatar_color is not None:
                    updates.append("avatar_color=%s")
                    params.append(avatar_color)
                if not updates:
                    return
                params.append(chat_id)
                cur.execute(
                    f"UPDATE chats SET {', '.join(updates)} WHERE id=%s",
                    params
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_leave_chat(chat_id, user_id):
    """Выйти из группы (для private не работает)"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT type FROM chats WHERE id=%s", (chat_id,))
                row = cur.fetchone()
                if not row or row["type"] != "group":
                    return False
                cur.execute(
                    "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_get_user_chats(user_id):
    """Список чатов пользователя с последним сообщением и непрочитанными"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        c.id, c.type, c.title, c.avatar_data, c.avatar_color,
                        (SELECT COALESCE(u2.display_name, u2.username)
                         FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_name,
                        (SELECT u2.avatar_color FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_color,
                        (SELECT u2.avatar_data FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_avatar,
                        (SELECT user_id FROM chat_members
                         WHERE chat_id = c.id AND user_id != %s LIMIT 1) as other_id,
                        (SELECT text FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_text,
                        (SELECT msg_type FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_type,
                        (SELECT sender_name FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_sender,
                        (SELECT created_at FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_time,
                        (SELECT COUNT(*) FROM messages
                         WHERE chat_id=c.id AND id > COALESCE(
                             (SELECT last_read_id FROM chat_members
                              WHERE chat_id=c.id AND user_id=%s), 0
                         ) AND sender_id != %s AND NOT deleted) as unread,
                        (SELECT COUNT(*) FROM chat_members WHERE chat_id=c.id) as member_count
                    FROM chats c
                    JOIN chat_members m ON m.chat_id=c.id AND m.user_id=%s
                    ORDER BY last_time DESC NULLS LAST
                """, (user_id, user_id, user_id, user_id, user_id, user_id, user_id))
                rows = cur.fetchall()
                result = []
                for r in rows:
                    if r["last_time"]:
                        r["last_time"] = r["last_time"].isoformat()
                    result.append(dict(r))
                return result

    return await asyncio.to_thread(_q)


async def db_mark_chat_read(chat_id, user_id, last_message_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE chat_members SET last_read_id=%s WHERE chat_id=%s AND user_id=%s",
                    (last_message_id, chat_id, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


# ============ КОНЕЦ БЛОКА 2 ============
# ============ MESSAGES ============
async def db_save_message(chat_id, sender_id, sender_name, msg_type='text',
                          text=None, file_data=None, file_name=None,
                          file_type=None, duration=None, reply_to=None,
                          forwarded_from=None):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO messages
                    (chat_id, sender_id, sender_name, msg_type, text,
                     file_data, file_name, file_type, duration, reply_to, forwarded_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (chat_id, sender_id, sender_name, msg_type, text,
                      file_data, file_name, file_type, duration, reply_to, forwarded_from))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


async def db_get_chat_history(chat_id, limit=MAX_HISTORY, before_id=None):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                if before_id:
                    cur.execute("""
                        SELECT m.id, m.sender_id, m.sender_name, m.msg_type,
                               m.text, m.file_data, m.file_name, m.file_type,
                               m.duration, m.reply_to, m.forwarded_from,
                               m.edited, m.created_at,
                               COALESCE(u.display_name, u.username) as sender_display,
                               u.avatar_color
                        FROM messages m
                        LEFT JOIN users u ON u.id=m.sender_id
                        WHERE m.chat_id=%s AND NOT m.deleted AND m.id < %s
                        ORDER BY m.id DESC LIMIT %s
                    """, (chat_id, before_id, limit))
                else:
                    cur.execute("""
                        SELECT m.id, m.sender_id, m.sender_name, m.msg_type,
                               m.text, m.file_data, m.file_name, m.file_type,
                               m.duration, m.reply_to, m.forwarded_from,
                               m.edited, m.created_at,
                               COALESCE(u.display_name, u.username) as sender_display,
                               u.avatar_color
                        FROM messages m
                        LEFT JOIN users u ON u.id=m.sender_id
                        WHERE m.chat_id=%s AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT %s
                    """, (chat_id, limit))
                rows = list(reversed(cur.fetchall()))
                for m in rows:
                    cur.execute(
                        "SELECT emoji, COUNT(*) as cnt FROM reactions "
                        "WHERE message_id=%s GROUP BY emoji",
                        (m["id"],)
                    )
                    m["reactions"] = {r["emoji"]: r["cnt"] for r in cur.fetchall()}
                    if m["created_at"]:
                        m["created_at"] = m["created_at"].isoformat()
                return rows

    return await asyncio.to_thread(_q)


async def db_get_message_by_id(message_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.id, m.chat_id, m.sender_id, m.sender_name, m.msg_type,
                           m.text, m.file_data, m.file_name, m.file_type,
                           m.duration, m.reply_to, m.forwarded_from,
                           m.edited, m.deleted, m.created_at,
                           COALESCE(u.display_name, u.username) as sender_display,
                           u.avatar_color
                    FROM messages m
                    LEFT JOIN users u ON u.id=m.sender_id
                    WHERE m.id=%s
                """, (message_id,))
                row = cur.fetchone()
                if row and row["created_at"]:
                    row["created_at"] = row["created_at"].isoformat()
                return row

    return await asyncio.to_thread(_q)


async def db_edit_message(message_id, user_id, new_text):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE messages SET text=%s, edited=TRUE "
                    "WHERE id=%s AND sender_id=%s AND msg_type='text' RETURNING id",
                    (new_text, message_id, user_id)
                )
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_delete_message(message_id, user_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE messages SET deleted=TRUE, text=NULL, file_data=NULL "
                    "WHERE id=%s AND sender_id=%s RETURNING id",
                    (message_id, user_id)
                )
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_toggle_reaction(message_id, user_id, emoji):
    if not DATABASE_URL:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT emoji FROM reactions WHERE message_id=%s AND user_id=%s",
                    (message_id, user_id)
                )
                existing = cur.fetchone()
                if existing and existing["emoji"] == emoji:
                    cur.execute(
                        "DELETE FROM reactions WHERE message_id=%s AND user_id=%s",
                        (message_id, user_id)
                    )
                else:
                    cur.execute("""
                        INSERT INTO reactions (message_id, user_id, emoji)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (message_id, user_id) DO UPDATE SET emoji=%s
                    """, (message_id, user_id, emoji, emoji))
                cur.execute(
                    "SELECT emoji, COUNT(*) as cnt FROM reactions "
                    "WHERE message_id=%s GROUP BY emoji",
                    (message_id,)
                )
                reactions = {r["emoji"]: r["cnt"] for r in cur.fetchall()}
            conn.commit()
            return reactions

    return await asyncio.to_thread(_q)


async def db_search_messages(user_id, query, chat_id=None):
    """Поиск по сообщениям в чатах, где состоит пользователь"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                q = f"%{query}%"
                if chat_id:
                    cur.execute("""
                        SELECT m.id, m.chat_id, m.sender_id, m.sender_name, m.text,
                               m.created_at, c.title, c.type
                        FROM messages m
                        JOIN chats c ON c.id=m.chat_id
                        JOIN chat_members cm ON cm.chat_id=c.id AND cm.user_id=%s
                        WHERE m.chat_id=%s AND m.text ILIKE %s AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT 100
                    """, (user_id, chat_id, q))
                else:
                    cur.execute("""
                        SELECT m.id, m.chat_id, m.sender_id, m.sender_name, m.text,
                               m.created_at, c.title, c.type
                        FROM messages m
                        JOIN chats c ON c.id=m.chat_id
                        JOIN chat_members cm ON cm.chat_id=c.id AND cm.user_id=%s
                        WHERE m.text ILIKE %s AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT 100
                    """, (user_id, q))
                rows = cur.fetchall()
                for r in rows:
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()
                return rows

    return await asyncio.to_thread(_q)


async def db_forward_message(message_id, target_chat_id, sender_id, sender_name):
    """Переслать сообщение — копия с пометкой forwarded_from"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT msg_type, text, file_data, file_name, file_type, "
                    "duration, sender_name FROM messages WHERE id=%s",
                    (message_id,)
                )
                orig = cur.fetchone()
                if not orig:
                    return None
                cur.execute("""
                    INSERT INTO messages
                    (chat_id, sender_id, sender_name, msg_type, text,
                     file_data, file_name, file_type, duration, forwarded_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (target_chat_id, sender_id, sender_name, orig["msg_type"],
                      orig["text"], orig["file_data"], orig["file_name"],
                      orig["file_type"], orig["duration"], orig["sender_name"]))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


# ============ PUSH SUBSCRIPTIONS ============
async def db_save_push_subscription(user_id, endpoint, p256dh, auth):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO push_subscriptions (user_id, endpoint, p256dh, auth)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (endpoint) DO UPDATE
                    SET user_id=%s, p256dh=%s, auth=%s
                """, (user_id, endpoint, p256dh, auth, user_id, p256dh, auth))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_push_subs(user_ids):
    if not DATABASE_URL or not user_ids:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT endpoint, p256dh, auth FROM push_subscriptions
                    WHERE user_id = ANY(%s)
                """, (user_ids,))
                return cur.fetchall()

    return await asyncio.to_thread(_q)


# ============ КОНЕЦ БЛОКА 3 ============
# ============ WEBSOCKET СЕРВЕР ============
clients = {}           # {websocket: {"id": int, "name": str, "color": str}}
user_sockets = {}      # {user_id: set(websocket)}


async def process_request(path, request_headers):
    """Health-check для Render"""
    if "Upgrade" not in request_headers.get("Connection", ""):
        return http.HTTPStatus.OK, [], b"Blaze Messenger v2.0 is running\n"
    return None


async def send_safe(ws, data):
    try:
        await ws.send(data)
    except Exception:
        pass


async def send_to_user(user_id, message):
    """Отправить сообщение всем сокетам пользователя (мультиустройство)"""
    if user_id in user_sockets:
        data = json.dumps(message, ensure_ascii=False)
        await asyncio.gather(
            *[send_safe(ws, data) for ws in user_sockets[user_id]],
            return_exceptions=True
        )


async def broadcast_chat(chat_id, message, exclude=None):
    """Разослать всем участникам чата"""
    member_ids = await db_get_chat_member_ids(chat_id)
    data = json.dumps(message, ensure_ascii=False)
    tasks = []
    for uid in member_ids:
        if uid in user_sockets:
            for ws in user_sockets[uid]:
                if ws != exclude and ws.open:
                    tasks.append(send_safe(ws, data))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def now_str():
    return datetime.datetime.now().strftime("%H:%M")


def user_public(u):
    """Публичные данные пользователя"""
    # Поддерживаем оба варианта: dict из БД (username) и dict из памяти (name)
    username = u.get("username") or u.get("name") or ""
    return {
        "id": u.get("id"),
        "username": username,
        "display_name": u.get("display_name") or username,
        "color": u.get("avatar_color") or u.get("color") or "#ff6b00",
        "avatar_data": u.get("avatar_data"),
        "bio": u.get("bio", ""),
    }

# ============ АВТОРИЗАЦИЯ ============
async def handle_auth(websocket, first_msg):
    action = first_msg.get("action")
    username = str(first_msg.get("username", "")).strip()[:20]
    password = str(first_msg.get("password", ""))

    if not username or not password:
        await websocket.send(json.dumps({
            "type": "auth_error",
            "text": "Логин и пароль обязательны"
        }, ensure_ascii=False))
        return None

    if not DATABASE_URL:
        # Гостевой режим
        return {
            "id": 1, "name": username, "color": "#ff6b00",
            "display_name": username, "bio": "", "avatar_data": None,
            "language": "ru", "theme": "dark", "token": ""
        }

    if action == "register":
        user = await db_create_user(username, password)
        if not user:
            await websocket.send(json.dumps({
                "type": "auth_error",
                "text": "Такой пользователь уже есть"
            }, ensure_ascii=False))
            return None
    elif action == "login":
        user = await db_get_user(username)
        if not user or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
            await websocket.send(json.dumps({
                "type": "auth_error",
                "text": "Неверный логин или пароль"
            }, ensure_ascii=False))
            return None
    else:
        await websocket.send(json.dumps({
            "type": "auth_error",
            "text": "Укажите action: login или register"
        }, ensure_ascii=False))
        return None

    token = jwt.encode({
        "user_id": user["id"],
        "username": user["username"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=30),
    }, JWT_SECRET, algorithm=JWT_ALGO)

    return {
        "id": user["id"],
        "name": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "color": user.get("avatar_color", "#ff6b00"),
        "bio": user.get("bio", ""),
        "avatar_data": user.get("avatar_data"),
        "language": user.get("language", "ru"),
        "theme": user.get("theme", "dark"),
        "token": token,
    }


# ============ ОСНОВНОЙ ОБРАБОТЧИК ============
async def handler(websocket):
    user = None
    try:
        raw = await websocket.recv()
        first = json.loads(raw)
        user = await handle_auth(websocket, first)
        if not user:
            await websocket.close()
            return

        clients[websocket] = user
        user_sockets.setdefault(user["id"], set()).add(websocket)
        print(f"[+] {user['name']} (id={user['id']}). Всего: {len(clients)}")

        # Приветствие
        await websocket.send(json.dumps({
            "type": "auth_ok",
            "user": user_public(user),
            "language": user["language"],
            "theme": user["theme"],
            "token": user["token"],
        }, ensure_ascii=False))

        # Список чатов
        chats = await db_get_user_chats(user["id"])
        await websocket.send(json.dumps({
            "type": "chats_list",
            "chats": chats,
        }, ensure_ascii=False))

        # Все пользователи (для создания чатов)
        users = await db_get_all_users(exclude_id=user["id"])
        await websocket.send(json.dumps({
            "type": "all_users",
            "users": [user_public(u) for u in users],
        }, ensure_ascii=False))

        # Основной цикл
        async for raw_msg in websocket:
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")
            if not mtype:
                continue

            # ===== СООБЩЕНИЕ (любого типа) =====
            if mtype == "message":
                await handle_new_message(websocket, user, msg)

            # ===== ОТКРЫТЬ ЧАТ =====
            elif mtype == "open_chat":
                chat_id = msg.get("chat_id")
                if not chat_id:
                    continue
                # Проверка членства
                member = await db_check_member(chat_id, user["id"])
                if not member:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "text": "Вы не участник этого чата"
                    }, ensure_ascii=False))
                    continue
                history = await db_get_chat_history(chat_id)
                await websocket.send(json.dumps({
                    "type": "chat_history",
                    "chat_id": chat_id,
                    "messages": history,
                }, ensure_ascii=False))
                if history:
                    last_id = history[-1]["id"]
                    await db_mark_chat_read(chat_id, user["id"], last_id)
                    chats = await db_get_user_chats(user["id"])
                    await send_to_user(user["id"], {"type": "chats_list", "chats": chats})

            # ===== СОЗДАТЬ ЛИЧНЫЙ ЧАТ =====
            elif mtype == "create_private":
                other_id = msg.get("user_id")
                if not other_id or other_id == user["id"]:
                    continue
                chat = await db_get_or_create_private_chat(user["id"], other_id)
                if chat:
                    chats = await db_get_user_chats(user["id"])
                    await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
                    chats2 = await db_get_user_chats(other_id)
                    await send_to_user(other_id, {"type": "chats_list", "chats": chats2})

            # ===== СОЗДАТЬ ГРУППУ =====
            elif mtype == "create_group":
                title = str(msg.get("title", "")).strip()[:60]
                member_ids = msg.get("member_ids", [])
                if not title:
                    continue
                group = await db_create_group(user["id"], title, member_ids)
                if group:
                    chat_id = group["id"]
                    all_members = [user["id"]] + list(member_ids)
                    for uid in all_members:
                        chats = await db_get_user_chats(uid)
                        await send_to_user(uid, {"type": "chats_list", "chats": chats})

            # ===== ПЕЧАТАЕТ =====
            elif mtype == "typing":
                chat_id = msg.get("chat_id")
                if chat_id:
                    await broadcast_chat(chat_id, {
                        "type": "typing",
                        "chat_id": chat_id,
                        "nickname": user["display_name"],
                    }, exclude=websocket)

            # ===== РЕАКЦИЯ =====
            elif mtype == "react":
                message_id = msg.get("message_id")
                emoji = msg.get("emoji")
                chat_id = msg.get("chat_id")
                if message_id and emoji and chat_id:
                    reactions = await db_toggle_reaction(message_id, user["id"], emoji)
                    await broadcast_chat(chat_id, {
                        "type": "reactions_update",
                        "message_id": message_id,
                        "chat_id": chat_id,
                        "reactions": reactions,
                    })

            # ===== РЕДАКТИРОВАТЬ =====
            elif mtype == "edit_message":
                message_id = msg.get("message_id")
                chat_id = msg.get("chat_id")
                new_text = str(msg.get("text", ""))[:4000]
                if message_id and chat_id and new_text.strip():
                    ok = await db_edit_message(message_id, user["id"], new_text)
                    if ok:
                        await broadcast_chat(chat_id, {
                            "type": "message_edited",
                            "message_id": message_id,
                            "chat_id": chat_id,
                            "text": new_text,
                        })

            # ===== УДАЛИТЬ =====
            elif mtype == "delete_message":
                message_id = msg.get("message_id")
                chat_id = msg.get("chat_id")
                if message_id and chat_id:
                    ok = await db_delete_message(message_id, user["id"])
                    if ok:
                        await broadcast_chat(chat_id, {
                            "type": "message_deleted",
                            "message_id": message_id,
                            "chat_id": chat_id,
                        })

            # ===== ПЕРЕСЛАТЬ =====
            elif mtype == "forward_message":
                message_id = msg.get("message_id")
                target_chat_id = msg.get("target_chat_id")
                if message_id and target_chat_id:
                    saved = await db_forward_message(
                        message_id, target_chat_id, user["id"], user["display_name"]
                    )
                    if saved:
                        orig = await db_get_message_by_id(message_id)
                        if orig:
                            await broadcast_chat(target_chat_id, {
                                "type": "new_message",
                                "chat_id": target_chat_id,
                                "id": saved["id"],
                                "sender_id": user["id"],
                                "nickname": user["display_name"],
                                "color": user["color"],
                                "msg_type": orig["msg_type"],
                                "text": orig["text"],
                                "file_data": orig["file_data"],
                                "file_name": orig["file_name"],
                                "file_type": orig["file_type"],
                                "duration": orig.get("duration"),
                                "forwarded_from": orig["sender_name"],
                                "reply_to": None,
                                "time": now_str(),
                                "created_at": saved["created_at"].isoformat(),
                                "edited": False,
                                "reactions": {},
                            })
                            for uid in await db_get_chat_member_ids(target_chat_id):
                                chats = await db_get_user_chats(uid)
                                await send_to_user(uid, {"type": "chats_list", "chats": chats})

            # ===== ПОИСК СООБЩЕНИЙ =====
            elif mtype == "search_messages":
                query = str(msg.get("query", "")).strip()
                chat_id = msg.get("chat_id")
                if not query:
                    continue
                results = await db_search_messages(user["id"], query, chat_id)
                await websocket.send(json.dumps({
                    "type": "search_results",
                    "query": query,
                    "results": results,
                }, ensure_ascii=False))

            # ===== ПОИСК ПОЛЬЗОВАТЕЛЕЙ =====
            elif mtype == "search_users":
                query = str(msg.get("query", "")).strip()
                if not query:
                    continue
                results = await db_search_users(query, exclude_id=user["id"])
                await websocket.send(json.dumps({
                    "type": "user_search_results",
                    "query": query,
                    "users": [user_public(u) for u in results],
                }, ensure_ascii=False))

            # ===== ПРОФИЛЬ =====
            elif mtype == "update_profile":
                await db_update_user(
                    user["id"],
                    display_name=msg.get("display_name"),
                    bio=msg.get("bio"),
                    avatar_data=msg.get("avatar_data"),
                    language=msg.get("language"),
                    theme=msg.get("theme"),
                )
                # Обновляем в памяти
                updated = await db_get_user_by_id(user["id"])
                if updated:
                    user["display_name"] = updated["display_name"]
                    user["bio"] = updated["bio"]
                    user["avatar_data"] = updated["avatar_data"]
                    clients[websocket] = user
                await websocket.send(json.dumps({
                    "type": "profile_updated",
                    "user": user_public(updated) if updated else user_public(user),
                }, ensure_ascii=False))

            # ===== ЧАТ: ИНФО =====
            elif mtype == "get_chat_info":
                chat_id = msg.get("chat_id")
                if chat_id:
                    info = await db_get_chat_info(chat_id)
                    members = await db_get_chat_members(chat_id)
                    if info:
                        if info["created_at"]:
                            info["created_at"] = info["created_at"].isoformat()
                        await websocket.send(json.dumps({
                            "type": "chat_info",
                            "chat": dict(info),
                            "members": [dict(m) for m in members],
                        }, ensure_ascii=False))

            # ===== ГРУППА: ДОБАВИТЬ УЧАСТНИКОВ =====
            elif mtype == "add_members":
                chat_id = msg.get("chat_id")
                user_ids = msg.get("user_ids", [])
                member = await db_check_member(chat_id, user["id"])
                if member and member["role"] == "admin" and user_ids:
                    await db_add_members(chat_id, user_ids)
                    for uid in list(user_ids) + [user["id"]]:
                        chats = await db_get_user_chats(uid)
                        await send_to_user(uid, {"type": "chats_list", "chats": chats})
                    members = await db_get_chat_members(chat_id)
                    await broadcast_chat(chat_id, {
                        "type": "chat_members_updated",
                        "chat_id": chat_id,
                        "members": [dict(m) for m in members],
                    })

            # ===== ГРУППА: УДАЛИТЬ УЧАСТНИКА =====
            elif mtype == "remove_member":
                chat_id = msg.get("chat_id")
                target_id = msg.get("user_id")
                member = await db_check_member(chat_id, user["id"])
                if member and member["role"] == "admin" and target_id:
                    await db_remove_member(chat_id, target_id)
                    await send_to_user(target_id, {
                        "type": "removed_from_chat",
                        "chat_id": chat_id,
                    })
                    chats = await db_get_user_chats(target_id)
                    await send_to_user(target_id, {"type": "chats_list", "chats": chats})
                    members = await db_get_chat_members(chat_id)
                    await broadcast_chat(chat_id, {
                        "type": "chat_members_updated",
                        "chat_id": chat_id,
                        "members": [dict(m) for m in members],
                    })

            # ===== ГРУППА: СДЕЛАТЬ АДМИНОМ =====
            elif mtype == "set_admin":
                chat_id = msg.get("chat_id")
                target_id = msg.get("user_id")
                role = msg.get("role", "admin")
                member = await db_check_member(chat_id, user["id"])
                if member and member["role"] == "admin" and target_id:
                    await db_set_member_role(chat_id, target_id, role)
                    members = await db_get_chat_members(chat_id)
                    await broadcast_chat(chat_id, {
                        "type": "chat_members_updated",
                        "chat_id": chat_id,
                        "members": [dict(m) for m in members],
                    })

            # ===== ГРУППА: ОБНОВИТЬ =====
            elif mtype == "update_group":
                chat_id = msg.get("chat_id")
                member = await db_check_member(chat_id, user["id"])
                if member and member["role"] == "admin":
                    await db_update_group(
                        chat_id,
                        title=msg.get("title"),
                        description=msg.get("description"),
                        avatar_data=msg.get("avatar_data"),
                    )
                    info = await db_get_chat_info(chat_id)
                    await broadcast_chat(chat_id, {
                        "type": "chat_updated",
                        "chat": dict(info) if info else {},
                    })

            # ===== ГРУППА: ВЫЙТИ =====
            elif mtype == "leave_chat":
                chat_id = msg.get("chat_id")
                if chat_id:
                    ok = await db_leave_chat(chat_id, user["id"])
                    if ok:
                        chats = await db_get_user_chats(user["id"])
                        await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
                        members = await db_get_chat_members(chat_id)
                        await broadcast_chat(chat_id, {
                            "type": "chat_members_updated",
                            "chat_id": chat_id,
                            "members": [dict(m) for m in members],
                        })

            # ===== ЗВОНОК: СИГНАЛИНГ =====
            elif mtype in ("call_offer", "call_answer", "call_ice",
                           "call_end", "call_reject", "call_busy"):
                target_id = msg.get("target_id")
                if target_id:
                    await send_to_user(target_id, {
                        **msg,
                        "from_id": user["id"],
                        "from_name": user["display_name"],
                    })

            # ===== PUSH ПОДПИСКА =====
            elif mtype == "push_subscribe":
                sub = msg.get("subscription", {})
                if sub.get("endpoint"):
                    await db_save_push_subscription(
                        user["id"],
                        sub["endpoint"],
                        sub.get("keys", {}).get("p256dh", ""),
                        sub.get("keys", {}).get("auth", ""),
                    )

            # ===== PING =====
            elif mtype == "ping":
                await websocket.send(json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[!] Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if websocket in clients:
            user_info = clients[websocket]
            del clients[websocket]
            if user_info["id"] in user_sockets:
                user_sockets[user_info["id"]].discard(websocket)
                if not user_sockets[user_info["id"]]:
                    del user_sockets[user_info["id"]]
            if user_info["id"]:
                await db_update_last_seen(user_info["id"])
            print(f"[-] {user_info['name']}. Всего: {len(clients)}")


# ============ КОНЕЦ БЛОКА 4 ============
# ============ ОБРАБОТКА НОВОГО СООБЩЕНИЯ ============
async def handle_new_message(websocket, user, msg):
    """Обработка нового сообщения (text, file, voice, circle, image, video)"""
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    # Проверка членства
    member = await db_check_member(chat_id, user["id"])
    if not member:
        await websocket.send(json.dumps({
            "type": "error",
            "text": "Вы не участник этого чата"
        }, ensure_ascii=False))
        return

    msg_type = msg.get("msg_type", "text")
    text = msg.get("text")
    file_data = msg.get("file_data")
    file_name = msg.get("file_name")
    file_type = msg.get("file_type")
    duration = msg.get("duration")
    reply_to = msg.get("reply_to")

    # Валидация
    if text:
        text = str(text)[:4000]
    if file_data:
        max_size = MAX_FILE_SIZE
        if msg_type == "voice":
            max_size = MAX_VOICE_SIZE
        elif msg_type == "avatar":
            max_size = MAX_AVATAR_SIZE
        if len(file_data) > max_size * 1.4:
            await websocket.send(json.dumps({
                "type": "error",
                "text": f"Файл слишком большой (макс {max_size // (1024*1024)} МБ)"
            }, ensure_ascii=False))
            return

    # Пустое сообщение — игнор
    if not text and not file_data:
        return

    # Сохраняем
    saved = await db_save_message(
        chat_id=chat_id,
        sender_id=user["id"],
        sender_name=user["display_name"],
        msg_type=msg_type,
        text=text,
        file_data=file_data,
        file_name=file_name,
        file_type=file_type,
        duration=duration,
        reply_to=reply_to,
    )
    if not saved:
        return

    msg_id = saved["id"]
    created = saved["created_at"].isoformat() if saved["created_at"] else datetime.datetime.now().isoformat()

    # Загружаем инфу о reply-сообщении
    reply_info = None
    if reply_to:
        orig = await db_get_message_by_id(reply_to)
        if orig:
            reply_info = {
                "id": orig["id"],
                "nickname": orig["sender_display"],
                "text": orig["text"] or "📎 Файл",
            }

    # Рассылаем всем участникам
    await broadcast_chat(chat_id, {
        "type": "new_message",
        "chat_id": chat_id,
        "id": msg_id,
        "sender_id": user["id"],
        "nickname": user["display_name"],
        "color": user["color"],
        "avatar_data": user.get("avatar_data"),
        "msg_type": msg_type,
        "text": text,
        "file_data": file_data,
        "file_name": file_name,
        "file_type": file_type,
        "duration": duration,
        "reply_to": reply_info,
        "forwarded_from": None,
        "time": now_str(),
        "created_at": created,
        "edited": False,
        "reactions": {},
    })

    # Обновляем список чатов у всех участников
    for uid in await db_get_chat_member_ids(chat_id):
        chats = await db_get_user_chats(uid)
        await send_to_user(uid, {"type": "chats_list", "chats": chats})


# ============ MAIN ============
async def main():
    await init_db()
    port = int(os.environ.get("PORT", 8765))
    print(f"🚀 Blaze Messenger v2.0 на порту {port}")
    print(f"📦 Модули: чаты, группы, голосовые, кружки, реакции, поиск,")
    print(f"          профили, языки, push, звонки (WebRTC)")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        process_request=process_request,
        max_size=15 * 1024 * 1024,   # 15 МБ для входящих
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())