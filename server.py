# ============================================================
#  BLAZE MESSENGER v3.0 — SERVER (без email)
# ============================================================

import asyncio
import websockets
import json
import datetime
import os
import http
import re
import secrets
import base64
from typing import Optional, Dict, List, Any

import psycopg
from psycopg.rows import dict_row
import bcrypt
import jwt

try:
    from cryptography.fernet import Fernet, InvalidToken
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    print("⚠️ cryptography не установлена")


# ============ КОНФИГ ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGO = "HS256"
JWT_EXP_DAYS = 30

MAX_HISTORY = 200
MAX_FILE_SIZE = 10 * 1024 * 1024
MAX_VOICE_SIZE = 3 * 1024 * 1024
MAX_AVATAR_SIZE = 2 * 1024 * 1024
MAX_MESSAGE_LENGTH = 4000
MAX_BIO_LENGTH = 200
MAX_DISPLAY_NAME = 40
MAX_USERNAME = 20
MAX_GROUP_TITLE = 80
MAX_GROUP_DESCRIPTION = 500


# ============ ШИФРОВАНИЕ ============
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "").encode()
cipher = None
if ENCRYPTION_KEY and CRYPTO_AVAILABLE:
    try:
        cipher = Fernet(ENCRYPTION_KEY)
        print("🔐 Шифрование БД: ВКЛЮЧЕНО")
    except Exception as e:
        print(f"⚠️ Ошибка ключа шифрования: {e}")
        cipher = None
else:
    print("⚠️ Шифрование БД: ВЫКЛЮЧЕНО")


def encrypt_text(text):
    if not cipher or not text:
        return text
    try:
        return cipher.encrypt(text.encode()).decode()
    except Exception:
        return text


def decrypt_text(text):
    if not cipher or not text:
        return text
    try:
        return cipher.decrypt(text.encode()).decode()
    except (InvalidToken, Exception):
        return text


# ============ УТИЛИТЫ ============
def now_str():
    return datetime.datetime.now().strftime("%H:%M")


def now_iso():
    return datetime.datetime.now().isoformat()


def validate_username(username: str) -> bool:
    if not username:
        return False
    if len(username) < 3 or len(username) > MAX_USERNAME:
        return False
    return bool(re.match(r"^[a-zA-Z0-9_]+$", username))


def get_file_size_base64(b64_str: str) -> int:
    if not b64_str:
        return 0
    try:
        padding = b64_str.count("=")
        return (len(b64_str) * 3) // 4 - padding
    except Exception:
        return len(b64_str)


def db_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# === КОНЕЦ ЧАСТИ 1/9 ===
,# ============================================================
#  ИНИЦИАЛИЗАЦИЯ БД
# ============================================================
async def init_db():
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL не задан")
        return

    def _init():
        with db_conn() as conn:
            with conn.cursor() as cur:

                # ===== USERS =====
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
                        is_online BOOLEAN DEFAULT FALSE,
                        last_seen TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # ===== CHATS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS chats (
                        id SERIAL PRIMARY KEY,
                        type TEXT DEFAULT 'private',
                        title TEXT,
                        description TEXT DEFAULT '',
                        avatar_data TEXT,
                        avatar_color TEXT DEFAULT '#ff6b00',
                        is_public BOOLEAN DEFAULT FALSE,
                        invite_code TEXT UNIQUE,
                        created_by INTEGER REFERENCES users(id),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # ===== CHAT MEMBERS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS chat_members (
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        role TEXT DEFAULT 'member',
                        last_read_id INTEGER DEFAULT 0,
                        muted BOOLEAN DEFAULT FALSE,
                        joined_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (chat_id, user_id)
                    );
                """)

                # ===== MESSAGES =====
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

                # ===== REACTIONS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reactions (
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        emoji TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (message_id, user_id)
                    );
                """)

                # ===== PUSH SUBSCRIPTIONS =====
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

                # ===== MESSAGE READS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS message_reads (
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        read_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (message_id, user_id)
                    );
                """)

                # ===== PINNED CHATS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pinned_chats (
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        pinned_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (user_id, chat_id)
                    );
                """)

                # ===== PINNED MESSAGES =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pinned_messages (
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        pinned_by INTEGER REFERENCES users(id),
                        pinned_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (chat_id, message_id)
                    );
                """)

                # ===== ARCHIVED CHATS =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS archived_chats (
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        archived_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (user_id, chat_id)
                    );
                """)

                # ===== ИНДЕКСЫ =====
                cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_members_user ON chat_members(user_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_reactions_message ON reactions(message_id);")

                # ===== ОБЩИЙ ЧАТ =====
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                if not cur.fetchone():
                    cur.execute("INSERT INTO chats (type, title, description) VALUES ('global', 'Общий чат', 'Все пользователи Blaze')")

            conn.commit()

    try:
        await asyncio.to_thread(_init)
        print("✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        import traceback
        traceback.print_exc()


# === КОНЕЦ ЧАСТИ 2/9 ===
# ============================================================
#  USERS
# ============================================================
async def db_create_user(username, password):
    if not DATABASE_URL:
        return None
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    def _q():
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    colors = [
                        "#ff6b00", "#e53935", "#ff8c1a", "#d32f2f",
                        "#ff5722", "#f4511e", "#bf360c", "#e64a19"
                    ]
                    color = colors[sum(ord(c) for c in username) % len(colors)]
                    cur.execute(
                        "INSERT INTO users (username, display_name, password_hash, avatar_color) "
                        "VALUES (%s, %s, %s, %s) "
                        "RETURNING id, username, display_name, avatar_color, bio, "
                        "avatar_data, language, theme",
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
                    "SELECT id, username, display_name, password_hash, avatar_color, "
                    "bio, avatar_data, language, theme, last_seen "
                    "FROM users WHERE username=%s",
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
                    "SELECT id, username, display_name, avatar_color, bio, "
                    "avatar_data, language, theme, is_online, last_seen "
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
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, is_online, last_seen FROM users "
                        "WHERE id != %s ORDER BY display_name NULLS LAST, username",
                        (exclude_id,)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, is_online, last_seen FROM users "
                        "ORDER BY display_name NULLS LAST, username"
                    )
                return cur.fetchall()

    return await asyncio.to_thread(_q)


async def db_search_users(query, exclude_id=None):
    if not DATABASE_URL:
        return []
    query = query.strip()
    if not query:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                q = f"%{query}%"
                if exclude_id:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, is_online, last_seen FROM users "
                        "WHERE (username ILIKE %s OR display_name ILIKE %s) "
                        "AND id != %s ORDER BY display_name LIMIT 50",
                        (q, q, exclude_id)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, is_online, last_seen FROM users "
                        "WHERE username ILIKE %s OR display_name ILIKE %s "
                        "ORDER BY display_name LIMIT 50",
                        (q, q)
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
                    params.append(bio[:MAX_BIO_LENGTH])

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


async def db_change_username(user_id, new_username):
    if not DATABASE_URL:
        return False
    if not validate_username(new_username):
        return False

    def _q():
        try:
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET username=%s WHERE id=%s RETURNING id",
                        (new_username, user_id)
                    )
                    ok = cur.fetchone() is not None
                conn.commit()
                return ok
        except psycopg.errors.UniqueViolation:
            return False

    return await asyncio.to_thread(_q)


async def db_update_last_seen(user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET last_seen=NOW(), is_online=FALSE WHERE id=%s",
                    (user_id,)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_set_online(user_id, is_online: bool):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET is_online=%s, last_seen=NOW() WHERE id=%s",
                    (is_online, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 3/9 ===
# ============================================================
#  CHATS
# ============================================================
async def db_get_global_chat():
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_add_user_to_global_chat(user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                row = cur.fetchone()
                if not row:
                    return
                chat_id = row["id"]
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'member')
                    ON CONFLICT DO NOTHING
                """, (chat_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


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


async def db_create_group(creator_id, title, member_ids=None, description=""):
    if not DATABASE_URL:
        return None

    title = (title or "").strip()[:MAX_GROUP_TITLE]
    if not title:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO chats (type, title, description, created_by)
                    VALUES ('group', %s, %s, %s)
                    RETURNING id
                """, (title, description[:MAX_GROUP_DESCRIPTION], creator_id))
                chat_id = cur.fetchone()["id"]

                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'admin')
                """, (chat_id, creator_id))

                if member_ids:
                    for uid in member_ids:
                        if uid == creator_id:
                            continue
                        cur.execute("""
                            INSERT INTO chat_members (chat_id, user_id, role)
                            VALUES (%s, %s, 'member')
                            ON CONFLICT DO NOTHING
                        """, (chat_id, uid))
            conn.commit()
            return {"id": chat_id}

    return await asyncio.to_thread(_q)


async def db_create_channel(creator_id, title, description="", is_public=False):
    if not DATABASE_URL:
        return None

    title = (title or "").strip()[:MAX_GROUP_TITLE]
    if not title:
        return None

    invite_code = secrets.token_urlsafe(12) if is_public else None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO chats (type, title, description, is_public, invite_code, created_by)
                    VALUES ('channel', %s, %s, %s, %s, %s)
                    RETURNING id
                """, (title, description[:MAX_GROUP_DESCRIPTION], is_public, invite_code, creator_id))
                chat_id = cur.fetchone()["id"]

                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'admin')
                """, (chat_id, creator_id))
            conn.commit()
            return {"id": chat_id, "invite_code": invite_code}

    return await asyncio.to_thread(_q)


async def db_get_chat_info(chat_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, type, title, description, avatar_data, avatar_color,
                           is_public, invite_code, created_by, created_at
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
                           u.avatar_data, u.last_seen, u.is_online,
                           m.role, m.joined_at, m.muted
                    FROM chat_members m
                    JOIN users u ON u.id = m.user_id
                    WHERE m.chat_id=%s
                    ORDER BY m.role DESC, u.display_name NULLS LAST, u.username
                """, (chat_id,))
                rows = cur.fetchall()
                for r in rows:
                    if r["last_seen"]:
                        r["last_seen"] = r["last_seen"].isoformat()
                    if r["joined_at"]:
                        r["joined_at"] = r["joined_at"].isoformat()
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
                    "SELECT role, muted FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_is_admin(chat_id, user_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role FROM chat_members "
                    "WHERE chat_id=%s AND user_id=%s AND role='admin'",
                    (chat_id, user_id)
                )
                return cur.fetchone() is not None

    return await asyncio.to_thread(_q)


async def db_add_members(chat_id, user_ids):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                for uid in user_ids:
                    cur.execute("""
                        INSERT INTO chat_members (chat_id, user_id, role)
                        VALUES (%s, %s, 'member')
                        ON CONFLICT DO NOTHING
                    """, (chat_id, uid))
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
    if role not in ("admin", "member"):
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


async def db_update_group(chat_id, title=None, description=None, avatar_data=None, avatar_color=None):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                updates = []
                params = []

                if title is not None:
                    updates.append("title=%s")
                    params.append(title[:MAX_GROUP_TITLE])
                if description is not None:
                    updates.append("description=%s")
                    params.append(description[:MAX_GROUP_DESCRIPTION])
                if avatar_data is not None:
                    updates.append("avatar_data=%s")
                    params.append(avatar_data)
                if avatar_color is not None:
                    updates.append("avatar_color=%s")
                    params.append(avatar_color)

                if not updates:
                    return

                params.append(chat_id)
                cur.execute(f"UPDATE chats SET {', '.join(updates)} WHERE id=%s", params)
            conn.commit()

    await asyncio.to_thread(_q)


async def db_leave_chat(chat_id, user_id):
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

                cur.execute("SELECT COUNT(*) as cnt FROM chat_members WHERE chat_id=%s", (chat_id,))
                cnt_row = cur.fetchone()
                if cnt_row and cnt_row["cnt"] == 0:
                    cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_mark_chat_read(chat_id, user_id, last_message_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE chat_members SET last_read_id=%s
                    WHERE chat_id=%s AND user_id=%s
                    AND (last_read_id IS NULL OR last_read_id < %s)
                """, (last_message_id, chat_id, user_id, last_message_id))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_user_chats(user_id):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Pinned chats
                cur.execute("SELECT chat_id FROM pinned_chats WHERE user_id=%s", (user_id,))
                pinned_ids = {r["chat_id"] for r in cur.fetchall()}

                # Archived chats
                cur.execute("SELECT chat_id FROM archived_chats WHERE user_id=%s", (user_id,))
                archived_ids = {r["chat_id"] for r in cur.fetchall()}

                cur.execute("""
                    SELECT
                        c.id, c.type, c.title, c.description, c.avatar_data,
                        c.avatar_color, c.is_public, c.invite_code, c.created_at,
                        (SELECT COALESCE(u2.display_name, u2.username)
                         FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_name,
                        (SELECT u2.avatar_color FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_color,
                        (SELECT u2.avatar_data FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_avatar,
                        (SELECT u2.id FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_id,
                        (SELECT u2.last_seen FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_last_seen,
                        (SELECT u2.is_online FROM users u2 WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_online,
                        (SELECT text FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_text,
                        (SELECT msg_type FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_type,
                        (SELECT sender_name FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_sender,
                        (SELECT created_at FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_time,
                        (SELECT COUNT(*) FROM messages
                         WHERE chat_id=c.id
                         AND id > COALESCE((SELECT last_read_id FROM chat_members
                             WHERE chat_id=c.id AND user_id=%s), 0)
                         AND sender_id != %s AND NOT deleted) as unread,
                        (SELECT COUNT(*) FROM chat_members WHERE chat_id=c.id) as member_count,
                        (SELECT muted FROM chat_members WHERE chat_id=c.id AND user_id=%s) as muted
                    FROM chats c
                    JOIN chat_members m ON m.chat_id=c.id AND m.user_id=%s
                    ORDER BY last_time DESC NULLS LAST, c.id DESC
                """, (user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id))
                rows = cur.fetchall()

                result = []
                for r in rows:
                    if r.get("last_text"):
                        r["last_text"] = decrypt_text(r["last_text"])
                    if r["last_time"]:
                        r["last_time"] = r["last_time"].isoformat()
                    if r["other_last_seen"]:
                        r["other_last_seen"] = r["other_last_seen"].isoformat()
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()

                    r["pinned"] = r["id"] in pinned_ids
                    r["archived"] = r["id"] in archived_ids

                    result.append(dict(r))

                # Сортировка: pinned сверху
                pinned = [c for c in result if c["pinned"]]
                others = [c for c in result if not c["pinned"]]
                pinned.sort(key=lambda x: x.get("last_time") or "", reverse=True)
                others.sort(key=lambda x: x.get("last_time") or "", reverse=True)

                return pinned + others

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 4/9 ===
# ============================================================
#  MESSAGES
# ============================================================
async def db_save_message(chat_id, sender_id, sender_name, msg_type='text',
                          text=None, file_data=None, file_name=None,
                          file_type=None, duration=None, reply_to=None,
                          forwarded_from=None):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                encrypted_text = encrypt_text(text) if text else None
                cur.execute("""
                    INSERT INTO messages
                    (chat_id, sender_id, sender_name, msg_type, text,
                     file_data, file_name, file_type, duration, reply_to, forwarded_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (chat_id, sender_id, sender_name, msg_type, encrypted_text,
                      file_data, file_name, file_type, duration, reply_to, forwarded_from))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


async def db_get_chat_history(chat_id, limit=MAX_HISTORY):
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
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
                    cur.execute("""
                        SELECT emoji, COUNT(*) as cnt FROM reactions
                        WHERE message_id=%s GROUP BY emoji
                    """, (m["id"],))
                    m["reactions"] = {r["emoji"]: r["cnt"] for r in cur.fetchall()}

                    if m.get("text"):
                        m["text"] = decrypt_text(m["text"])

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
                if not row:
                    return None
                if row.get("text"):
                    row["text"] = decrypt_text(row["text"])
                if row["created_at"]:
                    row["created_at"] = row["created_at"].isoformat()
                return row

    return await asyncio.to_thread(_q)


async def db_edit_message(message_id, user_id, new_text):
    if not DATABASE_URL:
        return False

    new_text = (new_text or "").strip()[:MAX_MESSAGE_LENGTH]
    if not new_text:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                encrypted = encrypt_text(new_text)
                cur.execute("""
                    UPDATE messages SET text=%s, edited=TRUE
                    WHERE id=%s AND sender_id=%s
                    AND msg_type='text' AND NOT deleted
                    RETURNING id
                """, (encrypted, message_id, user_id))
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
                cur.execute("""
                    UPDATE messages SET deleted=TRUE, text=NULL, file_data=NULL
                    WHERE id=%s AND sender_id=%s
                    RETURNING id
                """, (message_id, user_id))
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_delete_message_admin(message_id, chat_id, user_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT role FROM chat_members
                    WHERE chat_id=%s AND user_id=%s AND role='admin'
                """, (chat_id, user_id))
                if not cur.fetchone():
                    return False

                cur.execute("""
                    UPDATE messages SET deleted=TRUE, text=NULL, file_data=NULL
                    WHERE id=%s AND chat_id=%s RETURNING id
                """, (message_id, chat_id))
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_toggle_reaction(message_id, user_id, emoji):
    if not DATABASE_URL:
        return {}
    emoji = (emoji or "").strip()
    if not emoji or len(emoji) > 8:
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


async def db_get_my_reactions(chat_id, user_id):
    if not DATABASE_URL:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT r.message_id, r.emoji
                    FROM reactions r
                    JOIN messages m ON m.id = r.message_id
                    WHERE r.user_id=%s AND m.chat_id=%s
                """, (user_id, chat_id))
                return {r["message_id"]: r["emoji"] for r in cur.fetchall()}

    return await asyncio.to_thread(_q)


async def db_search_messages(user_id, query, chat_id=None, limit=100):
    if not DATABASE_URL:
        return []
    query = (query or "").strip()
    if not query:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                if chat_id:
                    cur.execute(
                        "SELECT role FROM chat_members WHERE chat_id=%s AND user_id=%s",
                        (chat_id, user_id)
                    )
                    if not cur.fetchone():
                        return []

                    cur.execute("""
                        SELECT m.id, m.chat_id, m.sender_id, m.sender_name,
                               m.text, m.created_at, m.msg_type,
                               c.title, c.type,
                               COALESCE(u.display_name, u.username) as sender_display
                        FROM messages m
                        JOIN chats c ON c.id = m.chat_id
                        LEFT JOIN users u ON u.id = m.sender_id
                        WHERE m.chat_id = %s
                        AND m.text IS NOT NULL AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT %s
                    """, (chat_id, limit))
                else:
                    cur.execute("""
                        SELECT m.id, m.chat_id, m.sender_id, m.sender_name,
                               m.text, m.created_at, m.msg_type,
                               c.title, c.type,
                               COALESCE(u.display_name, u.username) as sender_display
                        FROM messages m
                        JOIN chats c ON c.id = m.chat_id
                        JOIN chat_members cm ON cm.chat_id = c.id AND cm.user_id = %s
                        LEFT JOIN users u ON u.id = m.sender_id
                        WHERE m.text IS NOT NULL AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT %s
                    """, (user_id, limit))

                rows = cur.fetchall()
                matched = []
                q_lower = query.lower()
                for r in rows:
                    text = r.get("text")
                    if not text:
                        continue
                    decrypted = decrypt_text(text)
                    if q_lower in decrypted.lower():
                        r["text"] = decrypted
                        if r["created_at"]:
                            r["created_at"] = r["created_at"].isoformat()
                        matched.append(r)
                        if len(matched) >= limit:
                            break
                return matched

    return await asyncio.to_thread(_q)


async def db_forward_message(message_id, target_chat_id, sender_id, sender_name):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT msg_type, text, file_data, file_name, file_type,
                           duration, sender_name
                    FROM messages WHERE id=%s AND NOT deleted
                """, (message_id,))
                orig = cur.fetchone()
                if not orig:
                    return None

                cur.execute("""
                    INSERT INTO messages
                    (chat_id, sender_id, sender_name, msg_type, text,
                     file_data, file_name, file_type, duration, forwarded_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (target_chat_id, sender_id, sender_name,
                      orig["msg_type"], orig["text"], orig["file_data"],
                      orig["file_name"], orig["file_type"], orig["duration"],
                      orig["sender_name"]))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


# ============================================================
#  MESSAGE READS
# ============================================================
async def db_mark_message_read(message_id, user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO message_reads (message_id, user_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (message_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


# ============================================================
#  PINS / ARCHIVE / MUTE
# ============================================================
async def db_pin_chat(user_id, chat_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                if not cur.fetchone():
                    return False
                cur.execute("""
                    INSERT INTO pinned_chats (user_id, chat_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (user_id, chat_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unpin_chat(user_id, chat_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pinned_chats WHERE user_id=%s AND chat_id=%s",
                    (user_id, chat_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_archive_chat(user_id, chat_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                if not cur.fetchone():
                    return False
                cur.execute("""
                    INSERT INTO archived_chats (user_id, chat_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (user_id, chat_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unarchive_chat(user_id, chat_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM archived_chats WHERE user_id=%s AND chat_id=%s",
                    (user_id, chat_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_mute_chat(chat_id, user_id, muted=True):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE chat_members SET muted=%s WHERE chat_id=%s AND user_id=%s",
                    (muted, chat_id, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_delete_chat_for_user(chat_id, user_id):
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT type FROM chats WHERE id=%s", (chat_id,))
                row = cur.fetchone()
                if not row:
                    return False

                if row["type"] == "private":
                    cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
                else:
                    cur.execute(
                        "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                        (chat_id, user_id)
                    )
                    cur.execute("SELECT COUNT(*) as cnt FROM chat_members WHERE chat_id=%s", (chat_id,))
                    cnt_row = cur.fetchone()
                    if cnt_row and cnt_row["cnt"] == 0:
                        cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_clear_chat_history(chat_id, user_id):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(id) as last_id FROM messages WHERE chat_id=%s", (chat_id,))
                row = cur.fetchone()
                last_id = row["last_id"] if row and row["last_id"] else 0
                cur.execute("""
                    UPDATE chat_members SET last_read_id=%s
                    WHERE chat_id=%s AND user_id=%s
                """, (last_id, chat_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 5/9 ==
# ============================================================
#  WEBSOCKET — СОСТОЯНИЕ
# ============================================================
clients: Dict[Any, Dict[str, Any]] = {}
user_sockets: Dict[int, set] = {}
clients_lock = asyncio.Lock()


async def process_request(path, request_headers):
    if "Upgrade" not in request_headers.get("Connection", ""):
        if path == "/" or path == "":
            html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Blaze Messenger API</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0a0a0a;color:#fff;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background-image:radial-gradient(circle at 0% 0%,rgba(255,107,0,0.15) 0%,transparent 40%),radial-gradient(circle at 100% 100%,rgba(229,57,53,0.15) 0%,transparent 40%);}
.box{text-align:center;padding:40px 60px;background:rgba(22,22,28,0.85);border-radius:24px;backdrop-filter:blur(20px);border:1px solid rgba(255,255,255,0.08);box-shadow:0 30px 90px rgba(0,0,0,0.7),0 0 100px rgba(255,107,0,0.15);}
.flame{font-size:72px;line-height:1;filter:drop-shadow(0 0 20px rgba(255,107,0,0.6));}
h1{background:linear-gradient(135deg,#ff6b00,#e53935);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:32px;margin:12px 0 8px;letter-spacing:-1px;}
.status{color:#4ade80;font-size:14px;font-weight:600;margin-top:20px;}
.status::before{content:"●";margin-right:8px;animation:pulse 2s infinite;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.4;}}
p{color:#a0a0aa;font-size:14px;margin:6px 0;}
</style>
</head>
<body>
<div class="box">
<div class="flame">🔥</div>
<h1>Blaze Messenger</h1>
<p>WebSocket API v3.0</p>
<div class="status">Server is running</div>
</div>
</body>
</html>"""
            return http.HTTPStatus.OK, [("Content-Type", "text/html; charset=utf-8")], html.encode("utf-8")

        return http.HTTPStatus.OK, [], b"Blaze Messenger v3.0 is running\n"

    return None


async def send_safe(ws, data: str):
    try:
        await ws.send(data)
    except Exception:
        pass


async def send_json(ws, obj: dict):
    try:
        await ws.send(json.dumps(obj, ensure_ascii=False))
    except Exception:
        pass


async def send_to_user(user_id: int, message: dict):
    if not user_id or user_id not in user_sockets:
        return
    data = json.dumps(message, ensure_ascii=False)
    sockets = list(user_sockets[user_id])
    if not sockets:
        return
    await asyncio.gather(
        *[send_safe(ws, data) for ws in sockets],
        return_exceptions=True,
    )


async def send_to_users(user_ids: List[int], message: dict):
    if not user_ids:
        return
    data = json.dumps(message, ensure_ascii=False)
    tasks = []
    for uid in user_ids:
        if uid in user_sockets:
            for ws in list(user_sockets[uid]):
                if ws.open:
                    tasks.append(send_safe(ws, data))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def broadcast_chat(chat_id: int, message: dict, exclude=None):
    member_ids = await db_get_chat_member_ids(chat_id)
    if not member_ids:
        return
    data = json.dumps(message, ensure_ascii=False)
    tasks = []
    for uid in member_ids:
        if uid not in user_sockets:
            continue
        for ws in list(user_sockets[uid]):
            if ws == exclude:
                continue
            if ws.open:
                tasks.append(send_safe(ws, data))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def refresh_chats_list(user_ids: List[int]):
    for uid in user_ids:
        if uid not in user_sockets:
            continue
        try:
            chats = await db_get_user_chats(uid)
            await send_to_user(uid, {"type": "chats_list", "chats": chats})
        except Exception as e:
            print(f"refresh_chats_list error for {uid}: {e}")


async def refresh_chats_for_chat(chat_id: int):
    member_ids = await db_get_chat_member_ids(chat_id)
    await refresh_chats_list(member_ids)


def user_public(u: dict) -> dict:
    if not u:
        return {}
    username = u.get("username") or u.get("name") or ""
    display_name = u.get("display_name") or username
    last_seen = u.get("last_seen")
    if isinstance(last_seen, datetime.datetime):
        last_seen = last_seen.isoformat()

    return {
        "id": u.get("id"),
        "username": username,
        "display_name": display_name,
        "color": u.get("avatar_color") or u.get("color") or "#ff6b00",
        "avatar_data": u.get("avatar_data"),
        "bio": u.get("bio", "") or "",
        "last_seen": last_seen,
        "is_online": bool(u.get("is_online", False)),
    }


async def safe_close(ws, code=1000, reason=""):
    try:
        await ws.close(code, reason)
    except Exception:
        pass


async def disconnect_user(ws, user_info: Optional[dict] = None):
    async with clients_lock:
        info = clients.pop(ws, None)

    info = info or user_info
    if not info:
        return

    uid = info.get("id")
    if uid and uid in user_sockets:
        user_sockets[uid].discard(ws)
        if not user_sockets[uid]:
            del user_sockets[uid]
            try:
                await db_set_online(uid, False)
                await db_update_last_seen(uid)
            except Exception as e:
                print(f"Set offline error: {e}")


# ============================================================
#  АВТОРИЗАЦИЯ
# ============================================================
async def handle_auth(websocket, first_msg: dict):
    action = first_msg.get("action")

    # ===== ВХОД ПО JWT-ТОКЕНУ =====
    if action == "token":
        token = first_msg.get("token", "")
        if not token:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Токен не передан"
            })
            return None

        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
            user_id = payload.get("user_id")
            if not user_id:
                raise ValueError("No user_id")

            u = await db_get_user_by_id(user_id)
            if not u:
                await send_json(websocket, {
                    "type": "auth_error",
                    "text": "Сессия истекла"
                })
                return None

            return {
                "id": u["id"],
                "name": u["username"],
                "display_name": u.get("display_name") or u["username"],
                "color": u.get("avatar_color", "#ff6b00"),
                "bio": u.get("bio", "") or "",
                "avatar_data": u.get("avatar_data"),
                "language": u.get("language", "ru"),
                "theme": u.get("theme", "dark"),
                "token": token,
            }

        except jwt.ExpiredSignatureError:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Сессия истекла — войди заново"
            })
            return None
        except Exception as e:
            print(f"Token auth error: {e}")
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Ошибка авторизации"
            })
            return None

    # ===== ОБЫЧНЫЙ ВХОД / РЕГИСТРАЦИЯ =====
    username = str(first_msg.get("username", "")).strip()[:MAX_USERNAME]
    password = str(first_msg.get("password", ""))

    if not username or not password:
        await send_json(websocket, {
            "type": "auth_error",
            "text": "Логин и пароль обязательны"
        })
        return None

    if not DATABASE_URL:
        return {
            "id": 1,
            "name": username,
            "display_name": username,
            "color": "#ff6b00",
            "bio": "",
            "avatar_data": None,
            "language": "ru",
            "theme": "dark",
            "token": "",
        }

    if action == "register":
        if not validate_username(username):
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Ник: 3-20 символов (латиница, цифры, _)"
            })
            return None

        if len(password) < 4:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Пароль минимум 4 символа"
            })
            return None

        user = await db_create_user(username, password)
        if not user:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Такой пользователь уже есть"
            })
            return None

        try:
            await db_add_user_to_global_chat(user["id"])
        except Exception as e:
            print(f"Add to global chat error: {e}")

    elif action == "login":
        user = await db_get_user(username)
        if not user or not bcrypt.checkpw(
            password.encode(), user["password_hash"].encode()
        ):
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Неверный логин или пароль"
            })
            return None
    else:
        await send_json(websocket, {
            "type": "auth_error",
            "text": "Укажите action: login или register"
        })
        return None

    token = jwt.encode({
        "user_id": user["id"],
        "username": user["username"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=JWT_EXP_DAYS),
    }, JWT_SECRET, algorithm=JWT_ALGO)

    try:
        await db_set_online(user["id"], True)
    except Exception:
        pass

    return {
        "id": user["id"],
        "name": user["username"],
        "display_name": user.get("display_name") or user["username"],
        "color": user.get("avatar_color", "#ff6b00"),
        "bio": user.get("bio", "") or "",
        "avatar_data": user.get("avatar_data"),
        "language": user.get("language", "ru"),
        "theme": user.get("theme", "dark"),
        "token": token,
    }


# === КОНЕЦ ЧАСТИ 6/9 ===
# ============================================================
#  MAIN HANDLER
# ============================================================
async def handler(websocket):
    user = None
    try:
        raw = await websocket.recv()
        try:
            first = json.loads(raw)
        except json.JSONDecodeError:
            await safe_close(websocket, 1003, "Invalid JSON")
            return

        user = await handle_auth(websocket, first)
        if not user:
            await safe_close(websocket, 1008, "Auth failed")
            return

        async with clients_lock:
            clients[websocket] = user
            user_sockets.setdefault(user["id"], set()).add(websocket)

        print(f"[+] {user['name']} (id={user['id']}). Всего: {len(clients)}")

        # auth_ok
        await send_json(websocket, {
            "type": "auth_ok",
            "user": user_public(user),
            "language": user.get("language", "ru"),
            "theme": user.get("theme", "dark"),
            "token": user.get("token", ""),
        })

        # Список чатов
        try:
            chats = await db_get_user_chats(user["id"])
            await send_json(websocket, {
                "type": "chats_list",
                "chats": chats,
            })
        except Exception as e:
            print(f"chats_list error: {e}")

        # Все пользователи
        try:
            users = await db_get_all_users(exclude_id=user["id"])
            await send_json(websocket, {
                "type": "all_users",
                "users": [user_public(u) for u in users],
            })
        except Exception as e:
            print(f"all_users error: {e}")

        # Уведомить остальных, что онлайн
        try:
            online_notif = {
                "type": "user_online",
                "user_id": user["id"],
                "username": user["name"],
                "display_name": user["display_name"],
                "is_online": True,
            }
            for uid, sockets in list(user_sockets.items()):
                if uid == user["id"]:
                    continue
                for ws in list(sockets):
                    if ws.open:
                        try:
                            await send_json(ws, online_notif)
                        except Exception:
                            pass
        except Exception:
            pass

        # Основной цикл
        async for raw_msg in websocket:
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")
            if not mtype:
                continue

            try:
                await route_message(websocket, user, mtype, msg)
            except websockets.exceptions.ConnectionClosed:
                break
            except Exception as e:
                print(f"[!] Ошибка обработки '{mtype}': {e}")
                import traceback
                traceback.print_exc()
                try:
                    await send_json(websocket, {
                        "type": "error",
                        "text": f"Ошибка: {mtype}"
                    })
                except Exception:
                    pass

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[!] Handler error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if websocket in clients:
            user_info = clients[websocket]
            await disconnect_user(websocket, user_info)

            print(f"[-] {user_info.get('name')}. Всего: {len(clients)}")

            try:
                offline_notif = {
                    "type": "user_offline",
                    "user_id": user_info["id"],
                    "username": user_info["name"],
                    "is_online": False,
                }
                for uid, sockets in list(user_sockets.items()):
                    if uid == user_info["id"]:
                        continue
                    for ws in list(sockets):
                        if ws.open:
                            try:
                                await send_json(ws, offline_notif)
                            except Exception:
                                pass
            except Exception:
                pass


# ============================================================
#  МАРШРУТИЗАЦИЯ
# ============================================================
async def route_message(websocket, user, mtype, msg):
    # Сообщения
    if mtype == "message":
        await handle_new_message(websocket, user, msg)
    elif mtype == "open_chat":
        await handle_open_chat(websocket, user, msg)
    elif mtype == "typing":
        await handle_typing(websocket, user, msg)
    elif mtype == "read":
        await handle_read(websocket, user, msg)

    # Реакции / edit / delete / forward
    elif mtype == "react":
        await handle_react(websocket, user, msg)
    elif mtype == "edit_message":
        await handle_edit_message(websocket, user, msg)
    elif mtype == "delete_message":
        await handle_delete_message(websocket, user, msg)
    elif mtype == "forward_message":
        await handle_forward_message(websocket, user, msg)

    # Чаты
    elif mtype == "create_private":
        await handle_create_private(websocket, user, msg)
    elif mtype == "create_group":
        await handle_create_group(websocket, user, msg)
    elif mtype == "create_channel":
        await handle_create_channel(websocket, user, msg)
    elif mtype == "get_chat_info":
        await handle_get_chat_info(websocket, user, msg)
    elif mtype == "add_members":
        await handle_add_members(websocket, user, msg)
    elif mtype == "remove_member":
        await handle_remove_member(websocket, user, msg)
    elif mtype == "set_admin":
        await handle_set_admin(websocket, user, msg)
    elif mtype == "update_group":
        await handle_update_group(websocket, user, msg)
    elif mtype == "leave_chat":
        await handle_leave_chat(websocket, user, msg)
    elif mtype == "delete_chat":
        await handle_delete_chat(websocket, user, msg)

    # Пин/архив/мьют
    elif mtype == "pin_chat":
        await handle_pin_chat(websocket, user, msg, True)
    elif mtype == "unpin_chat":
        await handle_pin_chat(websocket, user, msg, False)
    elif mtype == "archive_chat":
        await handle_archive_chat(websocket, user, msg, True)
    elif mtype == "unarchive_chat":
        await handle_archive_chat(websocket, user, msg, False)
    elif mtype == "mute_chat":
        await handle_mute_chat(websocket, user, msg)

    # Поиск
    elif mtype == "search_messages":
        await handle_search_messages(websocket, user, msg)
    elif mtype == "search_users":
        await handle_search_users(websocket, user, msg)

    # Профиль
    elif mtype == "update_profile":
        await handle_update_profile(websocket, user, msg)
    elif mtype == "change_username":
        await handle_change_username(websocket, user, msg)

    # Звонки
    elif mtype in ("call_offer", "call_answer", "call_ice",
                   "call_end", "call_reject", "call_busy"):
        await handle_call_signal(websocket, user, msg)

    # Push
    elif mtype == "push_subscribe":
        await handle_push_subscribe(websocket, user, msg)

    # Ping
    elif mtype == "ping":
        await send_json(websocket, {"type": "pong"})

    else:
        await send_json(websocket, {
            "type": "error",
            "text": f"Неизвестный тип: {mtype}"
        })


# === КОНЕЦ ЧАСТИ 7/9 ===
# ============================================================
#  НОВОЕ СООБЩЕНИЕ
# ============================================================
async def handle_new_message(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        await send_json(websocket, {"type": "error", "text": "Нет доступа"})
        return

    # Проверка прав для канала
    chat_info = await db_get_chat_info(chat_id)
    if chat_info and chat_info["type"] == "channel":
        if member.get("role") != "admin":
            await send_json(websocket, {"type": "error", "text": "Только админы"})
            return

    msg_type = msg.get("msg_type", "text")
    text = msg.get("text")
    file_data = msg.get("file_data")
    file_name = msg.get("file_name")
    file_type = msg.get("file_type")
    duration = msg.get("duration")
    reply_to = msg.get("reply_to")

    if text:
        text = str(text)[:MAX_MESSAGE_LENGTH]

    if file_data:
        max_size = MAX_FILE_SIZE
        if msg_type == "voice":
            max_size = MAX_VOICE_SIZE
        elif msg_type == "avatar":
            max_size = MAX_AVATAR_SIZE
        size = get_file_size_base64(file_data)
        if size > max_size:
            await send_json(websocket, {
                "type": "error",
                "text": f"Файл слишком большой"
            })
            return

    if not text and not file_data:
        return

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
    created = saved["created_at"].isoformat() if saved["created_at"] else now_iso()

    reply_info = None
    if reply_to:
        orig = await db_get_message_by_id(reply_to)
        if orig:
            reply_info = {
                "id": orig["id"],
                "nickname": orig.get("sender_display") or orig.get("sender_name"),
                "text": orig.get("text") or "📎",
            }

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

    await refresh_chats_for_chat(chat_id)


# ============================================================
#  ОТКРЫТИЕ ЧАТА
# ============================================================
async def handle_open_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        await send_json(websocket, {"type": "error", "text": "Нет доступа"})
        return

    history = await db_get_chat_history(chat_id)
    my_reactions = await db_get_my_reactions(chat_id, user["id"])

    await send_json(websocket, {
        "type": "chat_history",
        "chat_id": chat_id,
        "messages": history,
        "my_reactions": my_reactions,
    })

    if history:
        last_id = history[-1]["id"]
        await db_mark_chat_read(chat_id, user["id"], last_id)

        try:
            chats = await db_get_user_chats(user["id"])
            await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
        except Exception:
            pass


# ============================================================
#  TYPING
# ============================================================
async def handle_typing(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return
    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    await broadcast_chat(chat_id, {
        "type": "typing",
        "chat_id": chat_id,
        "nickname": user["display_name"],
        "user_id": user["id"],
    }, exclude=websocket)


# ============================================================
#  READ
# ============================================================
async def handle_read(websocket, user, msg):
    chat_id = msg.get("chat_id")
    message_id = msg.get("message_id")
    if not chat_id or not message_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    await db_mark_message_read(message_id, user["id"])
    await db_mark_chat_read(chat_id, user["id"], message_id)

    await broadcast_chat(chat_id, {
        "type": "message_read",
        "chat_id": chat_id,
        "message_id": message_id,
        "user_id": user["id"],
        "username": user["display_name"],
    }, exclude=websocket)


# ============================================================
#  РЕАКЦИЯ
# ============================================================
async def handle_react(websocket, user, msg):
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")
    emoji = msg.get("emoji")

    if not message_id or not chat_id or not emoji:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    orig = await db_get_message_by_id(message_id)
    if not orig or orig.get("chat_id") != chat_id:
        return

    reactions = await db_toggle_reaction(message_id, user["id"], emoji)

    await broadcast_chat(chat_id, {
        "type": "reactions_update",
        "message_id": message_id,
        "chat_id": chat_id,
        "reactions": reactions,
        "user_id": user["id"],
        "emoji": emoji,
    })


# ============================================================
#  EDIT
# ============================================================
async def handle_edit_message(websocket, user, msg):
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")
    new_text = str(msg.get("text", ""))[:MAX_MESSAGE_LENGTH]

    if not message_id or not chat_id or not new_text.strip():
        return

    ok = await db_edit_message(message_id, user["id"], new_text)
    if ok:
        await broadcast_chat(chat_id, {
            "type": "message_edited",
            "message_id": message_id,
            "chat_id": chat_id,
            "text": new_text,
        })


# ============================================================
#  DELETE
# ============================================================
async def handle_delete_message(websocket, user, msg):
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")

    if not message_id or not chat_id:
        return

    ok = await db_delete_message(message_id, user["id"])
    if not ok:
        ok = await db_delete_message_admin(message_id, chat_id, user["id"])

    if ok:
        await broadcast_chat(chat_id, {
            "type": "message_deleted",
            "message_id": message_id,
            "chat_id": chat_id,
        })


# ============================================================
#  FORWARD
# ============================================================
async def handle_forward_message(websocket, user, msg):
    message_id = msg.get("message_id")
    target_chat_id = msg.get("target_chat_id")

    if not message_id or not target_chat_id:
        return

    member = await db_check_member(target_chat_id, user["id"])
    if not member:
        await send_json(websocket, {"type": "error", "text": "Нет доступа"})
        return

    orig = await db_get_message_by_id(message_id)
    if not orig or orig.get("deleted"):
        return

    orig_chat_member = await db_check_member(orig["chat_id"], user["id"])
    if not orig_chat_member:
        return

    saved = await db_forward_message(
        message_id, target_chat_id, user["id"], user["display_name"]
    )
    if not saved:
        return

    orig_full = await db_get_message_by_id(message_id)
    if not orig_full:
        return

    created = saved["created_at"].isoformat() if saved["created_at"] else now_iso()

    await broadcast_chat(target_chat_id, {
        "type": "new_message",
        "chat_id": target_chat_id,
        "id": saved["id"],
        "sender_id": user["id"],
        "nickname": user["display_name"],
        "color": user["color"],
        "avatar_data": user.get("avatar_data"),
        "msg_type": orig_full["msg_type"],
        "text": orig_full.get("text"),
        "file_data": orig_full.get("file_data"),
        "file_name": orig_full.get("file_name"),
        "file_type": orig_full.get("file_type"),
        "duration": orig_full.get("duration"),
        "forwarded_from": orig_full.get("sender_name"),
        "reply_to": None,
        "time": now_str(),
        "created_at": created,
        "edited": False,
        "reactions": {},
    })

    await refresh_chats_for_chat(target_chat_id)


# ============================================================
#  СОЗДАНИЕ ЧАТОВ
# ============================================================
async def handle_create_private(websocket, user, msg):
    other_id = msg.get("user_id")
    if not other_id or other_id == user["id"]:
        return

    other_user = await db_get_user_by_id(other_id)
    if not other_user:
        await send_json(websocket, {"type": "error", "text": "Юзер не найден"})
        return

    chat = await db_get_or_create_private_chat(user["id"], other_id)
    if chat:
        await refresh_chats_list([user["id"], other_id])
        await send_json(websocket, {
            "type": "chat_opened",
            "chat_id": chat["id"],
        })


async def handle_create_group(websocket, user, msg):
    title = str(msg.get("title", "")).strip()[:MAX_GROUP_TITLE]
    description = str(msg.get("description", ""))[:MAX_GROUP_DESCRIPTION]
    member_ids = msg.get("member_ids", [])

    if not title:
        await send_json(websocket, {"type": "error", "text": "Введи название"})
        return
    if not isinstance(member_ids, list):
        member_ids = []
    if len(member_ids) > 200:
        member_ids = member_ids[:200]

    group = await db_create_group(user["id"], title, member_ids, description)
    if not group:
        await send_json(websocket, {"type": "error", "text": "Не удалось создать"})
        return

    chat_id = group["id"]
    all_members = [user["id"]] + [m for m in member_ids if m != user["id"]]
    await refresh_chats_list(all_members)

    await broadcast_chat(chat_id, {
        "type": "system",
        "chat_id": chat_id,
        "text": f"{user['display_name']} создал группу «{title}»",
        "time": now_str(),
    })

    await send_json(websocket, {"type": "chat_opened", "chat_id": chat_id})


async def handle_create_channel(websocket, user, msg):
    title = str(msg.get("title", "")).strip()[:MAX_GROUP_TITLE]
    description = str(msg.get("description", ""))[:MAX_GROUP_DESCRIPTION]
    is_public = bool(msg.get("is_public", False))

    if not title:
        await send_json(websocket, {"type": "error", "text": "Введи название"})
        return

    channel = await db_create_channel(user["id"], title, description, is_public)
    if not channel:
        await send_json(websocket, {"type": "error", "text": "Не удалось создать"})
        return

    chat_id = channel["id"]
    await refresh_chats_list([user["id"]])

    await broadcast_chat(chat_id, {
        "type": "system",
        "chat_id": chat_id,
        "text": f"Канал «{title}» создан",
        "time": now_str(),
    })

    await send_json(websocket, {"type": "chat_opened", "chat_id": chat_id})


# ============================================================
#  ИНФО О ЧАТЕ / УЧАСТНИКИ
# ============================================================
async def handle_get_chat_info(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        await send_json(websocket, {"type": "error", "text": "Нет доступа"})
        return

    info = await db_get_chat_info(chat_id)
    if not info:
        return

    members = await db_get_chat_members(chat_id)
    if info.get("created_at"):
        info["created_at"] = info["created_at"].isoformat()

    await send_json(websocket, {
        "type": "chat_info",
        "chat": dict(info),
        "members": [dict(m) for m in members],
        "my_role": member.get("role", "member"),
        "my_muted": bool(member.get("muted", False)),
    })


async def handle_add_members(websocket, user, msg):
    chat_id = msg.get("chat_id")
    user_ids = msg.get("user_ids", [])
    if not chat_id or not isinstance(user_ids, list) or not user_ids:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {"type": "error", "text": "Только админ"})
        return

    await db_add_members(chat_id, user_ids)

    current_members = await db_get_chat_member_ids(chat_id)
    all_affected = list(set(current_members + user_ids))
    await refresh_chats_list(all_affected)

    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })


async def handle_remove_member(websocket, user, msg):
    chat_id = msg.get("chat_id")
    target_id = msg.get("user_id")
    if not chat_id or not target_id:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin or target_id == user["id"]:
        return

    await db_remove_member(chat_id, target_id)

    await send_to_user(target_id, {"type": "removed_from_chat", "chat_id": chat_id})
    await refresh_chats_list([target_id, user["id"]])

    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })


async def handle_set_admin(websocket, user, msg):
    chat_id = msg.get("chat_id")
    target_id = msg.get("user_id")
    role = msg.get("role", "admin")
    if not chat_id or not target_id or role not in ("admin", "member"):
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        return

    await db_set_member_role(chat_id, target_id, role)

    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })

    await send_to_user(target_id, {
        "type": "role_changed",
        "chat_id": chat_id,
        "role": role,
    })


async def handle_update_group(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {"type": "error", "text": "Только админ"})
        return

    title = msg.get("title")
    if title is not None:
        title = str(title).strip()[:MAX_GROUP_TITLE] or None

    description = msg.get("description")
    if description is not None:
        description = str(description)[:MAX_GROUP_DESCRIPTION]

    await db_update_group(chat_id, title=title, description=description)

    info = await db_get_chat_info(chat_id)
    if info and info.get("created_at"):
        info["created_at"] = info["created_at"].isoformat()

    await broadcast_chat(chat_id, {
        "type": "chat_updated",
        "chat": dict(info) if info else {},
    })

    await refresh_chats_for_chat(chat_id)


async def handle_leave_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    ok = await db_leave_chat(chat_id, user["id"])
    if not ok:
        await send_json(websocket, {"type": "error", "text": "Нельзя выйти"})
        return

    members = await db_get_chat_members(chat_id)
    if members:
        await broadcast_chat(chat_id, {
            "type": "chat_members_updated",
            "chat_id": chat_id,
            "members": [dict(m) for m in members],
        })
        await broadcast_chat(chat_id, {
            "type": "system",
            "chat_id": chat_id,
            "text": f"{user['display_name']} покинул чат",
            "time": now_str(),
        })

    await refresh_chats_list([user["id"]])


async def handle_delete_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return
    ok = await db_delete_chat_for_user(chat_id, user["id"])
    if ok:
        await refresh_chats_list([user["id"]])


# ============================================================
#  ПИН / АРХИВ / МЬЮТ
# ============================================================
async def handle_pin_chat(websocket, user, msg, pin: bool):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return
    if pin:
        await db_pin_chat(user["id"], chat_id)
    else:
        await db_unpin_chat(user["id"], chat_id)
    await refresh_chats_list([user["id"]])


async def handle_archive_chat(websocket, user, msg, archive: bool):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return
    if archive:
        await db_archive_chat(user["id"], chat_id)
    else:
        await db_unarchive_chat(user["id"], chat_id)
    await refresh_chats_list([user["id"]])


async def handle_mute_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    muted = bool(msg.get("muted", True))
    if not chat_id:
        return
    await db_mute_chat(chat_id, user["id"], muted)
    await send_to_user(user["id"], {
        "type": "mute_changed",
        "chat_id": chat_id,
        "muted": muted,
    })
    await refresh_chats_list([user["id"]])


# ============================================================
#  ПОИСК
# ============================================================
async def handle_search_messages(websocket, user, msg):
    query = str(msg.get("query", "")).strip()
    chat_id = msg.get("chat_id")
    limit = min(int(msg.get("limit", 50)), 200)
    if not query:
        return

    results = await db_search_messages(user["id"], query, chat_id, limit)

    await send_json(websocket, {
        "type": "search_results",
        "query": query,
        "chat_id": chat_id,
        "results": results,
    })


async def handle_search_users(websocket, user, msg):
    query = str(msg.get("query", "")).strip()
    if not query:
        return
    results = await db_search_users(query, exclude_id=user["id"])
    await send_json(websocket, {
        "type": "user_search_results",
        "query": query,
        "users": [user_public(u) for u in results],
    })


# ============================================================
#  ПРОФИЛЬ
# ============================================================
async def handle_update_profile(websocket, user, msg):
    display_name = msg.get("display_name")
    bio = msg.get("bio")
    avatar_data = msg.get("avatar_data")
    language = msg.get("language")
    theme = msg.get("theme")

    if display_name is not None:
        display_name = str(display_name).strip()[:MAX_DISPLAY_NAME] or None

    if bio is not None:
        bio = str(bio)[:MAX_BIO_LENGTH]

    if language is not None and language not in ("ru", "en"):
        language = None

    if theme is not None and theme not in ("dark", "light"):
        theme = None

    if avatar_data is not None:
        size = get_file_size_base64(avatar_data)
        if size > MAX_AVATAR_SIZE:
            await send_json(websocket, {"type": "error", "text": "Аватар большой"})
            return

    await db_update_user(
        user_id=user["id"],
        display_name=display_name,
        bio=bio,
        avatar_data=avatar_data,
        language=language,
        theme=theme,
    )

    if display_name:
        user["display_name"] = display_name
    if bio is not None:
        user["bio"] = bio
    if avatar_data:
        user["avatar_data"] = avatar_data
    if language:
        user["language"] = language
    if theme:
        user["theme"] = theme

    clients[websocket] = user

    updated = await db_get_user_by_id(user["id"])
    if not updated:
        return

    pub = user_public(updated)

    await send_to_user(user["id"], {
        "type": "profile_updated",
        "user": pub,
    })

    # Уведомить всех в общих чатах
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT cm2.user_id FROM chat_members cm1
                    JOIN chat_members cm2 ON cm2.chat_id = cm1.chat_id
                    WHERE cm1.user_id = %s AND cm2.user_id != %s
                """, (user["id"], user["id"]))
                member_ids = [r["user_id"] for r in cur.fetchall()]
        if member_ids:
            await send_to_users(member_ids, {"type": "user_updated", "user": pub})
    except Exception:
        pass


async def handle_change_username(websocket, user, msg):
    new_username = str(msg.get("username", "")).strip().lower()[:MAX_USERNAME]
    if not new_username:
        await send_json(websocket, {"type": "error", "text": "Введите username"})
        return
    if not validate_username(new_username):
        await send_json(websocket, {"type": "error", "text": "Неверный формат"})
        return
    if new_username == user["name"]:
        await send_json(websocket, {"type": "error", "text": "Это твой текущий"})
        return

    ok = await db_change_username(user["id"], new_username)
    if not ok:
        await send_json(websocket, {"type": "error", "text": "Занят"})
        return

    user["name"] = new_username
    clients[websocket] = user

    updated = await db_get_user_by_id(user["id"])
    pub = user_public(updated) if updated else user_public(user)

    await send_to_user(user["id"], {
        "type": "username_changed",
        "new_username": new_username,
        "user": pub,
    })

    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT cm2.user_id FROM chat_members cm1
                    JOIN chat_members cm2 ON cm2.chat_id = cm1.chat_id
                    WHERE cm1.user_id = %s AND cm2.user_id != %s
                """, (user["id"], user["id"]))
                member_ids = [r["user_id"] for r in cur.fetchall()]
        if member_ids:
            await send_to_users(member_ids, {"type": "user_updated", "user": pub})
    except Exception:
        pass


# ============================================================
#  ЗВОНКИ
# ============================================================
async def handle_call_signal(websocket, user, msg):
    target_id = msg.get("target_id")
    if not target_id or target_id == user["id"]:
        return

    target = await db_get_user_by_id(target_id)
    if not target:
        await send_json(websocket, {"type": "call_reject", "reason": "user_not_found"})
        return

    enriched = {
        **msg,
        "from_id": user["id"],
        "from_name": user["display_name"],
        "from_color": user["color"],
        "from_avatar": user.get("avatar_data"),
    }

    delivered = False
    if target_id in user_sockets and user_sockets[target_id]:
        await send_to_user(target_id, enriched)
        delivered = True

    if not delivered and msg.get("type") == "call_offer":
        await send_json(websocket, {"type": "call_offline", "target_id": target_id})


# ============================================================
#  PUSH
# ============================================================
async def handle_push_subscribe(websocket, user, msg):
    sub = msg.get("subscription", {})
    endpoint = sub.get("endpoint")
    keys = sub.get("keys", {})
    if not endpoint:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO push_subscriptions (user_id, endpoint, p256dh, auth)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (endpoint) DO UPDATE
                    SET user_id=%s, p256dh=%s, auth=%s
                """, (user["id"], endpoint, keys.get("p256dh",""), keys.get("auth",""),
                      user["id"], keys.get("p256dh",""), keys.get("auth","")))
            conn.commit()

    await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 8/9 ===
# ============================================================
#  MAIN
# ============================================================
async def main():
    print("=" * 60)
    print("🔥 BLAZE MESSENGER v3.0")
    print("=" * 60)

    await init_db()

    print(f"📦 DATABASE_URL: {'✅' if DATABASE_URL else '❌ не задан'}")
    print(f"🔑 JWT_SECRET: {'✅' if JWT_SECRET and JWT_SECRET != 'change-me-in-production' else '⚠️ по умолчанию'}")
    print(f"🔐 Шифрование БД: {'✅' if cipher else '⚠️ выключено'}")

    port = int(os.environ.get("PORT", 8765))
    print(f"🌐 Порт: {port}")
    print("-" * 60)
    print(f"🚀 Запуск сервера...")
    print(f"📦 Модули:")
    print(f"   • Чаты (личные, группы, каналы)")
    print(f"   • Сообщения (текст, файлы, голосовые, огоньки)")
    print(f"   • Реакции, reply, edit, delete, forward")
    print(f"   • Профили, @username, аватары")
    print(f"   • Поиск (чаты, юзеры, сообщения)")
    print(f"   • Push-подписки")
    print(f"   • Звонки (WebRTC сигналинг)")
    print(f"   • Защита: bcrypt, JWT, шифрование БД")
    print("=" * 60)

    try:
        async with websockets.serve(
            handler,
            "0.0.0.0",
            port,
            process_request=process_request,
            max_size=20 * 1024 * 1024,
            ping_interval=25,
            ping_timeout=25,
            close_timeout=10,
            max_queue=32,
        ):
            await asyncio.Future()
    except asyncio.CancelledError:
        print("⚠️ Сервер остановлен")
    finally:
        print("👋 Сервер завершил работу")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Остановлено пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        raise


# ════════════════════════════════════════════════════════════
#  КОНЕЦ ФАЙЛА server.py
#  Blaze Messenger v3.0 — без email, чистый
# ════════════════════════════════════════════════════════════