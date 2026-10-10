# ============================================================
#  BLAZE MESSENGER v3.0 — SERVER (МЕГА-ВЕРСИЯ)
#  Полный сервер: чаты, группы, каналы, голосовые, кружки-огоньки,
#  реакции, reply, edit, delete, forward, search, профили,
#  2 языка, push, звонки, шифрование БД, @username,
#  автосохранение входа, аватары
# ============================================================

import asyncio
import websockets
import json
import datetime
import os
import http
import re
import secrets
import hashlib
import smtplib
import ssl
import base64
import mimetypes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
    print("⚠️ cryptography не установлена — шифрование выключено")


# ============================================================
#  КОНФИГ
# ============================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGO = "HS256"
JWT_EXP_DAYS = 30

# Лимиты
MAX_HISTORY = 200
MAX_FILE_SIZE = 10 * 1024 * 1024      # 10 МБ
MAX_VOICE_SIZE = 3 * 1024 * 1024      # 3 МБ
MAX_AVATAR_SIZE = 2 * 1024 * 1024     # 2 МБ
MAX_MESSAGE_LENGTH = 4000
MAX_BIO_LENGTH = 200
MAX_DISPLAY_NAME = 40
MAX_USERNAME = 20
MAX_GROUP_TITLE = 80
MAX_GROUP_DESCRIPTION = 500

# Email SMTP
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER)
SMTP_FROM_NAME = os.environ.get("SMTP_FROM_NAME", "Blaze Messenger")
EMAILS_ENABLED = bool(SMTP_USER and SMTP_PASS)

# Шифрование БД
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "").encode()
cipher = None
if ENCRYPTION_KEY and CRYPTO_AVAILABLE:
    try:
        cipher = Fernet(ENCRYPTION_KEY)
    except Exception as e:
        print(f"⚠️ Ошибка ключа шифрования: {e}")
        cipher = None

if cipher:
    print("🔐 Шифрование БД: ВКЛЮЧЕНО")
else:
    print("⚠️ Шифрование БД: ВЫКЛЮЧЕНО")


# ============================================================
#  ФУНКЦИИ ШИФРОВАНИЯ
# ============================================================
def encrypt_text(text):
    """Зашифровать текст"""
    if not cipher or not text:
        return text
    try:
        return cipher.encrypt(text.encode()).decode()
    except Exception as e:
        print(f"Encrypt error: {e}")
        return text


def decrypt_text(text):
    """Расшифровать текст"""
    if not cipher or not text:
        return text
    try:
        return cipher.decrypt(text.encode()).decode()
    except (InvalidToken, Exception):
        return text


# ============================================================
#  УТИЛИТЫ
# ============================================================
def now_str():
    return datetime.datetime.now().strftime("%H:%M")


def now_iso():
    return datetime.datetime.now().isoformat()


def generate_code(length: int = 6) -> str:
    """Генерация цифрового кода"""
    return "".join([str(secrets.randbelow(10)) for _ in range(length)])


def validate_username(username: str) -> bool:
    """Только латиница, цифры, подчёркивание, от 3 до 20"""
    if not username:
        return False
    if len(username) < 3 or len(username) > MAX_USERNAME:
        return False
    return bool(re.match(r"^[a-zA-Z0-9_]+$", username))


def validate_display_name(name: str) -> bool:
    """Имя от 1 до 40 символов"""
    if not name:
        return False
    if len(name) < 1 or len(name) > MAX_DISPLAY_NAME:
        return False
    return True


def mask_email(email: str) -> str:
    """user@example.com → u***r@example.com"""
    if not email or "@" not in email:
        return email
    name, domain = email.split("@", 1)
    if len(name) <= 2:
        masked = name[0] + "*"
    else:
        masked = name[0] + "*" * (len(name) - 2) + name[-1]
    return f"{masked}@{domain}"


def get_file_size_base64(b64_str: str) -> int:
    """Размер base64-строки в байтах"""
    if not b64_str:
        return 0
    try:
        padding = b64_str.count("=")
        return (len(b64_str) * 3) // 4 - padding
    except Exception:
        return len(b64_str)


# ============================================================
#  БАЗА ДАННЫХ — ПОДКЛЮЧЕНИЕ
# ============================================================
def db_conn():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# === КОНЕЦ ЧАСТИ 1/25 ===
# ============================================================
#  ИНИЦИАЛИЗАЦИЯ БД
# ============================================================
async def init_db():
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL не задан — работаю без истории")
        return

    def _init():
        with db_conn() as conn:
            with conn.cursor() as cur:

                # ===== ПОЛЬЗОВАТЕЛИ =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        username TEXT UNIQUE NOT NULL,
                        password_hash TEXT NOT NULL,
                        display_name TEXT,
                        bio TEXT DEFAULT '',
                        avatar_data TEXT,
                        avatar_color TEXT DEFAULT '#ff6b00',
                        email TEXT,
                        email_verified BOOLEAN DEFAULT FALSE,
                        language TEXT DEFAULT 'ru',
                        theme TEXT DEFAULT 'dark',
                        is_online BOOLEAN DEFAULT FALSE,
                        last_seen TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # ===== ЧАТЫ (private / group / channel) =====
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

                # ===== УЧАСТНИКИ ЧАТОВ =====
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

                # ===== СООБЩЕНИЯ =====
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

                # ===== РЕАКЦИИ =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reactions (
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        emoji TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (message_id, user_id)
                    );
                """)

                # ===== PUSH-ПОДПИСКИ =====
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

                # ===== КОДЫ ВЕРИФИКАЦИИ (email) =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS verification_codes (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        email TEXT NOT NULL,
                        code TEXT NOT NULL,
                        purpose TEXT DEFAULT 'login',
                        expires_at TIMESTAMP NOT NULL,
                        used BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)

                # ===== ПРОЧТЕНИЯ =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS message_reads (
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        read_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (message_id, user_id)
                    );
                """)

                # ===== ЗАКРЕПЛЁННЫЕ ЧАТЫ =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pinned_chats (
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        pinned_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (user_id, chat_id)
                    );
                """)

                # ===== ЗАКРЕПЛЁННЫЕ СООБЩЕНИЯ =====
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pinned_messages (
                        chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
                        message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
                        pinned_by INTEGER REFERENCES users(id),
                        pinned_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (chat_id, message_id)
                    );
                """)

                # ===== АРХИВ =====
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
                cur.execute("CREATE INDEX IF NOT EXISTS idx_verification_user ON verification_codes(user_id);")

                # ===== ОБЩИЙ ЧАТ =====
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO chats (type, title, description) "
                        "VALUES ('global', 'Общий чат', 'Все пользователи Blaze')"
                    )

            conn.commit()

    try:
        await asyncio.to_thread(_init)
        print("✅ База данных инициализирована")
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        import traceback
        traceback.print_exc()


# === КОНЕЦ ЧАСТИ 2/25 ===
# ============================================================
#  USERS — СОЗДАНИЕ, ПОЛУЧЕНИЕ, ПОИСК
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
                        "avatar_data, language, theme, email, email_verified",
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
                    "bio, avatar_data, language, theme, email, email_verified, "
                    "last_seen FROM users WHERE username=%s",
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
                    "avatar_data, language, theme, email, email_verified, "
                    "last_seen FROM users WHERE id=%s",
                    (user_id,)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_user_by_email(email):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, display_name, avatar_color, bio, "
                    "avatar_data, language, theme, email, email_verified "
                    "FROM users WHERE email=%s LIMIT 1",
                    (email,)
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
                        "avatar_data, last_seen FROM users "
                        "WHERE id != %s ORDER BY display_name NULLS LAST, username",
                        (exclude_id,)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, last_seen FROM users "
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
                        "avatar_data, last_seen FROM users "
                        "WHERE (username ILIKE %s OR display_name ILIKE %s OR email ILIKE %s) "
                        "AND id != %s ORDER BY display_name LIMIT 50",
                        (q, q, q, exclude_id)
                    )
                else:
                    cur.execute(
                        "SELECT id, username, display_name, avatar_color, bio, "
                        "avatar_data, last_seen FROM users "
                        "WHERE username ILIKE %s OR display_name ILIKE %s OR email ILIKE %s "
                        "ORDER BY display_name LIMIT 50",
                        (q, q, q)
                    )
                return cur.fetchall()

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 3/25 ===
# ============================================================
#  USERS — ОБНОВЛЕНИЕ ПРОФИЛЯ
# ============================================================
async def db_update_user(user_id, display_name=None, bio=None,
                         avatar_data=None, language=None, theme=None,
                         email=None):
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

                if email is not None:
                    updates.append("email=%s")
                    params.append(email)

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
    """Меняет @username. Возвращает True при успехе, False если занят."""
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


async def db_update_avatar(user_id, avatar_data):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET avatar_data=%s WHERE id=%s",
                    (avatar_data, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_display_name(user_id, display_name):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET display_name=%s WHERE id=%s",
                    (display_name, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_bio(user_id, bio):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET bio=%s WHERE id=%s",
                    (bio[:MAX_BIO_LENGTH], user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_language(user_id, language):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET language=%s WHERE id=%s",
                    (language, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_theme(user_id, theme):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET theme=%s WHERE id=%s",
                    (theme, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_set_user_email(user_id, email):
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET email=%s, email_verified=TRUE WHERE id=%s",
                    (email, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_user_email(user_id):
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT email FROM users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                return row["email"] if row else None

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 4/25 ===
# ============================================================
#  EMAIL-КОДЫ ВЕРИФИКАЦИИ
# ============================================================
async def db_save_code(user_id, email, code, purpose="login", ttl_minutes=10):
    """Сохраняет код. Удаляет старые для того же purpose."""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Удаляем старые коды того же назначения
                cur.execute(
                    "DELETE FROM verification_codes WHERE user_id=%s AND purpose=%s",
                    (user_id, purpose)
                )
                # Удаляем истёкшие коды пользователя
                cur.execute(
                    "DELETE FROM verification_codes WHERE user_id=%s AND expires_at < NOW()",
                    (user_id,)
                )
                # Сохраняем новый
                expires = datetime.datetime.now() + datetime.timedelta(minutes=ttl_minutes)
                cur.execute("""
                    INSERT INTO verification_codes
                    (user_id, email, code, purpose, expires_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, email, code, purpose, expires))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_check_code(user_id, code, purpose="login"):
    """Проверяет код. Возвращает True если верный и не использован."""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, expires_at, used FROM verification_codes
                    WHERE user_id=%s AND code=%s AND purpose=%s
                    ORDER BY id DESC LIMIT 1
                """, (user_id, code, purpose))
                row = cur.fetchone()
                if not row:
                    return False
                if row["used"]:
                    return False
                if row["expires_at"] < datetime.datetime.now():
                    return False
                # Помечаем использованным
                cur.execute(
                    "UPDATE verification_codes SET used=TRUE WHERE id=%s",
                    (row["id"],)
                )
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_get_active_code(user_id, purpose="login"):
    """Возвращает активный код (для отладки/переотправки)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT code, expires_at FROM verification_codes
                    WHERE user_id=%s AND purpose=%s AND used=FALSE
                    AND expires_at > NOW()
                    ORDER BY id DESC LIMIT 1
                """, (user_id, purpose))
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_delete_old_codes():
    """Удаляет старые истёкшие коды (вызывается периодически)"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM verification_codes "
                    "WHERE expires_at < NOW() - INTERVAL '1 day'"
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_count_user_codes(user_id, purpose="login"):
    """Сколько кодов создано за последний час (антиспам)"""
    if not DATABASE_URL:
        return 0

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as cnt FROM verification_codes
                    WHERE user_id=%s AND purpose=%s
                    AND created_at > NOW() - INTERVAL '1 hour'
                """, (user_id, purpose))
                row = cur.fetchone()
                return row["cnt"] if row else 0

    return await asyncio.to_thread(_q)


# ============================================================
#  ВСПОМОГАТЕЛЬНОЕ — генерация и валидация
# ============================================================
def generate_verification_code() -> str:
    """6-значный код"""
    return f"{secrets.randbelow(1000000):06d}"


def is_code_valid_format(code: str) -> bool:
    """Проверяет что код — 6 цифр"""
    if not code:
        return False
    if len(code) != 6:
        return False
    return code.isdigit()


# === КОНЕЦ ЧАСТИ 5/25 ===
# ============================================================
#  EMAIL — ОТПРАВКА ПИСЕМ
# ============================================================
def send_email_sync(to_email: str, subject: str, html_body: str) -> bool:
    """Синхронная отправка email (вызывается через to_thread)"""
    if not EMAILS_ENABLED:
        print(f"⚠️ Email не настроен. Письмо для {to_email} не отправлено.")
        return False

    if not to_email or "@" not in to_email:
        print(f"⚠️ Некорректный email: {to_email}")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM}>"
        msg["To"] = to_email
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, to_email, msg.as_string())

        print(f"📧 Письмо отправлено: {to_email} — «{subject}»")
        return True

    except smtplib.SMTPAuthenticationError as e:
        print(f"❌ SMTP auth error: {e}")
        return False
    except smtplib.SMTPException as e:
        print(f"❌ SMTP error: {e}")
        return False
    except Exception as e:
        print(f"❌ Ошибка email: {e}")
        return False


async def send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Асинхронная отправка email"""
    return await asyncio.to_thread(send_email_sync, to_email, subject, html_body)


# ============================================================
#  EMAIL — HTML ШАБЛОНЫ
# ============================================================
def _email_wrapper(title: str, content_html: str) -> str:
    """Обёртка для всех писем — единый стиль Blaze"""
    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
    </head>
    <body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
        <table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0a;padding:40px 20px;">
            <tr>
                <td align="center">
                    <table width="100%" cellpadding="0" cellspacing="0" style="max-width:520px;background:#121212;border-radius:20px;overflow:hidden;border:1px solid #26262e;">
                        <!-- HEADER -->
                        <tr>
                            <td style="padding:32px 32px 16px;text-align:center;background:linear-gradient(135deg,rgba(255,107,0,0.1),rgba(229,57,53,0.1));">
                                <div style="font-size:64px;line-height:1;filter:drop-shadow(0 0 20px rgba(255,107,0,0.6));">🔥</div>
                                <h1 style="margin:12px 0 0;font-size:28px;font-weight:800;letter-spacing:-1px;background:linear-gradient(135deg,#ff6b00,#e53935);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">Blaze Messenger</h1>
                            </td>
                        </tr>
                        <!-- CONTENT -->
                        <tr>
                            <td style="padding:24px 32px;">
                                {content_html}
                            </td>
                        </tr>
                        <!-- FOOTER -->
                        <tr>
                            <td style="padding:20px 32px 32px;text-align:center;border-top:1px solid #26262e;">
                                <p style="margin:0;color:#666670;font-size:12px;line-height:1.6;">
                                    Это автоматическое сообщение от Blaze Messenger.<br>
                                    Если это не ты — просто проигнорируй письмо.
                                </p>
                                <p style="margin:12px 0 0;color:#666670;font-size:11px;">
                                    © 2026 Blaze Messenger
                                </p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """


def make_code_email_html(code: str, username: str = "", purpose: str = "login") -> str:
    """Письмо с кодом подтверждения"""
    titles = {
        "login": "Код входа",
        "register": "Подтверждение регистрации",
        "change_email": "Подтверждение email",
        "reset_password": "Сброс пароля",
    }
    title = titles.get(purpose, "Код подтверждения")
    greeting = f"Привет, {username}!" if username else "Привет!"

    content = f"""
        <h2 style="margin:0 0 12px;font-size:20px;color:#ffffff;font-weight:700;text-align:center;">{title}</h2>
        <p style="margin:0 0 20px;color:#a0a0aa;font-size:14px;text-align:center;line-height:1.5;">
            {greeting} Твой код подтверждения:
        </p>
        <div style="background:#1a1a1a;border:2px solid #ff6b00;border-radius:16px;padding:28px 20px;text-align:center;margin:24px 0;box-shadow:0 0 40px rgba(255,107,0,0.25);">
            <div style="font-size:44px;font-weight:800;letter-spacing:14px;color:#ff6b00;font-family:'Courier New',monospace;text-shadow:0 0 20px rgba(255,107,0,0.6);">{code}</div>
        </div>
        <p style="margin:0;color:#666670;font-size:13px;text-align:center;line-height:1.5;">
            Код действителен <b style="color:#ff8c1a;">10 минут</b>.<br>
            Никому не сообщай этот код.
        </p>
    """

    return _email_wrapper(title, content)


def make_welcome_email_html(username: str) -> str:
    """Приветственное письмо после регистрации"""
    content = f"""
        <h2 style="margin:0 0 16px;font-size:22px;color:#ffffff;font-weight:700;">Добро пожаловать, {username}! 🔥</h2>
        <p style="margin:0 0 16px;color:#a0a0aa;font-size:15px;line-height:1.6;">
            Ты успешно зарегистрировался в <b style="color:#ff6b00;">Blaze Messenger</b>.
        </p>
        <p style="margin:0 0 16px;color:#a0a0aa;font-size:15px;line-height:1.6;">
            Теперь ты можешь:
        </p>
        <ul style="margin:0 0 20px;padding-left:20px;color:#a0a0aa;font-size:15px;line-height:1.8;">
            <li>💬 Общаться в личных чатах и группах</li>
            <li>🎤 Отправлять голосовые и видео-огоньки 🔥</li>
            <li>❤️ Ставить реакции и делать reply</li>
            <li>📞 Звонить по видео и аудио</li>
            <li>📎 Делиться файлами и медиа</li>
        </ul>
        <p style="margin:0;color:#666670;font-size:13px;text-align:center;">
            Если у тебя есть вопросы — просто напиши в общий чат!
        </p>
    """
    return _email_wrapper("Добро пожаловать в Blaze", content)


def make_password_reset_email_html(code: str, username: str = "") -> str:
    """Письмо для сброса пароля"""
    content = f"""
        <h2 style="margin:0 0 12px;font-size:20px;color:#ffffff;font-weight:700;text-align:center;">Сброс пароля</h2>
        <p style="margin:0 0 20px;color:#a0a0aa;font-size:14px;text-align:center;line-height:1.5;">
            {f'Привет, {username}!' if username else 'Привет!'} Ты запросил сброс пароля.
        </p>
        <div style="background:#1a1a1a;border:2px solid #e53935;border-radius:16px;padding:28px 20px;text-align:center;margin:24px 0;box-shadow:0 0 40px rgba(229,57,53,0.25);">
            <div style="font-size:44px;font-weight:800;letter-spacing:14px;color:#e53935;font-family:'Courier New',monospace;">{code}</div>
        </div>
        <p style="margin:0;color:#666670;font-size:13px;text-align:center;line-height:1.5;">
            Код действителен <b style="color:#e53935;">10 минут</b>.<br>
            Если ты не запрашивал сброс — проигнорируй письмо.
        </p>
    """
    return _email_wrapper("Сброс пароля — Blaze", content)


def make_test_email_html(username: str = "друг") -> str:
    """Тестовое письмо для проверки SMTP"""
    content = f"""
        <h2 style="margin:0 0 16px;font-size:22px;color:#ffffff;font-weight:700;">Тестовое письмо ✅</h2>
        <p style="margin:0 0 16px;color:#a0a0aa;font-size:15px;line-height:1.6;">
            Привет, {username}!
        </p>
        <p style="margin:0 0 16px;color:#a0a0aa;font-size:15px;line-height:1.6;">
            Если ты видишь это письмо — значит SMTP настроен правильно,
            и Blaze Messenger может отправлять email-уведомления.
        </p>
        <p style="margin:0;color:#666670;font-size:13px;text-align:center;">
            Это тестовое письмо, никаких действий не требуется.
        </p>
    """
    return _email_wrapper("Тест SMTP — Blaze", content)


# === КОНЕЦ ЧАСТИ 6/25 ===
# ============================================================
#  CHATS — ЛИЧНЫЕ И ОБЩИЕ
# ============================================================
async def db_get_global_chat():
    """Возвращает общий чат (для новых юзеров)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM chats WHERE type='global' LIMIT 1")
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_add_user_to_global_chat(user_id):
    """Добавляет пользователя в общий чат"""
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
    """Возвращает существующий личный чат или создаёт новый"""
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

                cur.execute(
                    "INSERT INTO chats (type) VALUES ('private') RETURNING id"
                )
                chat_id = cur.fetchone()["id"]

                cur.execute(
                    "INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)",
                    (chat_id, a)
                )
                cur.execute(
                    "INSERT INTO chat_members (chat_id, user_id) VALUES (%s, %s)",
                    (chat_id, b)
                )
            conn.commit()
            return {"id": chat_id}

    return await asyncio.to_thread(_q)


async def db_get_chat_info(chat_id):
    """Полная информация о чате"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, type, title, description, avatar_data,
                           avatar_color, is_public, invite_code,
                           created_by, created_at
                    FROM chats WHERE id=%s
                """, (chat_id,))
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_get_chat_members(chat_id):
    """Список участников чата с ролями"""
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
    """Просто список ID участников"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id FROM chat_members WHERE chat_id=%s",
                    (chat_id,)
                )
                return [r["user_id"] for r in cur.fetchall()]

    return await asyncio.to_thread(_q)


async def db_check_member(chat_id, user_id):
    """Проверяет, состоит ли юзер в чате, и возвращает его роль"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT role, muted FROM chat_members "
                    "WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                return cur.fetchone()

    return await asyncio.to_thread(_q)


async def db_is_admin(chat_id, user_id):
    """Проверка что юзер — админ чата"""
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


async def db_mark_chat_read(chat_id, user_id, last_message_id):
    """Отмечает чат прочитанным до указанного сообщения"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE chat_members
                    SET last_read_id=%s
                    WHERE chat_id=%s AND user_id=%s
                    AND (last_read_id IS NULL OR last_read_id < %s)
                """, (last_message_id, chat_id, user_id, last_message_id))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_chat_unread_count(chat_id, user_id):
    """Число непрочитанных в чате"""
    if not DATABASE_URL:
        return 0

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as cnt FROM messages m
                    JOIN chat_members cm ON cm.chat_id = m.chat_id AND cm.user_id = %s
                    WHERE m.chat_id = %s
                    AND m.id > COALESCE(cm.last_read_id, 0)
                    AND m.sender_id != %s
                    AND NOT m.deleted
                """, (user_id, chat_id, user_id))
                row = cur.fetchone()
                return row["cnt"] if row else 0

    return await asyncio.to_thread(_q)


async def db_get_unread_total(user_id):
    """Общее число непрочитанных по всем чатам"""
    if not DATABASE_URL:
        return 0

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as cnt FROM messages m
                    JOIN chat_members cm ON cm.chat_id = m.chat_id AND cm.user_id = %s
                    WHERE m.id > COALESCE(cm.last_read_id, 0)
                    AND m.sender_id != %s
                    AND NOT m.deleted
                """, (user_id, user_id))
                row = cur.fetchone()
                return row["cnt"] if row else 0

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 7/25 ===
# ============================================================
#  ГРУППЫ — СОЗДАНИЕ И УПРАВЛЕНИЕ
# ============================================================
async def db_create_group(creator_id, title, member_ids=None, description=""):
    """Создаёт группу, создатель — админ"""
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

                # Создатель — админ
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'admin')
                """, (chat_id, creator_id))

                # Остальные — участники
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


async def db_add_members(chat_id, user_ids):
    """Добавляет участников в группу"""
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
    """Удаляет участника из группы"""
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
    """Меняет роль участника (admin/member)"""
    if not DATABASE_URL:
        return

    if role not in ("admin", "member"):
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE chat_members SET role=%s "
                    "WHERE chat_id=%s AND user_id=%s",
                    (role, chat_id, user_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_update_group(chat_id, title=None, description=None,
                          avatar_data=None, avatar_color=None):
    """Обновляет данные группы"""
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
                cur.execute(
                    f"UPDATE chats SET {', '.join(updates)} WHERE id=%s",
                    params
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_leave_chat(chat_id, user_id):
    """Выход из группы. Возвращает True при успехе."""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT type FROM chats WHERE id=%s", (chat_id,))
                row = cur.fetchone()
                if not row:
                    return False
                if row["type"] != "group":
                    return False

                cur.execute(
                    "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )

                # Если группа пустая — удаляем чат
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM chat_members WHERE chat_id=%s",
                    (chat_id,)
                )
                cnt_row = cur.fetchone()
                if cnt_row and cnt_row["cnt"] == 0:
                    cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))

            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_delete_group(chat_id, user_id):
    """Удаление группы (только админ)"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверка роли
                cur.execute(
                    "SELECT role FROM chat_members "
                    "WHERE chat_id=%s AND user_id=%s AND role='admin'",
                    (chat_id, user_id)
                )
                if not cur.fetchone():
                    return False

                cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_generate_invite_code(chat_id):
    """Генерирует инвайт-код для группы"""
    if not DATABASE_URL:
        return None

    code = secrets.token_urlsafe(12)

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE chats SET invite_code=%s, is_public=TRUE "
                    "WHERE id=%s RETURNING invite_code",
                    (code, chat_id)
                )
                row = cur.fetchone()
            conn.commit()
            return row["invite_code"] if row else None

    return await asyncio.to_thread(_q)


async def db_join_by_invite_code(code, user_id):
    """Присоединиться к группе по инвайт-коду"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, type FROM chats WHERE invite_code=%s",
                    (code,)
                )
                row = cur.fetchone()
                if not row:
                    return None

                chat_id = row["id"]
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'member')
                    ON CONFLICT DO NOTHING
                """, (chat_id, user_id))
            conn.commit()
            return {"id": chat_id, "type": row["type"]}

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 8/25 ===
# ============================================================
#  КАНАЛЫ — СОЗДАНИЕ И УПРАВЛЕНИЕ
# ============================================================
async def db_create_channel(creator_id, title, description="",
                             is_public=False, avatar_data=None):
    """Создаёт канал. Создатель — админ/владелец."""
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
                    INSERT INTO chats
                    (type, title, description, is_public, invite_code,
                     avatar_data, created_by)
                    VALUES ('channel', %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    title,
                    (description or "")[:MAX_GROUP_DESCRIPTION],
                    is_public,
                    invite_code,
                    avatar_data,
                    creator_id,
                ))
                chat_id = cur.fetchone()["id"]

                # Создатель — админ канала
                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'admin')
                """, (chat_id, creator_id))
            conn.commit()
            return {"id": chat_id, "invite_code": invite_code}

    return await asyncio.to_thread(_q)


async def db_update_channel(chat_id, user_id, title=None, description=None,
                             is_public=None, avatar_data=None):
    """Обновление канала (только админ)"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверка что юзер — админ канала
                cur.execute("""
                    SELECT role FROM chat_members
                    WHERE chat_id=%s AND user_id=%s AND role='admin'
                """, (chat_id, user_id))
                if not cur.fetchone():
                    return False

                updates = []
                params = []

                if title is not None:
                    updates.append("title=%s")
                    params.append(title[:MAX_GROUP_TITLE])

                if description is not None:
                    updates.append("description=%s")
                    params.append(description[:MAX_GROUP_DESCRIPTION])

                if is_public is not None:
                    updates.append("is_public=%s")
                    params.append(is_public)
                    # Генерим invite_code если стал публичным и его нет
                    if is_public:
                        cur.execute(
                            "SELECT invite_code FROM chats WHERE id=%s",
                            (chat_id,)
                        )
                        row = cur.fetchone()
                        if row and not row["invite_code"]:
                            updates.append("invite_code=%s")
                            params.append(secrets.token_urlsafe(12))

                if avatar_data is not None:
                    updates.append("avatar_data=%s")
                    params.append(avatar_data)

                if not updates:
                    return False

                params.append(chat_id)
                cur.execute(
                    f"UPDATE chats SET {', '.join(updates)} WHERE id=%s",
                    params
                )
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_subscribe_to_channel(chat_id, user_id):
    """Подписаться на канал"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверяем что это канал
                cur.execute(
                    "SELECT type FROM chats WHERE id=%s",
                    (chat_id,)
                )
                row = cur.fetchone()
                if not row or row["type"] != "channel":
                    return False

                cur.execute("""
                    INSERT INTO chat_members (chat_id, user_id, role)
                    VALUES (%s, %s, 'member')
                    ON CONFLICT DO NOTHING
                """, (chat_id, user_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unsubscribe_from_channel(chat_id, user_id):
    """Отписаться от канала"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Нельзя отписаться если ты владелец
                cur.execute("""
                    SELECT role FROM chat_members
                    WHERE chat_id=%s AND user_id=%s
                """, (chat_id, user_id))
                row = cur.fetchone()
                if not row:
                    return False
                if row["role"] == "admin":
                    return False

                cur.execute(
                    "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_can_post_to_chat(chat_id, user_id):
    """
    Может ли юзер отправлять сообщения в чат?
    - В private/group — да, если участник
    - В channel — только админ
    """
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.type, m.role FROM chats c
                    LEFT JOIN chat_members m
                        ON m.chat_id = c.id AND m.user_id = %s
                    WHERE c.id = %s
                """, (user_id, chat_id))
                row = cur.fetchone()
                if not row:
                    return False

                if row["type"] == "channel":
                    return row["role"] == "admin"

                return row["role"] is not None

    return await asyncio.to_thread(_q)


async def db_get_channel_subscribers_count(chat_id):
    """Число подписчиков канала"""
    if not DATABASE_URL:
        return 0

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM chat_members WHERE chat_id=%s",
                    (chat_id,)
                )
                row = cur.fetchone()
                return row["cnt"] if row else 0

    return await asyncio.to_thread(_q)


async def db_get_public_channels(limit=50):
    """Список публичных каналов (для поиска)"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.title, c.description, c.avatar_data,
                           c.avatar_color, c.created_at,
                           (SELECT COUNT(*) FROM chat_members
                            WHERE chat_id=c.id) as subscribers
                    FROM chats c
                    WHERE c.type='channel' AND c.is_public=TRUE
                    ORDER BY subscribers DESC, c.created_at DESC
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                for r in rows:
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()
                return rows

    return await asyncio.to_thread(_q)


async def db_search_public_channels(query, limit=30):
    """Поиск по публичным каналам"""
    if not DATABASE_URL or not query.strip():
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                q = f"%{query.strip()}%"
                cur.execute("""
                    SELECT c.id, c.title, c.description, c.avatar_data,
                           c.avatar_color,
                           (SELECT COUNT(*) FROM chat_members
                            WHERE chat_id=c.id) as subscribers
                    FROM chats c
                    WHERE c.type='channel' AND c.is_public=TRUE
                    AND (c.title ILIKE %s OR c.description ILIKE %s)
                    ORDER BY subscribers DESC
                    LIMIT %s
                """, (q, q, limit))
                return cur.fetchall()

    return await asyncio.to_thread(_q)


async def db_get_admin_channels(user_id):
    """Каналы, где юзер — админ"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.title, c.avatar_data, c.avatar_color,
                           (SELECT COUNT(*) FROM chat_members
                            WHERE chat_id=c.id) as subscribers
                    FROM chats c
                    JOIN chat_members m ON m.chat_id=c.id AND m.user_id=%s
                    WHERE c.type='channel' AND m.role='admin'
                    ORDER BY c.title
                """, (user_id,))
                return cur.fetchall()

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 9/25 ===
# ============================================================
#  СООБЩЕНИЯ — СОХРАНЕНИЕ
# ============================================================
async def db_save_message(chat_id, sender_id, sender_name, msg_type='text',
                          text=None, file_data=None, file_name=None,
                          file_type=None, duration=None, reply_to=None,
                          forwarded_from=None):
    """Сохраняет сообщение в БД (с шифрованием текста)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                encrypted_text = encrypt_text(text) if text else None
                cur.execute("""
                    INSERT INTO messages
                    (chat_id, sender_id, sender_name, msg_type, text,
                     file_data, file_name, file_type, duration,
                     reply_to, forwarded_from)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, created_at
                """, (
                    chat_id, sender_id, sender_name, msg_type,
                    encrypted_text, file_data, file_name, file_type,
                    duration, reply_to, forwarded_from
                ))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


# ============================================================
#  СООБЩЕНИЯ — ПОЛУЧЕНИЕ ИСТОРИИ
# ============================================================
async def db_get_chat_history(chat_id, limit=MAX_HISTORY, before_id=None):
    """История сообщений чата (с расшифровкой + реакциями)"""
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
                               u.avatar_color, u.avatar_data
                        FROM messages m
                        LEFT JOIN users u ON u.id = m.sender_id
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
                               u.avatar_color, u.avatar_data
                        FROM messages m
                        LEFT JOIN users u ON u.id = m.sender_id
                        WHERE m.chat_id=%s AND NOT m.deleted
                        ORDER BY m.id DESC LIMIT %s
                    """, (chat_id, limit))

                rows = list(reversed(cur.fetchall()))

                # Подгружаем реакции и расшифровываем текст
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
    """Одно сообщение по ID (для reply/forward)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.id, m.chat_id, m.sender_id, m.sender_name,
                           m.msg_type, m.text, m.file_data, m.file_name,
                           m.file_type, m.duration, m.reply_to,
                           m.forwarded_from, m.edited, m.deleted, m.created_at,
                           COALESCE(u.display_name, u.username) as sender_display,
                           u.avatar_color
                    FROM messages m
                    LEFT JOIN users u ON u.id = m.sender_id
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


async def db_get_last_message(chat_id):
    """Последнее сообщение в чате (для превью)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT m.id, m.sender_name, m.msg_type, m.text,
                           m.file_name, m.created_at
                    FROM messages m
                    WHERE m.chat_id=%s AND NOT m.deleted
                    ORDER BY m.id DESC LIMIT 1
                """, (chat_id,))
                row = cur.fetchone()
                if not row:
                    return None
                if row.get("text"):
                    row["text"] = decrypt_text(row["text"])
                if row["created_at"]:
                    row["created_at"] = row["created_at"].isoformat()
                return row

    return await asyncio.to_thread(_q)


async def db_get_messages_around(chat_id, message_id, count=30):
    """Сообщения вокруг указанного (для поиска с переходом)"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    (SELECT m.id, m.sender_name, m.msg_type, m.text,
                            m.created_at
                     FROM messages m
                     WHERE m.chat_id=%s AND m.id <= %s AND NOT m.deleted
                     ORDER BY m.id DESC LIMIT %s)
                    UNION ALL
                    (SELECT m.id, m.sender_name, m.msg_type, m.text,
                            m.created_at
                     FROM messages m
                     WHERE m.chat_id=%s AND m.id > %s AND NOT m.deleted
                     ORDER BY m.id ASC LIMIT %s)
                """, (chat_id, message_id, count, chat_id, message_id, count))
                rows = cur.fetchall()
                for r in rows:
                    if r.get("text"):
                        r["text"] = decrypt_text(r["text"])
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()
                return sorted(rows, key=lambda x: x["id"])

    return await asyncio.to_thread(_q)


# ============================================================
#  СООБЩЕНИЯ — РЕДАКТИРОВАНИЕ И УДАЛЕНИЕ
# ============================================================
async def db_edit_message(message_id, user_id, new_text):
    """Редактирование (только текст, только свой)"""
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
                    UPDATE messages
                    SET text=%s, edited=TRUE
                    WHERE id=%s AND sender_id=%s
                    AND msg_type='text' AND NOT deleted
                    RETURNING id
                """, (encrypted, message_id, user_id))
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_delete_message(message_id, user_id):
    """Удаление (soft delete)"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE messages
                    SET deleted=TRUE, text=NULL, file_data=NULL
                    WHERE id=%s AND sender_id=%s
                    RETURNING id
                """, (message_id, user_id))
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


async def db_delete_message_admin(message_id, chat_id, user_id):
    """Удаление админом (для групп/каналов)"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверяем что юзер — админ чата
                cur.execute("""
                    SELECT role FROM chat_members
                    WHERE chat_id=%s AND user_id=%s AND role='admin'
                """, (chat_id, user_id))
                if not cur.fetchone():
                    return False

                cur.execute("""
                    UPDATE messages
                    SET deleted=TRUE, text=NULL, file_data=NULL
                    WHERE id=%s AND chat_id=%s
                    RETURNING id
                """, (message_id, chat_id))
                ok = cur.fetchone() is not None
            conn.commit()
            return ok

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 10/25 ===
# ============================================================
#  РЕАКЦИИ
# ============================================================
async def db_toggle_reaction(message_id, user_id, emoji):
    """Ставит/убирает реакцию. Возвращает актуальный список реакций."""
    if not DATABASE_URL:
        return {}

    emoji = (emoji or "").strip()
    if not emoji or len(emoji) > 8:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверяем существующую реакцию
                cur.execute(
                    "SELECT emoji FROM reactions "
                    "WHERE message_id=%s AND user_id=%s",
                    (message_id, user_id)
                )
                existing = cur.fetchone()

                if existing and existing["emoji"] == emoji:
                    # Убираем — повторное нажатие той же
                    cur.execute(
                        "DELETE FROM reactions "
                        "WHERE message_id=%s AND user_id=%s",
                        (message_id, user_id)
                    )
                else:
                    # Ставим или меняем
                    cur.execute("""
                        INSERT INTO reactions (message_id, user_id, emoji)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (message_id, user_id)
                        DO UPDATE SET emoji=%s
                    """, (message_id, user_id, emoji, emoji))

                # Возвращаем актуальный список
                cur.execute("""
                    SELECT emoji, COUNT(*) as cnt FROM reactions
                    WHERE message_id=%s GROUP BY emoji
                """, (message_id,))
                reactions = {r["emoji"]: r["cnt"] for r in cur.fetchall()}

            conn.commit()
            return reactions

    return await asyncio.to_thread(_q)


async def db_get_message_reactions(message_id):
    """Получить реакции на сообщение"""
    if not DATABASE_URL:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT emoji, COUNT(*) as cnt FROM reactions
                    WHERE message_id=%s GROUP BY emoji
                """, (message_id,))
                return {r["emoji"]: r["cnt"] for r in cur.fetchall()}

    return await asyncio.to_thread(_q)


async def db_get_my_reactions(chat_id, user_id):
    """Реакции юзера в чате (для подсветки «моя реакция»)."""
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


async def db_clear_reactions(message_id):
    """Очистить все реакции сообщения"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM reactions WHERE message_id=%s",
                    (message_id,)
                )
            conn.commit()

    await asyncio.to_thread(_q)


# ============================================================
#  ПРОЧТЕНИЯ СООБЩЕНИЙ
# ============================================================
async def db_mark_message_read(message_id, user_id):
    """Отметить сообщение прочитанным"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO message_reads (message_id, user_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (message_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_read_count(message_id):
    """Сколько человек прочитали сообщение"""
    if not DATABASE_URL:
        return 0

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) as cnt FROM message_reads "
                    "WHERE message_id=%s",
                    (message_id,)
                )
                row = cur.fetchone()
                return row["cnt"] if row else 0

    return await asyncio.to_thread(_q)


async def db_get_reads_for_messages(message_ids):
    """Число прочтений для списка сообщений"""
    if not DATABASE_URL or not message_ids:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT message_id, COUNT(*) as cnt
                    FROM message_reads
                    WHERE message_id = ANY(%s)
                    GROUP BY message_id
                """, (message_ids,))
                return {r["message_id"]: r["cnt"] for r in cur.fetchall()}

    return await asyncio.to_thread(_q)


async def db_get_message_readers(message_id):
    """Список юзеров, кто прочитал"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT u.id, u.username, u.display_name, u.avatar_color,
                           u.avatar_data, r.read_at
                    FROM message_reads r
                    JOIN users u ON u.id = r.user_id
                    WHERE r.message_id=%s
                    ORDER BY r.read_at
                """, (message_id,))
                rows = cur.fetchall()
                for row in rows:
                    if row["read_at"]:
                        row["read_at"] = row["read_at"].isoformat()
                return rows

    return await asyncio.to_thread(_q)


# ============================================================
#  ЗАКРЕПЛЁННЫЕ СООБЩЕНИЯ
# ============================================================
async def db_pin_message(chat_id, message_id, user_id):
    """Закрепить сообщение в чате"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверяем что сообщение в этом чате
                cur.execute(
                    "SELECT id FROM messages WHERE id=%s AND chat_id=%s",
                    (message_id, chat_id)
                )
                if not cur.fetchone():
                    return False

                cur.execute("""
                    INSERT INTO pinned_messages (chat_id, message_id, pinned_by)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (chat_id, message_id, user_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unpin_message(chat_id, message_id):
    """Открепить сообщение"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pinned_messages "
                    "WHERE chat_id=%s AND message_id=%s",
                    (chat_id, message_id)
                )
            conn.commit()

    await asyncio.to_thread(_q)


async def db_get_pinned_message(chat_id):
    """Закреплённое сообщение чата (одно)"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pm.message_id, pm.pinned_at,
                           m.sender_name, m.msg_type, m.text,
                           m.created_at
                    FROM pinned_messages pm
                    JOIN messages m ON m.id = pm.message_id
                    WHERE pm.chat_id=%s AND NOT m.deleted
                    ORDER BY pm.pinned_at DESC LIMIT 1
                """, (chat_id,))
                row = cur.fetchone()
                if not row:
                    return None
                if row.get("text"):
                    row["text"] = decrypt_text(row["text"])
                if row["created_at"]:
                    row["created_at"] = row["created_at"].isoformat()
                if row["pinned_at"]:
                    row["pinned_at"] = row["pinned_at"].isoformat()
                return row

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 11/25 ===
# ============================================================
#  ПЕРЕСЫЛКА СООБЩЕНИЙ (FORWARD)
# ============================================================
async def db_forward_message(message_id, target_chat_id, sender_id, sender_name):
    """Копирует сообщение в другой чат с пометкой forward"""
    if not DATABASE_URL:
        return None

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT msg_type, text, file_data, file_name, file_type,
                           duration, sender_name
                    FROM messages
                    WHERE id=%s AND NOT deleted
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
                """, (
                    target_chat_id, sender_id, sender_name,
                    orig["msg_type"], orig["text"],
                    orig["file_data"], orig["file_name"], orig["file_type"],
                    orig["duration"], orig["sender_name"]
                ))
                row = cur.fetchone()
            conn.commit()
            return row

    return await asyncio.to_thread(_q)


async def db_forward_multiple(message_ids, target_chat_id, sender_id, sender_name):
    """Пересылка нескольких сообщений"""
    results = []
    for mid in message_ids:
        r = await db_forward_message(mid, target_chat_id, sender_id, sender_name)
        if r:
            results.append(r)
    return results


# ============================================================
#  ПОИСК СООБЩЕНИЙ
# ============================================================
async def db_search_messages(user_id, query, chat_id=None, limit=100):
    """Поиск по сообщениям (в чате или во всех чатах юзера)"""
    if not DATABASE_URL:
        return []

    query = (query or "").strip()
    if not query:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                q = f"%{query}%"

                if chat_id:
                    # Проверяем доступ
                    cur.execute("""
                        SELECT role FROM chat_members
                        WHERE chat_id=%s AND user_id=%s
                    """, (chat_id, user_id))
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
                    # Поиск по всем чатам юзера
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

                # Расшифровка + фильтрация (in-memory, т.к. шифрование)
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


async def db_search_in_chat(chat_id, user_id, query, limit=100):
    """Поиск строго в одном чате"""
    return await db_search_messages(user_id, query, chat_id=chat_id, limit=limit)


# ============================================================
#  ЗАКРЕПЛЁННЫЕ ЧАТЫ (для юзера)
# ============================================================
async def db_pin_chat(user_id, chat_id):
    """Закрепить чат у юзера"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Проверка что юзер — участник
                cur.execute("""
                    SELECT 1 FROM chat_members
                    WHERE chat_id=%s AND user_id=%s
                """, (chat_id, user_id))
                if not cur.fetchone():
                    return False

                cur.execute("""
                    INSERT INTO pinned_chats (user_id, chat_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (user_id, chat_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unpin_chat(user_id, chat_id):
    """Открепить чат"""
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


async def db_get_pinned_chat_ids(user_id):
    """ID закреплённых чатов"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT chat_id FROM pinned_chats "
                    "WHERE user_id=%s ORDER BY pinned_at DESC",
                    (user_id,)
                )
                return [r["chat_id"] for r in cur.fetchall()]

    return await asyncio.to_thread(_q)


# ============================================================
#  АРХИВ ЧАТОВ
# ============================================================
async def db_archive_chat(user_id, chat_id):
    """Поместить чат в архив"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM chat_members
                    WHERE chat_id=%s AND user_id=%s
                """, (chat_id, user_id))
                if not cur.fetchone():
                    return False

                cur.execute("""
                    INSERT INTO archived_chats (user_id, chat_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (user_id, chat_id))
            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_unarchive_chat(user_id, chat_id):
    """Убрать из архива"""
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


async def db_get_archived_chat_ids(user_id):
    """ID архивных чатов"""
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT chat_id FROM archived_chats WHERE user_id=%s",
                    (user_id,)
                )
                return [r["chat_id"] for r in cur.fetchall()]

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 12/25 ===
# ============================================================
#  СПИСОК ЧАТОВ ЮЗЕРА (главный экран)
# ============================================================
async def db_get_user_chats(user_id, include_archived=False):
    """
    Возвращает список чатов пользователя с:
    - последним сообщением
    - непрочитанными
    - онлайн-статусом собеседника
    - закрепами
    - архивом
    """
    if not DATABASE_URL:
        return []

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                # Закреплённые и архивные ID
                cur.execute(
                    "SELECT chat_id FROM pinned_chats WHERE user_id=%s",
                    (user_id,)
                )
                pinned_ids = {r["chat_id"] for r in cur.fetchall()}

                cur.execute(
                    "SELECT chat_id FROM archived_chats WHERE user_id=%s",
                    (user_id,)
                )
                archived_ids = {r["chat_id"] for r in cur.fetchall()}

                # Основной запрос
                cur.execute("""
                    SELECT
                        c.id, c.type, c.title, c.description, c.avatar_data,
                        c.avatar_color, c.is_public, c.invite_code,
                        c.created_at,

                        -- Собеседник (для private)
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
                        (SELECT u2.id FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_id,
                        (SELECT u2.last_seen FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_last_seen,
                        (SELECT u2.is_online FROM users u2
                         WHERE u2.id = (
                             SELECT user_id FROM chat_members
                             WHERE chat_id = c.id AND user_id != %s LIMIT 1
                         )) as other_online,

                        -- Последнее сообщение
                        (SELECT text FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_text,
                        (SELECT msg_type FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_type,
                        (SELECT COALESCE(u3.display_name, u3.username)
                         FROM messages m3
                         LEFT JOIN users u3 ON u3.id = m3.sender_id
                         WHERE m3.chat_id=c.id AND NOT m3.deleted
                         ORDER BY m3.id DESC LIMIT 1) as last_sender,
                        (SELECT created_at FROM messages WHERE chat_id=c.id AND NOT deleted
                         ORDER BY id DESC LIMIT 1) as last_time,

                        -- Непрочитанные
                        (SELECT COUNT(*) FROM messages
                         WHERE chat_id=c.id
                         AND id > COALESCE(
                             (SELECT last_read_id FROM chat_members
                              WHERE chat_id=c.id AND user_id=%s), 0
                         )
                         AND sender_id != %s
                         AND NOT deleted
                        ) as unread,

                        -- Число участников
                        (SELECT COUNT(*) FROM chat_members WHERE chat_id=c.id) as member_count,

                        -- Мьют
                        (SELECT muted FROM chat_members
                         WHERE chat_id=c.id AND user_id=%s) as muted

                    FROM chats c
                    JOIN chat_members m ON m.chat_id=c.id AND m.user_id=%s
                    ORDER BY last_time DESC NULLS LAST, c.id DESC
                """, (
                    user_id, user_id, user_id, user_id, user_id,
                    user_id, user_id, user_id, user_id, user_id
                ))
                rows = cur.fetchall()

                result = []
                for r in rows:
                    # Фильтр архива
                    if not include_archived and r["id"] in archived_ids:
                        continue
                    if include_archived and r["id"] not in archived_ids:
                        continue

                    # Расшифровка
                    if r.get("last_text"):
                        r["last_text"] = decrypt_text(r["last_text"])

                    # ISO
                    if r["last_time"]:
                        r["last_time"] = r["last_time"].isoformat()
                    if r["other_last_seen"]:
                        r["other_last_seen"] = r["other_last_seen"].isoformat()
                    if r["created_at"]:
                        r["created_at"] = r["created_at"].isoformat()

                    # Флаги
                    r["pinned"] = r["id"] in pinned_ids
                    r["archived"] = r["id"] in archived_ids

                    result.append(dict(r))

                # Сортировка: закрепы первые
                result.sort(key=lambda x: (
                    not x["pinned"],
                    x["last_time"] is None,
                    x["last_time"] or "",
                ), reverse=False)
                # Правильный порядок: pinned первый, потом по времени (DESC)
                pinned = [c for c in result if c["pinned"]]
                others = [c for c in result if not c["pinned"]]
                pinned.sort(key=lambda x: x["last_time"] or "", reverse=True)
                others.sort(key=lambda x: x["last_time"] or "", reverse=True)

                return pinned + others

    return await asyncio.to_thread(_q)


async def db_get_archived_chats(user_id):
    """Только архивные чаты"""
    return await db_get_user_chats(user_id, include_archived=True)


# ============================================================
#  МЬЮТ ЧАТА
# ============================================================
async def db_mute_chat(chat_id, user_id, muted=True):
    """Включить/выключить мьют"""
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE chat_members SET muted=%s
                    WHERE chat_id=%s AND user_id=%s
                """, (muted, chat_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


async def db_is_chat_muted(chat_id, user_id):
    """Проверка мьюта"""
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT muted FROM chat_members "
                    "WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id)
                )
                row = cur.fetchone()
                return bool(row and row["muted"])

    return await asyncio.to_thread(_q)


# ============================================================
#  УДАЛЕНИЕ ЧАТА У ЮЗЕРА
# ============================================================
async def db_delete_chat_for_user(chat_id, user_id):
    """
    Удаляет чат у юзера (не для всех).
    Для приватных — удаляет полностью (2 участника).
    Для групп/каналов — выходит из чата.
    """
    if not DATABASE_URL:
        return False

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT type FROM chats WHERE id=%s", (chat_id,))
                row = cur.fetchone()
                if not row:
                    return False

                chat_type = row["type"]

                if chat_type == "private":
                    # Полное удаление
                    cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
                else:
                    # Выход из группы/канала
                    cur.execute(
                        "DELETE FROM chat_members WHERE chat_id=%s AND user_id=%s",
                        (chat_id, user_id)
                    )
                    # Если пусто — удаляем
                    cur.execute(
                        "SELECT COUNT(*) as cnt FROM chat_members WHERE chat_id=%s",
                        (chat_id,)
                    )
                    cnt_row = cur.fetchone()
                    if cnt_row and cnt_row["cnt"] == 0:
                        cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))

            conn.commit()
            return True

    return await asyncio.to_thread(_q)


async def db_clear_chat_history(chat_id, user_id):
    """
    Очистить историю у себя. 
    Реализация: устанавливаем last_read_id на последнее сообщение
    (чтобы считалось прочитанным) + скрытие для юзера.
    Пока — просто отмечаем как прочитанное.
    """
    if not DATABASE_URL:
        return

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(id) as last_id FROM messages WHERE chat_id=%s",
                    (chat_id,)
                )
                row = cur.fetchone()
                last_id = row["last_id"] if row and row["last_id"] else 0

                cur.execute("""
                    UPDATE chat_members SET last_read_id=%s
                    WHERE chat_id=%s AND user_id=%s
                """, (last_id, chat_id, user_id))
            conn.commit()

    await asyncio.to_thread(_q)


# ============================================================
#  ОБЩАЯ СТАТИСТИКА
# ============================================================
async def db_get_total_stats():
    """Общая статистика приложения"""
    if not DATABASE_URL:
        return {}

    def _q():
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as cnt FROM users")
                users = cur.fetchone()["cnt"]

                cur.execute("SELECT COUNT(*) as cnt FROM chats WHERE type='private'")
                privates = cur.fetchone()["cnt"]

                cur.execute("SELECT COUNT(*) as cnt FROM chats WHERE type='group'")
                groups = cur.fetchone()["cnt"]

                cur.execute("SELECT COUNT(*) as cnt FROM chats WHERE type='channel'")
                channels = cur.fetchone()["cnt"]

                cur.execute("SELECT COUNT(*) as cnt FROM messages WHERE NOT deleted")
                messages = cur.fetchone()["cnt"]

                return {
                    "users": users,
                    "private_chats": privates,
                    "groups": groups,
                    "channels": channels,
                    "messages": messages,
                }

    return await asyncio.to_thread(_q)


# === КОНЕЦ ЧАСТИ 13/25 ===
# ============================================================
#  WEBSOCKET — ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ============================================================
# { websocket: {"id": int, "name": str, "display_name": str, "color": str} }
clients: Dict[Any, Dict[str, Any]] = {}

# { user_id: set(websockets) } — мультиустройство
user_sockets: Dict[int, set] = {}

# Блокировка для атомарных операций с clients
clients_lock = asyncio.Lock()


# ============================================================
#  HEALTH-CHECK для Render / Railway
# ============================================================
async def process_request(path, request_headers):
    """Отвечает OK на обычные HTTP-запросы (health-check)."""
    if "Upgrade" not in request_headers.get("Connection", ""):
        return http.HTTPStatus.OK, [], b"Blaze Messenger v3.0 is running\n"
    return None


# ============================================================
#  БЕЗОПАСНАЯ ОТПРАВКА
# ============================================================
async def send_safe(ws, data: str):
    """Отправить данные, игнорируя ошибки."""
    try:
        await ws.send(data)
    except Exception:
        pass


async def send_json(ws, obj: dict):
    """Отправить JSON."""
    try:
        await ws.send(json.dumps(obj, ensure_ascii=False))
    except Exception:
        pass


# ============================================================
#  ОТПРАВКА ПОЛЬЗОВАТЕЛЮ (мультиустройство)
# ============================================================
async def send_to_user(user_id: int, message: dict):
    """Отправить сообщение всем сокетам пользователя."""
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
    """Отправить группе пользователей."""
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


# ============================================================
#  BROADCAST В ЧАТ
# ============================================================
async def broadcast_chat(chat_id: int, message: dict, exclude=None):
    """Отправить сообщение всем участникам чата."""
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


async def broadcast_chat_except(chat_id: int, message: dict, exclude_user_id: int):
    """Отправить всем, кроме одного пользователя (например отправителя)."""
    member_ids = await db_get_chat_member_ids(chat_id)
    data = json.dumps(message, ensure_ascii=False)
    tasks = []
    for uid in member_ids:
        if uid == exclude_user_id:
            continue
        if uid in user_sockets:
            for ws in list(user_sockets[uid]):
                if ws.open:
                    tasks.append(send_safe(ws, data))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ============================================================
#  ОБНОВЛЕНИЕ СПИСКА ЧАТОВ У УЧАСТНИКОВ
# ============================================================
async def refresh_chats_list(user_ids: List[int]):
    """Переслать актуальный список чатов указанным юзерам."""
    for uid in user_ids:
        if uid not in user_sockets:
            continue
        try:
            chats = await db_get_user_chats(uid)
            await send_to_user(uid, {"type": "chats_list", "chats": chats})
        except Exception as e:
            print(f"refresh_chats_list error for {uid}: {e}")


async def refresh_chats_for_chat(chat_id: int):
    """Переслать список чатов всем участникам чата."""
    member_ids = await db_get_chat_member_ids(chat_id)
    await refresh_chats_list(member_ids)


# ============================================================
#  ПУБЛИЧНЫЕ ДАННЫЕ ЮЗЕРА
# ============================================================
def user_public(u: dict) -> dict:
    """
    Универсальная функция — поддерживает и dict из БД,
    и dict из памяти (clients).
    """
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


def chat_public(c: dict) -> dict:
    """Нормализация чата для отправки клиенту."""
    if not c:
        return {}
    return {
        "id": c.get("id"),
        "type": c.get("type"),
        "title": c.get("title"),
        "description": c.get("description", ""),
        "avatar_data": c.get("avatar_data"),
        "avatar_color": c.get("avatar_color", "#ff6b00"),
        "is_public": bool(c.get("is_public", False)),
        "invite_code": c.get("invite_code"),
    }


# ============================================================
#  БЕЗОПАСНОЕ ЗАКРЫТИЕ СОКЕТА
# ============================================================
async def safe_close(ws, code=1000, reason=""):
    try:
        await ws.close(code, reason)
    except Exception:
        pass


async def disconnect_user(ws, user_info: Optional[dict] = None):
    """Аккуратно удалить сокет из структур."""
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
            # Последний сокет закрылся — оффлайн
            try:
                await db_set_online(uid, False)
                await db_update_last_seen(uid)
            except Exception as e:
                print(f"Set offline error: {e}")


# === КОНЕЦ ЧАСТИ 14/25 ===
# ============================================================
#  АУТЕНТИФИКАЦИЯ
# ============================================================
async def handle_auth(websocket, first_msg: dict):
    """
    Обработка первого сообщения клиента:
    - action='token' — вход по JWT
    - action='login' — вход по логину/паролю
    - action='register' — регистрация
    - action='verify_code' — верификация email-кода
    """
    action = first_msg.get("action")

    # ============ ВХОД ПО JWT-ТОКЕНУ ============
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
                raise ValueError("No user_id in token")

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

    # ============ ВЕРИФИКАЦИЯ КОДА ИЗ EMAIL ============
    if action == "verify_code":
        user_id = first_msg.get("user_id")
        code = str(first_msg.get("code", "")).strip()

        if not user_id or not is_code_valid_format(code):
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Неверный формат кода"
            })
            return None

        ok = await db_check_code(user_id, code, "login")
        if not ok:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Неверный или истёкший код"
            })
            return None

        u = await db_get_user_by_id(user_id)
        if not u:
            await send_json(websocket, {
                "type": "auth_error",
                "text": "Пользователь не найден"
            })
            return None

        token = jwt.encode({
            "user_id": u["id"],
            "username": u["username"],
            "exp": datetime.datetime.utcnow() + datetime.timedelta(days=JWT_EXP_DAYS),
        }, JWT_SECRET, algorithm=JWT_ALGO)

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

    # ============ ОБЫЧНЫЙ ВХОД / РЕГИСТРАЦИЯ ============
    username = str(first_msg.get("username", "")).strip()[:MAX_USERNAME]
    password = str(first_msg.get("password", ""))

    if not username or not password:
        await send_json(websocket, {
            "type": "auth_error",
            "text": "Логин и пароль обязательны"
        })
        return None

    # ============ ГОСТЕВОЙ РЕЖИМ (если БД не подключена) ============
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

    # ============ РЕГИСТРАЦИЯ ============
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

        # Добавляем в общий чат
        try:
            await db_add_user_to_global_chat(user["id"])
        except Exception as e:
            print(f"Add to global chat error: {e}")

    # ============ ЛОГИН ============
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

        # ============ EMAIL-ВЕРИФИКАЦИЯ (2FA) ============
        user_email = user.get("email")
        if user_email and EMAILS_ENABLED:
            # Проверка антиспама
            recent_codes = await db_count_user_codes(user["id"], "login")
            if recent_codes >= 5:
                await send_json(websocket, {
                    "type": "auth_error",
                    "text": "Слишком много попыток. Попробуй через час."
                })
                return None

            # Генерируем и сохраняем код
            code = generate_verification_code()
            await db_save_code(user["id"], user_email, code, "login", ttl_minutes=10)

            # Отправляем письмо
            html = make_code_email_html(
                code,
                user.get("display_name") or user["username"],
                "login"
            )
            sent = await send_email(user_email, "🔥 Blaze — код входа", html)

            if sent:
                await send_json(websocket, {
                    "type": "email_code_required",
                    "user_id": user["id"],
                    "email": mask_email(user_email),
                    "username": user["username"],
                    "text": f"Код отправлен на {mask_email(user_email)}"
                })
                return None
            else:
                print(f"⚠️ Email не отправлен, вход без 2FA")

    else:
        await send_json(websocket, {
            "type": "auth_error",
            "text": "Укажите action: login, register или token"
        })
        return None

    # ============ УСПЕШНЫЙ ВХОД ============
    token = jwt.encode({
        "user_id": user["id"],
        "username": user["username"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=JWT_EXP_DAYS),
    }, JWT_SECRET, algorithm=JWT_ALGO)

    # Обновляем статус «онлайн»
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


# === КОНЕЦ ЧАСТИ 15/25 ===
# ============================================================
#  ОСНОВНОЙ HANDLER — точка входа для каждого соединения
# ============================================================
async def handler(websocket):
    user = None
    try:
        # ============ ПЕРВОЕ СООБЩЕНИЕ — АВТОРИЗАЦИЯ ============
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

        # ============ РЕГИСТРАЦИЯ СОКЕТА ============
        async with clients_lock:
            clients[websocket] = user
            user_sockets.setdefault(user["id"], set()).add(websocket)

        print(f"[+] {user['name']} (id={user['id']}). Всего: {len(clients)}")

        # ============ ОТПРАВКА СТАРТОВЫХ ДАННЫХ ============
        # 1. auth_ok
        await send_json(websocket, {
            "type": "auth_ok",
            "user": user_public(user),
            "language": user.get("language", "ru"),
            "theme": user.get("theme", "dark"),
            "token": user.get("token", ""),
        })

        # 2. Список чатов
        try:
            chats = await db_get_user_chats(user["id"])
            await send_json(websocket, {
                "type": "chats_list",
                "chats": chats,
            })
        except Exception as e:
            print(f"chats_list error: {e}")

        # 3. Все пользователи (для создания чатов)
        try:
            users = await db_get_all_users(exclude_id=user["id"])
            await send_json(websocket, {
                "type": "all_users",
                "users": [user_public(u) for u in users],
            })
        except Exception as e:
            print(f"all_users error: {e}")

        # 4. Уведомить остальных, что юзер онлайн
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

        # ============ ОСНОВНОЙ ЦИКЛ ============
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
        # ============ ОТКЛЮЧЕНИЕ ============
        if websocket in clients:
            user_info = clients[websocket]
            await disconnect_user(websocket, user_info)

            print(f"[-] {user_info.get('name')}. Всего: {len(clients)}")

            # Уведомить остальных, что оффлайн
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
#  МАРШРУТИЗАЦИЯ СООБЩЕНИЙ
# ============================================================
async def route_message(websocket, user, mtype, msg):
    """Маршрутизирует входящее сообщение к нужному обработчику."""

    # ===== СООБЩЕНИЯ =====
    if mtype == "message":
        await handle_new_message(websocket, user, msg)

    elif mtype == "open_chat":
        await handle_open_chat(websocket, user, msg)

    elif mtype == "typing":
        await handle_typing(websocket, user, msg)

    elif mtype == "read":
        await handle_read(websocket, user, msg)

    # ===== РЕАКЦИИ / EDIT / DELETE / FORWARD =====
    elif mtype == "react":
        await handle_react(websocket, user, msg)

    elif mtype == "edit_message":
        await handle_edit_message(websocket, user, msg)

    elif mtype == "delete_message":
        await handle_delete_message(websocket, user, msg)

    elif mtype == "forward_message":
        await handle_forward_message(websocket, user, msg)

    # ===== ЧАТЫ =====
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

    # ===== ЗАКРЕПЫ / АРХИВ / МЬЮТ =====
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

    elif mtype == "pin_message":
        await handle_pin_message(websocket, user, msg, True)

    elif mtype == "unpin_message":
        await handle_pin_message(websocket, user, msg, False)

    # ===== ПОИСК =====
    elif mtype == "search_messages":
        await handle_search_messages(websocket, user, msg)

    elif mtype == "search_users":
        await handle_search_users(websocket, user, msg)

    elif mtype == "search_channels":
        await handle_search_channels(websocket, user, msg)

    # ===== ПРОФИЛЬ =====
    elif mtype == "update_profile":
        await handle_update_profile(websocket, user, msg)

    elif mtype == "change_username":
        await handle_change_username(websocket, user, msg)

    # ===== ЗВОНКИ (WebRTC signaling) =====
    elif mtype in ("call_offer", "call_answer", "call_ice",
                   "call_end", "call_reject", "call_busy"):
        await handle_call_signal(websocket, user, msg)

    # ===== PUSH =====
    elif mtype == "push_subscribe":
        await handle_push_subscribe(websocket, user, msg)

    # ===== PING (keep-alive) =====
    elif mtype == "ping":
        await send_json(websocket, {"type": "pong"})

    else:
        await send_json(websocket, {
            "type": "error",
            "text": f"Неизвестный тип: {mtype}"
        })


# === КОНЕЦ ЧАСТИ 16/25 ===
# ============================================================
#  ОБРАБОТЧИК: НОВОЕ СООБЩЕНИЕ
# ============================================================
async def handle_new_message(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    # Проверка прав
    can_post = await db_can_post_to_chat(chat_id, user["id"])
    if not can_post:
        await send_json(websocket, {
            "type": "error",
            "text": "Нет прав для отправки сообщений"
        })
        return

    msg_type = msg.get("msg_type", "text")
    text = msg.get("text")
    file_data = msg.get("file_data")
    file_name = msg.get("file_name")
    file_type = msg.get("file_type")
    duration = msg.get("duration")
    reply_to = msg.get("reply_to")

    # Валидация текста
    if text:
        text = str(text)[:MAX_MESSAGE_LENGTH]

    # Валидация файлов
    if file_data:
        max_size = MAX_FILE_SIZE
        if msg_type == "voice":
            max_size = MAX_VOICE_SIZE
        elif msg_type == "avatar":
            max_size = MAX_AVATAR_SIZE

        file_size = get_file_size_base64(file_data)
        if file_size > max_size:
            await send_json(websocket, {
                "type": "error",
                "text": f"Файл слишком большой (макс {max_size // (1024*1024)} МБ)"
            })
            return

    # Пустое сообщение — игнор
    if not text and not file_data:
        return

    # Проверка reply_to (существование)
    if reply_to:
        orig = await db_get_message_by_id(reply_to)
        if not orig or orig.get("chat_id") != chat_id:
            reply_to = None

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
    created = saved["created_at"].isoformat() if saved["created_at"] else now_iso()

    # Информация о reply
    reply_info = None
    if reply_to:
        orig = await db_get_message_by_id(reply_to)
        if orig:
            reply_info = {
                "id": orig["id"],
                "nickname": orig.get("sender_display") or orig.get("sender_name"),
                "text": orig.get("text") or "📎 Файл",
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
    await refresh_chats_for_chat(chat_id)


# ============================================================
#  ОБРАБОТЧИК: ОТКРЫТЬ ЧАТ (загрузка истории)
# ============================================================
async def handle_open_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        await send_json(websocket, {
            "type": "error",
            "text": "Вы не участник этого чата"
        })
        return

    # История
    history = await db_get_chat_history(chat_id)

    # Реакции юзера (для подсветки «моя реакция»)
    my_reactions = await db_get_my_reactions(chat_id, user["id"])

    # Отправляем историю
    await send_json(websocket, {
        "type": "chat_history",
        "chat_id": chat_id,
        "messages": history,
        "my_reactions": my_reactions,
    })

    # Отмечаем прочитанным
    if history:
        last_id = history[-1]["id"]
        await db_mark_chat_read(chat_id, user["id"], last_id)

        # Уведомить отправителей о прочтении
        await notify_read_receipt(chat_id, user["id"], last_id)

        # Обновить бейдж у себя
        try:
            chats = await db_get_user_chats(user["id"])
            await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
        except Exception:
            pass


async def notify_read_receipt(chat_id, reader_id, last_message_id):
    """Уведомить отправителей, что их сообщения прочитаны"""
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT sender_id FROM messages
                    WHERE chat_id=%s AND id <= %s
                    AND sender_id != %s AND NOT deleted
                """, (chat_id, last_message_id, reader_id))
                sender_ids = [r["sender_id"] for r in cur.fetchall()]

        if sender_ids:
            await send_to_users(sender_ids, {
                "type": "messages_read",
                "chat_id": chat_id,
                "reader_id": reader_id,
                "last_read_id": last_message_id,
            })
    except Exception as e:
        print(f"notify_read_receipt error: {e}")


# ============================================================
#  ОБРАБОТЧИК: «ПЕЧАТАЕТ...»
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
#  ОБРАБОТЧИК: ПРОЧИТАНО
# ============================================================
async def handle_read(websocket, user, msg):
    chat_id = msg.get("chat_id")
    message_id = msg.get("message_id")
    if not chat_id or not message_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    # Отмечаем в БД
    await db_mark_message_read(message_id, user["id"])
    await db_mark_chat_read(chat_id, user["id"], message_id)

    # Уведомить остальных
    await broadcast_chat(chat_id, {
        "type": "message_read",
        "chat_id": chat_id,
        "message_id": message_id,
        "user_id": user["id"],
        "username": user["display_name"],
    }, exclude=websocket)


# ============================================================
#  ОБРАБОТЧИК: РЕАКЦИЯ
# ============================================================
async def handle_react(websocket, user, msg):
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")
    emoji = msg.get("emoji")

    if not message_id or not chat_id or not emoji:
        return

    # Проверка доступа
    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    # Проверка что сообщение в этом чате
    orig = await db_get_message_by_id(message_id)
    if not orig or orig.get("chat_id") != chat_id:
        return

    # Toggle
    reactions = await db_toggle_reaction(message_id, user["id"], emoji)

    # Рассылаем
    await broadcast_chat(chat_id, {
        "type": "reactions_update",
        "message_id": message_id,
        "chat_id": chat_id,
        "reactions": reactions,
        "user_id": user["id"],
        "emoji": emoji,
    })


# === КОНЕЦ ЧАСТИ 17/25 ===
# ============================================================
#  ОБРАБОТЧИК: РЕДАКТИРОВАНИЕ СООБЩЕНИЯ
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
#  ОБРАБОТЧИК: УДАЛЕНИЕ СООБЩЕНИЯ
# ============================================================
async def handle_delete_message(websocket, user, msg):
    message_id = msg.get("message_id")
    chat_id = msg.get("chat_id")

    if not message_id or not chat_id:
        return

    # Пытаемся удалить как автор
    ok = await db_delete_message(message_id, user["id"])

    # Если не автор — пробуем как админ
    if not ok:
        ok = await db_delete_message_admin(message_id, chat_id, user["id"])

    if ok:
        await broadcast_chat(chat_id, {
            "type": "message_deleted",
            "message_id": message_id,
            "chat_id": chat_id,
        })


# ============================================================
#  ОБРАБОТЧИК: ПЕРЕСЫЛКА СООБЩЕНИЯ
# ============================================================
async def handle_forward_message(websocket, user, msg):
    message_id = msg.get("message_id")
    target_chat_id = msg.get("target_chat_id")

    if not message_id or not target_chat_id:
        return

    # Проверка доступа к целевому чату
    can_post = await db_can_post_to_chat(target_chat_id, user["id"])
    if not can_post:
        await send_json(websocket, {
            "type": "error",
            "text": "Нет прав для пересылки в этот чат"
        })
        return

    # Проверка что исходное сообщение доступно
    orig = await db_get_message_by_id(message_id)
    if not orig or orig.get("deleted"):
        await send_json(websocket, {
            "type": "error",
            "text": "Сообщение недоступно"
        })
        return

    # Проверка что юзер — участник исходного чата
    orig_chat_member = await db_check_member(orig["chat_id"], user["id"])
    if not orig_chat_member:
        return

    # Пересылка
    saved = await db_forward_message(
        message_id, target_chat_id, user["id"], user["display_name"]
    )
    if not saved:
        return

    # Оригинальное сообщение (для контента)
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
#  ОБРАБОТЧИК: СОЗДАНИЕ ЛИЧНОГО ЧАТА
# ============================================================
async def handle_create_private(websocket, user, msg):
    other_id = msg.get("user_id")
    if not other_id or other_id == user["id"]:
        return

    # Проверка что юзер существует
    other_user = await db_get_user_by_id(other_id)
    if not other_user:
        await send_json(websocket, {
            "type": "error",
            "text": "Пользователь не найден"
        })
        return

    chat = await db_get_or_create_private_chat(user["id"], other_id)
    if chat:
        # Обновляем у обоих
        await refresh_chats_list([user["id"], other_id])

        # Открываем чат у инициатора
        await send_json(websocket, {
            "type": "chat_opened",
            "chat_id": chat["id"],
        })


# ============================================================
#  ОБРАБОТЧИК: СОЗДАНИЕ ГРУППЫ
# ============================================================
async def handle_create_group(websocket, user, msg):
    title = str(msg.get("title", "")).strip()[:MAX_GROUP_TITLE]
    description = str(msg.get("description", ""))[:MAX_GROUP_DESCRIPTION]
    member_ids = msg.get("member_ids", [])
    avatar_data = msg.get("avatar_data")

    if not title:
        await send_json(websocket, {
            "type": "error",
            "text": "Введи название группы"
        })
        return

    if not isinstance(member_ids, list):
        member_ids = []

    # Лимит участников
    if len(member_ids) > 200:
        member_ids = member_ids[:200]

    group = await db_create_group(
        creator_id=user["id"],
        title=title,
        member_ids=member_ids,
        description=description,
    )

    if not group:
        await send_json(websocket, {
            "type": "error",
            "text": "Не удалось создать группу"
        })
        return

    chat_id = group["id"]

    # Обновить аватар если есть
    if avatar_data:
        try:
            await db_update_group(chat_id, avatar_data=avatar_data)
        except Exception:
            pass

    # Уведомить всех участников
    all_members = [user["id"]] + [m for m in member_ids if m != user["id"]]
    await refresh_chats_list(all_members)

    # Уведомление о создании
    await broadcast_chat(chat_id, {
        "type": "system",
        "chat_id": chat_id,
        "text": f"{user['display_name']} создал(а) группу «{title}»",
        "time": now_str(),
    })

    # Открыть у создателя
    await send_json(websocket, {
        "type": "chat_opened",
        "chat_id": chat_id,
    })


# ============================================================
#  ОБРАБОТЧИК: СОЗДАНИЕ КАНАЛА
# ============================================================
async def handle_create_channel(websocket, user, msg):
    title = str(msg.get("title", "")).strip()[:MAX_GROUP_TITLE]
    description = str(msg.get("description", ""))[:MAX_GROUP_DESCRIPTION]
    is_public = bool(msg.get("is_public", False))
    avatar_data = msg.get("avatar_data")

    if not title:
        await send_json(websocket, {
            "type": "error",
            "text": "Введи название канала"
        })
        return

    channel = await db_create_channel(
        creator_id=user["id"],
        title=title,
        description=description,
        is_public=is_public,
        avatar_data=avatar_data,
    )

    if not channel:
        await send_json(websocket, {
            "type": "error",
            "text": "Не удалось создать канал"
        })
        return

    chat_id = channel["id"]

    await refresh_chats_list([user["id"]])

    await broadcast_chat(chat_id, {
        "type": "system",
        "chat_id": chat_id,
        "text": f"Канал «{title}» создан",
        "time": now_str(),
    })

    await send_json(websocket, {
        "type": "chat_opened",
        "chat_id": chat_id,
    })


# === КОНЕЦ ЧАСТИ 18/25 ===
# ============================================================
#  ОБРАБОТЧИК: ИНФО О ЧАТЕ
# ============================================================
async def handle_get_chat_info(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        await send_json(websocket, {
            "type": "error",
            "text": "Нет доступа"
        })
        return

    info = await db_get_chat_info(chat_id)
    if not info:
        await send_json(websocket, {
            "type": "error",
            "text": "Чат не найден"
        })
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


# ============================================================
#  ОБРАБОТЧИК: ДОБАВИТЬ УЧАСТНИКОВ
# ============================================================
async def handle_add_members(websocket, user, msg):
    chat_id = msg.get("chat_id")
    user_ids = msg.get("user_ids", [])

    if not chat_id or not isinstance(user_ids, list) or not user_ids:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {
            "type": "error",
            "text": "Только админ может добавлять"
        })
        return

    # Лимит участников
    try:
        current_members = await db_get_chat_member_ids(chat_id)
        if len(current_members) + len(user_ids) > 200:
            await send_json(websocket, {
                "type": "error",
                "text": "Максимум 200 участников"
            })
            return
    except Exception:
        pass

    await db_add_members(chat_id, user_ids)

    # Уведомить всех
    all_affected = list(set(current_members + user_ids))
    await refresh_chats_list(all_affected)

    # Обновить список участников
    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })


# ============================================================
#  ОБРАБОТЧИК: УДАЛИТЬ УЧАСТНИКА
# ============================================================
async def handle_remove_member(websocket, user, msg):
    chat_id = msg.get("chat_id")
    target_id = msg.get("user_id")

    if not chat_id or not target_id:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {
            "type": "error",
            "text": "Только админ может удалять"
        })
        return

    # Нельзя удалить себя (для этого leave)
    if target_id == user["id"]:
        return

    await db_remove_member(chat_id, target_id)

    # Уведомить удалённого
    await send_to_user(target_id, {
        "type": "removed_from_chat",
        "chat_id": chat_id,
    })

    # Обновить у всех
    await refresh_chats_list([target_id, user["id"]])

    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })


# ============================================================
#  ОБРАБОТЧИК: НАЗНАЧИТЬ/СНЯТЬ АДМИНА
# ============================================================
async def handle_set_admin(websocket, user, msg):
    chat_id = msg.get("chat_id")
    target_id = msg.get("user_id")
    role = msg.get("role", "admin")

    if not chat_id or not target_id:
        return

    if role not in ("admin", "member"):
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {
            "type": "error",
            "text": "Только админ может менять роли"
        })
        return

    await db_set_member_role(chat_id, target_id, role)

    # Обновить участников
    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })

    # Уведомить целевого юзера
    await send_to_user(target_id, {
        "type": "role_changed",
        "chat_id": chat_id,
        "role": role,
    })


# ============================================================
#  ОБРАБОТЧИК: ОБНОВИТЬ ГРУППУ/КАНАЛ
# ============================================================
async def handle_update_group(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {
            "type": "error",
            "text": "Только админ может редактировать"
        })
        return

    chat_info = await db_get_chat_info(chat_id)
    if not chat_info:
        return

    # Валидация
    title = msg.get("title")
    if title is not None:
        title = str(title).strip()[:MAX_GROUP_TITLE] or None

    description = msg.get("description")
    if description is not None:
        description = str(description)[:MAX_GROUP_DESCRIPTION]

    avatar_data = msg.get("avatar_data")

    if chat_info["type"] == "channel":
        ok = await db_update_channel(
            chat_id, user["id"],
            title=title,
            description=description,
            avatar_data=avatar_data,
        )
    else:
        await db_update_group(
            chat_id,
            title=title,
            description=description,
            avatar_data=avatar_data,
        )
        ok = True

    if ok:
        info = await db_get_chat_info(chat_id)
        if info and info.get("created_at"):
            info["created_at"] = info["created_at"].isoformat()

        await broadcast_chat(chat_id, {
            "type": "chat_updated",
            "chat": dict(info) if info else {},
        })

        await refresh_chats_for_chat(chat_id)


# ============================================================
#  ОБРАБОТЧИК: ВЫХОД ИЗ ЧАТА
# ============================================================
async def handle_leave_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    ok = await db_leave_chat(chat_id, user["id"])
    if not ok:
        await send_json(websocket, {
            "type": "error",
            "text": "Нельзя выйти из этого чата"
        })
        return

    # Уведомить остальных
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
            "text": f"{user['display_name']} покинул(а) чат",
            "time": now_str(),
        })

    # Обновить у себя
    await refresh_chats_list([user["id"]])


# ============================================================
#  ОБРАБОТЧИК: УДАЛИТЬ ЧАТ У СЕБЯ
# ============================================================
async def handle_delete_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    ok = await db_delete_chat_for_user(chat_id, user["id"])
    if ok:
        await refresh_chats_list([user["id"]])


# === КОНЕЦ ЧАСТИ 19/25 ===
# ============================================================
#  ОБРАБОТЧИК: ЗАКРЕП / ОТКРЕП ЧАТА
# ============================================================
async def handle_pin_chat(websocket, user, msg, pin: bool):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    if pin:
        ok = await db_pin_chat(user["id"], chat_id)
        if not ok:
            await send_json(websocket, {
                "type": "error",
                "text": "Не удалось закрепить чат"
            })
            return
    else:
        await db_unpin_chat(user["id"], chat_id)

    # Обновить список чатов у себя
    try:
        chats = await db_get_user_chats(user["id"])
        await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
    except Exception:
        pass


# ============================================================
#  ОБРАБОТЧИК: АРХИВ / РАЗАРХИВ ЧАТА
# ============================================================
async def handle_archive_chat(websocket, user, msg, archive: bool):
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    if archive:
        ok = await db_archive_chat(user["id"], chat_id)
        if not ok:
            await send_json(websocket, {
                "type": "error",
                "text": "Не удалось добавить в архив"
            })
            return
    else:
        await db_unarchive_chat(user["id"], chat_id)

    # Обновить список чатов у себя
    try:
        chats = await db_get_user_chats(user["id"])
        await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
    except Exception:
        pass


# ============================================================
#  ОБРАБОТЧИК: МЬЮТ / АНМЬЮТ ЧАТА
# ============================================================
async def handle_mute_chat(websocket, user, msg):
    chat_id = msg.get("chat_id")
    muted = bool(msg.get("muted", True))

    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    await db_mute_chat(chat_id, user["id"], muted)

    # Уведомить свой клиент
    await send_to_user(user["id"], {
        "type": "mute_changed",
        "chat_id": chat_id,
        "muted": muted,
    })

    # Обновить список чатов у себя
    try:
        chats = await db_get_user_chats(user["id"])
        await send_to_user(user["id"], {"type": "chats_list", "chats": chats})
    except Exception:
        pass


# ============================================================
#  ОБРАБОТЧИК: ЗАКРЕП / ОТКРЕП СООБЩЕНИЯ
# ============================================================
async def handle_pin_message(websocket, user, msg, pin: bool):
    chat_id = msg.get("chat_id")
    message_id = msg.get("message_id")

    if not chat_id or not message_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    if pin:
        # Проверка прав: в группе — админ, в private — любой, в channel — админ
        chat_info = await db_get_chat_info(chat_id)
        if not chat_info:
            return

        if chat_info["type"] in ("group", "channel"):
            if member.get("role") != "admin":
                await send_json(websocket, {
                    "type": "error",
                    "text": "Только админ может закреплять"
                })
                return

        ok = await db_pin_message(chat_id, message_id, user["id"])
        if not ok:
            await send_json(websocket, {
                "type": "error",
                "text": "Не удалось закрепить"
            })
            return

        pinned = await db_get_pinned_message(chat_id)
        await broadcast_chat(chat_id, {
            "type": "message_pinned",
            "chat_id": chat_id,
            "pinned": pinned,
        })
    else:
        await db_unpin_message(chat_id, message_id)
        pinned = await db_get_pinned_message(chat_id)
        await broadcast_chat(chat_id, {
            "type": "message_unpinned",
            "chat_id": chat_id,
            "pinned": pinned,
        })


# ============================================================
#  ОБРАБОТЧИК: ПОИСК СООБЩЕНИЙ
# ============================================================
async def handle_search_messages(websocket, user, msg):
    query = str(msg.get("query", "")).strip()
    chat_id = msg.get("chat_id")  # опционально
    limit = min(int(msg.get("limit", 50)), 200)

    if not query:
        return

    try:
        results = await db_search_messages(
            user_id=user["id"],
            query=query,
            chat_id=chat_id,
            limit=limit,
        )
    except Exception as e:
        print(f"search_messages error: {e}")
        results = []

    await send_json(websocket, {
        "type": "search_results",
        "query": query,
        "chat_id": chat_id,
        "results": results,
    })


# ============================================================
#  ОБРАБОТЧИК: ПОИСК ПОЛЬЗОВАТЕЛЕЙ
# ============================================================
async def handle_search_users(websocket, user, msg):
    query = str(msg.get("query", "")).strip()
    if not query:
        return

    try:
        results = await db_search_users(query, exclude_id=user["id"])
    except Exception as e:
        print(f"search_users error: {e}")
        results = []

    await send_json(websocket, {
        "type": "user_search_results",
        "query": query,
        "users": [user_public(u) for u in results],
    })


# ============================================================
#  ОБРАБОТЧИК: ПОИСК КАНАЛОВ
# ============================================================
async def handle_search_channels(websocket, user, msg):
    query = str(msg.get("query", "")).strip()

    try:
        if query:
            results = await db_search_public_channels(query)
        else:
            results = await db_get_public_channels()
    except Exception as e:
        print(f"search_channels error: {e}")
        results = []

    await send_json(websocket, {
        "type": "channel_search_results",
        "query": query,
        "channels": results,
    })


# === КОНЕЦ ЧАСТИ 20/25 ===
# ============================================================
#  ОБРАБОТЧИК: ОБНОВЛЕНИЕ ПРОФИЛЯ
# ============================================================
async def handle_update_profile(websocket, user, msg):
    display_name = msg.get("display_name")
    bio = msg.get("bio")
    avatar_data = msg.get("avatar_data")
    language = msg.get("language")
    theme = msg.get("theme")
    email = msg.get("email")

    # Валидация
    if display_name is not None:
        display_name = str(display_name).strip()[:MAX_DISPLAY_NAME]
        if not display_name:
            display_name = None

    if bio is not None:
        bio = str(bio)[:MAX_BIO_LENGTH]

    if language is not None and language not in ("ru", "en"):
        language = None

    if theme is not None and theme not in ("dark", "light"):
        theme = None

    if avatar_data is not None:
        size = get_file_size_base64(avatar_data)
        if size > MAX_AVATAR_SIZE:
            await send_json(websocket, {
                "type": "error",
                "text": f"Аватар слишком большой (макс {MAX_AVATAR_SIZE // (1024*1024)} МБ)"
            })
            return

    # Обновляем в БД
    await db_update_user(
        user_id=user["id"],
        display_name=display_name,
        bio=bio,
        avatar_data=avatar_data,
        language=language,
        theme=theme,
        email=email,
    )

    # Обновляем в памяти
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

    # Обновляем всех сокетов этого юзера
    clients[websocket] = user

    # Загружаем свежие данные
    updated = await db_get_user_by_id(user["id"])
    if not updated:
        return

    pub = user_public(updated)

    # Отправляем себе (на все устройства)
    await send_to_user(user["id"], {
        "type": "profile_updated",
        "user": pub,
    })

    # Уведомляем всех, кто в чатах с нами — чтобы обновили аватар/имя
    await broadcast_profile_change(user["id"], pub)


async def broadcast_profile_change(user_id: int, pub: dict):
    """Уведомить всех, кто имеет общий чат с юзером, о смене профиля."""
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT cm2.user_id FROM chat_members cm1
                    JOIN chat_members cm2 ON cm2.chat_id = cm1.chat_id
                    WHERE cm1.user_id = %s AND cm2.user_id != %s
                """, (user_id, user_id))
                member_ids = [r["user_id"] for r in cur.fetchall()]

        if not member_ids:
            return

        notif = {
            "type": "user_updated",
            "user": pub,
        }
        await send_to_users(member_ids, notif)
    except Exception as e:
        print(f"broadcast_profile_change error: {e}")


# ============================================================
#  ОБРАБОТЧИК: СМЕНА @USERNAME
# ============================================================
async def handle_change_username(websocket, user, msg):
    new_username = str(msg.get("username", "")).strip().lower()[:MAX_USERNAME]

    if not new_username:
        await send_json(websocket, {
            "type": "error",
            "text": "Введите новый @username"
        })
        return

    if not validate_username(new_username):
        await send_json(websocket, {
            "type": "error",
            "text": "Только латиница, цифры, _ (3-20 символов)"
        })
        return

    if new_username == user["name"]:
        await send_json(websocket, {
            "type": "error",
            "text": "Это твой текущий @username"
        })
        return

    old_username = user["name"]

    ok = await db_change_username(user["id"], new_username)
    if not ok:
        await send_json(websocket, {
            "type": "error",
            "text": "Этот @username уже занят"
        })
        return

    # Обновляем в памяти
    user["name"] = new_username
    clients[websocket] = user

    # Обновляем профиль
    updated = await db_get_user_by_id(user["id"])
    pub = user_public(updated) if updated else user_public(user)

    # Отправляем себе (на все устройства)
    await send_to_user(user["id"], {
        "type": "username_changed",
        "new_username": new_username,
        "old_username": old_username,
        "user": pub,
    })

    # Уведомляем всех в общих чатах
    await broadcast_profile_change(user["id"], pub)


# ============================================================
#  ОБРАБОТЧИК: PUSH-ПОДПИСКА
# ============================================================
async def handle_push_subscribe(websocket, user, msg):
    sub = msg.get("subscription", {})
    endpoint = sub.get("endpoint")
    keys = sub.get("keys", {})
    p256dh = keys.get("p256dh", "")
    auth = keys.get("auth", "")

    if not endpoint:
        return

    try:
        await db_save_push_subscription(
            user_id=user["id"],
            endpoint=endpoint,
            p256dh=p256dh,
            auth=auth,
        )
        print(f"📱 Push подписка сохранена для {user['name']}")
    except Exception as e:
        print(f"push_subscribe error: {e}")


# ============================================================
#  ОБРАБОТЧИК: ЗВОНКИ (WebRTC сигналинг)
# ============================================================
async def handle_call_signal(websocket, user, msg):
    """Прокидывание WebRTC-сигналов между юзерами."""
    target_id = msg.get("target_id")
    if not target_id or target_id == user["id"]:
        return

    # Проверка что целевой юзер существует
    target = await db_get_user_by_id(target_id)
    if not target:
        await send_json(websocket, {
            "type": "call_reject",
            "reason": "user_not_found",
        })
        return

    # Для call_offer — проверяем что это личный чат
    mtype = msg.get("type")
    if mtype == "call_offer":
        # Проверим что чат — private между ними
        chat_id = msg.get("chat_id")
        if chat_id:
            chat_info = await db_get_chat_info(chat_id)
            if not chat_info or chat_info["type"] != "private":
                await send_json(websocket, {
                    "type": "call_reject",
                    "reason": "calls_only_private",
                })
                return

        # Проверка что целевой юзер не занят (уже в звонке)
        # Пока не отслеживаем — TODO

    # Прокидываем сигнал получателю
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

    # Если не доставлено и это call_offer — уведомляем инициатора
    if not delivered and mtype == "call_offer":
        await send_json(websocket, {
            "type": "call_offline",
            "target_id": target_id,
        })


# === КОНЕЦ ЧАСТИ 21/25 ===
# ============================================================
#  ОБРАБОТЧИК: СМЕНА EMAIL
# ============================================================
async def handle_change_email(websocket, user, msg):
    """Отправляет код для подтверждения нового email."""
    new_email = str(msg.get("email", "")).strip().lower()

    if not new_email or "@" not in new_email or "." not in new_email:
        await send_json(websocket, {
            "type": "error",
            "text": "Некорректный email"
        })
        return

    if len(new_email) > 120:
        await send_json(websocket, {
            "type": "error",
            "text": "Email слишком длинный"
        })
        return

    if not EMAILS_ENABLED:
        await send_json(websocket, {
            "type": "error",
            "text": "Email-подтверждение недоступно"
        })
        return

    # Антиспам
    recent = await db_count_user_codes(user["id"], "change_email")
    if recent >= 5:
        await send_json(websocket, {
            "type": "error",
            "text": "Слишком много попыток, попробуй позже"
        })
        return

    # Генерируем код
    code = generate_verification_code()
    await db_save_code(user["id"], new_email, code, "change_email", ttl_minutes=15)

    # Отправляем письмо
    html = make_code_email_html(
        code,
        user["display_name"] or user["name"],
        "change_email",
    )
    sent = await send_email(new_email, "🔥 Blaze — подтверждение email", html)

    if sent:
        await send_json(websocket, {
            "type": "email_code_sent",
            "purpose": "change_email",
            "email": mask_email(new_email),
            "text": f"Код отправлен на {mask_email(new_email)}",
        })
    else:
        await send_json(websocket, {
            "type": "error",
            "text": "Не удалось отправить письмо"
        })


async def handle_verify_email_change(websocket, user, msg):
    """Проверяет код и меняет email."""
    code = str(msg.get("code", "")).strip()
    new_email = str(msg.get("email", "")).strip().lower()

    if not is_code_valid_format(code) or not new_email:
        await send_json(websocket, {
            "type": "error",
            "text": "Код и email обязательны"
        })
        return

    ok = await db_check_code(user["id"], code, "change_email")
    if not ok:
        await send_json(websocket, {
            "type": "error",
            "text": "Неверный или истёкший код"
        })
        return

    # Проверка что email не занят
    existing = await db_get_user_by_email(new_email)
    if existing and existing["id"] != user["id"]:
        await send_json(websocket, {
            "type": "error",
            "text": "Этот email уже используется"
        })
        return

    # Меняем
    await db_set_user_email(user["id"], new_email)

    # Обновляем профиль
    updated = await db_get_user_by_id(user["id"])
    pub = user_public(updated) if updated else user_public(user)

    await send_to_user(user["id"], {
        "type": "profile_updated",
        "user": pub,
        "text": "Email успешно изменён",
    })


# ============================================================
#  ОБРАБОТЧИК: ОТПРАВКА КОДА ПОВТОРНО
# ============================================================
async def handle_resend_code(websocket, user, msg):
    """Переотправка кода для входа или смены email."""
    purpose = msg.get("purpose", "login")
    user_id = msg.get("user_id")  # для login — чужой, для change_email — свой

    if purpose == "login":
        # Пользователь не аутентифицирован (login flow)
        if not user_id:
            return

        target_user = await db_get_user_by_id(user_id)
        if not target_user:
            return

        email = target_user.get("email")
        if not email:
            return

        # Антиспам
        recent = await db_count_user_codes(user_id, "login")
        if recent >= 5:
            await send_json(websocket, {
                "type": "error",
                "text": "Слишком много попыток"
            })
            return

        code = generate_verification_code()
        await db_save_code(user_id, email, code, "login", ttl_minutes=10)

        html = make_code_email_html(
            code,
            target_user.get("display_name") or target_user["username"],
            "login",
        )
        sent = await send_email(email, "🔥 Blaze — новый код", html)

        if sent:
            await send_json(websocket, {
                "type": "email_code_resent",
                "text": f"Новый код отправлен на {mask_email(email)}"
            })
        else:
            await send_json(websocket, {
                "type": "error",
                "text": "Не удалось отправить письмо"
            })

    elif purpose == "change_email":
        # Пользователь аутентифицирован
        if not user:
            return
        # Повторно используем handle_change_email
        await handle_change_email(websocket, user, msg)


# ============================================================
#  ЕЖЕДНЕВНАЯ ОЧИСТКА (не обязательна, но полезна)
# ============================================================
async def cleanup_old_codes():
    """Удаляет старые коды верификации."""
    try:
        await db_delete_old_codes()
    except Exception as e:
        print(f"cleanup_old_codes error: {e}")


async def periodic_cleanup():
    """Периодическая очистка каждые 6 часов."""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            await cleanup_old_codes()
            print("🧹 Очистка старых кодов завершена")
        except Exception as e:
            print(f"periodic_cleanup error: {e}")


# === КОНЕЦ ЧАСТИ 22/25 ===
# ============================================================
#  MAIN — ТОЧКА ВХОДА
# ============================================================
async def main():
    print("=" * 60)
    print("🔥 BLAZE MESSENGER v3.0")
    print("=" * 60)

    # Инициализация БД
    await init_db()

    # Проверка конфигов
    print(f"📦 DATABASE_URL: {'✅' if DATABASE_URL else '❌ не задан'}")
    print(f"🔑 JWT_SECRET: {'✅' if JWT_SECRET and JWT_SECRET != 'change-me-in-production' else '⚠️ по умолчанию'}")
    print(f"🔐 Шифрование БД: {'✅' if cipher else '⚠️ выключено'}")
    print(f"📧 Email (SMTP): {'✅' if EMAILS_ENABLED else '⚠️ выключен'}")
    if EMAILS_ENABLED:
        print(f"   SMTP: {SMTP_USER} via {SMTP_HOST}:{SMTP_PORT}")

    # Порт
    port = int(os.environ.get("PORT", 8765))
    print(f"🌐 Порт: {port}")
    print("-" * 60)
    print(f"🚀 Запуск сервера...")
    print(f"📦 Модули:")
    print(f"   • Чаты (личные, группы, каналы)")
    print(f"   • Сообщения (текст, файлы, голосовые, огоньки)")
    print(f"   • Реакции, reply, edit, delete, forward")
    print(f"   • Профили, @username, аватары")
    print(f"   • Поиск (чаты, юзеры, каналы)")
    print(f"   • Push-подписки")
    print(f"   • Звонки (WebRTC сигналинг)")
    print(f"   • Защита: bcrypt, JWT, шифрование БД")
    print("=" * 60)

    # Фоновая задача очистки (не критично если упадёт)
    cleanup_task = asyncio.create_task(periodic_cleanup())

    try:
        async with websockets.serve(
            handler,
            "0.0.0.0",
            port,
            process_request=process_request,
            max_size=20 * 1024 * 1024,   # 20 МБ входные данные
            ping_interval=25,
            ping_timeout=25,
            close_timeout=10,
            max_queue=32,
        ):
            await asyncio.Future()  # вечно
    except asyncio.CancelledError:
        print("⚠️ Сервер остановлен")
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except (asyncio.CancelledError, Exception):
            pass
        print("👋 Сервер завершил работу")


# ============================================================
#  ТОЧКА ВХОДА
# ============================================================
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


# === КОНЕЦ ЧАСТИ 23/25 ===
# ============================================================
#  ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ
# ============================================================
async def handle_join_by_invite(websocket, user, msg):
    """Присоединение к группе/каналу по инвайт-коду."""
    code = str(msg.get("code", "")).strip()
    if not code:
        await send_json(websocket, {
            "type": "error",
            "text": "Укажи код приглашения"
        })
        return

    result = await db_join_by_invite_code(code, user["id"])
    if not result:
        await send_json(websocket, {
            "type": "error",
            "text": "Неверный или истёкший код"
        })
        return

    chat_id = result["id"]

    # Обновляем список чатов
    await refresh_chats_list([user["id"]])

    # Уведомляем остальных о новом участнике
    members = await db_get_chat_members(chat_id)
    await broadcast_chat(chat_id, {
        "type": "chat_members_updated",
        "chat_id": chat_id,
        "members": [dict(m) for m in members],
    })

    await broadcast_chat(chat_id, {
        "type": "system",
        "chat_id": chat_id,
        "text": f"{user['display_name']} присоединился к чату",
        "time": now_str(),
    })

    await send_json(websocket, {
        "type": "chat_opened",
        "chat_id": chat_id,
    })


async def handle_get_stats(websocket, user, msg):
    """Отправляет статистику приложения."""
    try:
        stats = await db_get_total_stats()
        await send_json(websocket, {
            "type": "stats",
            "stats": stats,
        })
    except Exception as e:
        print(f"get_stats error: {e}")


async def handle_create_invite(websocket, user, msg):
    """Создаёт/обновляет инвайт-код для группы."""
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    is_admin = await db_is_admin(chat_id, user["id"])
    if not is_admin:
        await send_json(websocket, {
            "type": "error",
            "text": "Только админ"
        })
        return

    code = await db_generate_invite_code(chat_id)
    if code:
        await send_json(websocket, {
            "type": "invite_created",
            "chat_id": chat_id,
            "code": code,
        })


async def handle_get_pinned(websocket, user, msg):
    """Запрос закреплённого сообщения в чате."""
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    member = await db_check_member(chat_id, user["id"])
    if not member:
        return

    pinned = await db_get_pinned_message(chat_id)
    await send_json(websocket, {
        "type": "pinned_message",
        "chat_id": chat_id,
        "pinned": pinned,
    })


async def handle_clear_history(websocket, user, msg):
    """Очистка истории чата у себя."""
    chat_id = msg.get("chat_id")
    if not chat_id:
        return

    await db_clear_chat_history(chat_id, user["id"])

    await send_json(websocket, {
        "type": "history_cleared",
        "chat_id": chat_id,
    })

    # Обновить список чатов у себя
    await refresh_chats_list([user["id"]])


async def handle_send_test_email(websocket, user, msg):
    """Отправка тестового письма (для проверки SMTP)."""
    if not EMAILS_ENABLED:
        await send_json(websocket, {
            "type": "error",
            "text": "SMTP не настроен"
        })
        return

    # Куда отправлять
    to_email = msg.get("email") or user.get("email")
    if not to_email:
        await send_json(websocket, {
            "type": "error",
            "text": "Укажи email"
        })
        return

    html = make_test_email_html(user["display_name"] or user["name"])
    sent = await send_email(to_email, "🔥 Blaze — тестовое письмо", html)

    if sent:
        await send_json(websocket, {
            "type": "test_email_sent",
            "email": mask_email(to_email),
        })
    else:
        await send_json(websocket, {
            "type": "error",
            "text": "Не удалось отправить"
        })


# ============================================================
#  РАСШИРЕННАЯ МАРШРУТИЗАЦИЯ (дополнение к ЧАСТИ 16)
# ============================================================
async def route_message_extended(websocket, user, mtype, msg):
    """Дополнительные маршруты."""
    if mtype == "join_by_invite":
        await handle_join_by_invite(websocket, user, msg)
    elif mtype == "get_stats":
        await handle_get_stats(websocket, user, msg)
    elif mtype == "create_invite":
        await handle_create_invite(websocket, user, msg)
    elif mtype == "get_pinned":
        await handle_get_pinned(websocket, user, msg)
    elif mtype == "clear_history":
        await handle_clear_history(websocket, user, msg)
    elif mtype == "send_test_email":
        await handle_send_test_email(websocket, user, msg)
    elif mtype == "change_email":
        await handle_change_email(websocket, user, msg)
    elif mtype == "verify_email_change":
        await handle_verify_email_change(websocket, user, msg)
    elif mtype == "resend_code":
        await handle_resend_code(websocket, user, msg)
    elif mtype == "change_username":
        await handle_change_username(websocket, user, msg)
    else:
        await send_json(websocket, {
            "type": "error",
            "text": f"Неизвестный тип: {mtype}"
        })


# ============================================================
#  ЭКСПОРТ ДЛЯ RENDER / Тестов
# ============================================================
__all__ = [
    # Точка входа
    "main",
    "handler",
    "process_request",
    # БД
    "init_db",
    "db_conn",
    # Users
    "db_create_user",
    "db_get_user",
    "db_get_user_by_id",
    "db_get_user_by_email",
    "db_get_all_users",
    "db_search_users",
    "db_update_user",
    "db_change_username",
    "db_update_last_seen",
    "db_set_online",
    # Chats
    "db_get_global_chat",
    "db_add_user_to_global_chat",
    "db_get_or_create_private_chat",
    "db_get_chat_info",
    "db_get_chat_members",
    "db_get_chat_member_ids",
    "db_check_member",
    "db_is_admin",
    "db_mark_chat_read",
    "db_get_user_chats",
    # Groups
    "db_create_group",
    "db_add_members",
    "db_remove_member",
    "db_set_member_role",
    "db_update_group",
    "db_leave_chat",
    "db_delete_group",
    "db_generate_invite_code",
    "db_join_by_invite_code",
    # Channels
    "db_create_channel",
    "db_update_channel",
    "db_subscribe_to_channel",
    "db_unsubscribe_from_channel",
    "db_can_post_to_chat",
    "db_get_channel_subscribers_count",
    "db_get_public_channels",
    "db_search_public_channels",
    "db_get_admin_channels",
    # Messages
    "db_save_message",
    "db_get_chat_history",
    "db_get_message_by_id",
    "db_get_last_message",
    "db_get_messages_around",
    "db_edit_message",
    "db_delete_message",
    "db_delete_message_admin",
    # Reactions
    "db_toggle_reaction",
    "db_get_message_reactions",
    "db_get_my_reactions",
    "db_clear_reactions",
    # Reads
    "db_mark_message_read",
    "db_get_read_count",
    "db_get_reads_for_messages",
    "db_get_message_readers",
    # Pins
    "db_pin_message",
    "db_unpin_message",
    "db_get_pinned_message",
    "db_pin_chat",
    "db_unpin_chat",
    "db_get_pinned_chat_ids",
    # Archive
    "db_archive_chat",
    "db_unarchive_chat",
    "db_get_archived_chat_ids",
    # Forward
    "db_forward_message",
    "db_forward_multiple",
    # Search
    "db_search_messages",
    "db_search_in_chat",
    # Mute
    "db_mute_chat",
    "db_is_chat_muted",
    # Delete chat
    "db_delete_chat_for_user",
    "db_clear_chat_history",
    # Stats
    "db_get_total_stats",
    # Email
    "db_save_code",
    "db_check_code",
    "db_get_active_code",
    "db_delete_old_codes",
    "db_count_user_codes",
    "send_email",
    "send_email_sync",
    "make_code_email_html",
    "make_welcome_email_html",
    "make_password_reset_email_html",
    "make_test_email_html",
    # Encryption
    "encrypt_text",
    "decrypt_text",
    # Utils
    "user_public",
    "chat_public",
    "now_str",
    "now_iso",
    "generate_code",
    "generate_verification_code",
    "validate_username",
    "validate_display_name",
    "mask_email",
    "get_file_size_base64",
    "is_code_valid_format",
]


# === КОНЕЦ ЧАСТИ 24/25 ===
# ============================================================
#  ФИНАЛЬНАЯ ОБЁРТКА — импорт всех обработчиков
# ============================================================
#  Примечание:
#  Все обработчики определены выше по тексту файла.
#  Эта часть нужна для чистоты импорта и порядка.
#
#  Если ты хочешь разбить server.py на несколько файлов —
#  используй структуру:
#
#  server/
#    ├── __init__.py
#    ├── db.py           (все db_* функции)
#    ├── email.py        (SMTP + шаблоны)
#    ├── crypto.py       (шифрование)
#    ├── ws.py           (WebSocket helpers)
#    ├── auth.py         (handle_auth)
#    ├── handlers.py     (все handle_*)
#    └── main.py         (точка входа)
#
#  Но для простоты — держим всё в одном файле server.py.
# ============================================================


# ============================================================
#  ДОПОЛНИТЕЛЬНАЯ ЗАЩИТА — ограничение размера соединений
# ============================================================
MAX_CONNECTIONS = 500       # общий лимит
MAX_CONNECTIONS_PER_USER = 10  # мультиустройство

async def check_connection_limit() -> bool:
    """Проверка общего лимита соединений."""
    return len(clients) < MAX_CONNECTIONS


async def check_user_connection_limit(user_id: int) -> bool:
    """Проверка лимита устройств одного юзера."""
    if user_id not in user_sockets:
        return True
    return len(user_sockets[user_id]) < MAX_CONNECTIONS_PER_USER


# ============================================================
#  ЗАЩИТА ОТ ФЛУДА (простой rate-limit)
# ============================================================
_rate_limit_store: Dict[int, List[float]] = {}

RATE_LIMIT_WINDOW = 60      # секунд
RATE_LIMIT_MAX_MESSAGES = 60  # сообщений за окно


def check_rate_limit(user_id: int) -> bool:
    """True если можно отправлять, False если превышен лимит."""
    now = datetime.datetime.now().timestamp()
    times = _rate_limit_store.get(user_id, [])
    # Чистим старые
    times = [t for t in times if now - t < RATE_LIMIT_WINDOW]
    if len(times) >= RATE_LIMIT_MAX_MESSAGES:
        return False
    times.append(now)
    _rate_limit_store[user_id] = times
    return True


# ============================================================
#  ЛОГИРОВАНИЕ СТАРТА (для Render)
# ============================================================
def log_startup_banner():
    """Красивый баннер в логах."""
    banner = """
    ╔══════════════════════════════════════════════════════════╗
    ║                                                          ║
    ║              🔥 BLAZE MESSENGER v3.0 🔥                  ║
    ║                                                          ║
    ║      Modern messenger with everything you need           ║
    ║                                                          ║
    ║   • Личные чаты, группы, каналы                          ║
    ║   • Текст, файлы, голосовые, огоньки                     ║
    ║   • Реакции, reply, edit, delete, forward                ║
    ║   • Профили, @username, аватары                          ║
    ║   • Поиск, закрепы, архив, мьют                          ║
    ║   • Push-уведомления, WebRTC-звонки                      ║
    ║   • bcrypt + JWT + шифрование БД                         ║
    ║                                                          ║
    ║                 Made with 🔥 by Blaze                    ║
    ║                                                          ║
    ╚══════════════════════════════════════════════════════════╝
    """
    print(banner)


# ============================================================
#  HEALTH-CHECK МАРШРУТ ДЛЯ КРАСИВОГО ОТВЕТА В БРАУЗЕРЕ
# ============================================================
async def process_request(path, request_headers):
    """Отвечает на HTTP-запросы (health-check + красивая страница)."""
    if "Upgrade" not in request_headers.get("Connection", ""):
        # Проверяем что это корень
        if path == "/" or path == "":
            html = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Blaze Messenger API</title>
<style>
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0a0a0a;
    color: #fff;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    margin: 0;
    background-image:
      radial-gradient(circle at 0% 0%, rgba(255,107,0,0.15) 0%, transparent 40%),
      radial-gradient(circle at 100% 100%, rgba(229,57,53,0.15) 0%, transparent 40%);
  }
  .box {
    text-align: center;
    padding: 40px 60px;
    background: rgba(22,22,28,0.85);
    border-radius: 24px;
    backdrop-filter: blur(20px);
    border: 1px solid rgba(255,255,255,0.08);
    box-shadow: 0 30px 90px rgba(0,0,0,0.7), 0 0 100px rgba(255,107,0,0.15);
  }
  .flame { font-size: 72px; line-height: 1; filter: drop-shadow(0 0 20px rgba(255,107,0,0.6)); }
  h1 {
    background: linear-gradient(135deg, #ff6b00, #e53935);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 32px;
    margin: 12px 0 8px;
    letter-spacing: -1px;
  }
  .status {
    color: #4ade80;
    font-size: 14px;
    font-weight: 600;
    margin-top: 20px;
  }
  .status::before {
    content: "●";
    margin-right: 8px;
    animation: pulse 2s infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
  }
  p { color: #a0a0aa; font-size: 14px; margin: 6px 0; }
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

        # Остальные пути — простой текст
        return http.HTTPStatus.OK, [], b"Blaze Messenger v3.0 is running\n"

    return None


# ============================================================
#  ФИНАЛЬНАЯ ПРОВЕРКА ЦЕЛОСТНОСТИ
# ============================================================
def sanity_check():
    """Проверяет что все необходимые компоненты на месте."""
    print("🔍 Проверка целостности сервера...")

    checks = {
        "DATABASE_URL": bool(DATABASE_URL),
        "JWT_SECRET": bool(JWT_SECRET),
        "cipher (шифрование)": bool(cipher),
        "EMAILS_ENABLED": EMAILS_ENABLED,
    }

    for name, ok in checks.items():
        status = "✅" if ok else "⚠️"
        print(f"   {status} {name}")

    if not DATABASE_URL:
        print("   ⚠️  Сервер запустится в гостевом режиме (без БД)")

    print("✅ Проверка завершена\n")


# ============================================================
#  ЗАПУСК
# ============================================================
if __name__ == "__main__":
    log_startup_banner()
    sanity_check()

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
#  Blaze Messenger v3.0 — полная версия
# ════════════════════════════════════════════════════════════