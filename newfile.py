import asyncio
import websockets
import json
import datetime
import os
import http
import asyncpg
import bcrypt
import jwt
import base64
import uuid
from typing import Optional

# ============ КОНФИГ ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGO = "HS256"
MAX_HISTORY = 100
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB

# ============ БАЗА ДАННЫХ ============
db_pool: Optional[asyncpg.Pool] = None

async def init_db():
    global db_pool
    if not DATABASE_URL:
        print("⚠️  DATABASE_URL не задан — работаю без истории")
        return
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                avatar_color TEXT DEFAULT '#f97316',
                created_at TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender_id INTEGER REFERENCES users(id),
                sender_name TEXT NOT NULL,
                text TEXT,
                file_data TEXT,
                file_name TEXT,
                file_type TEXT,
                reply_to INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reads (
                message_id INTEGER REFERENCES messages(id),
                user_id INTEGER REFERENCES users(id),
                read_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (message_id, user_id)
            );
        """)
    print("✅ База данных инициализирована")

async def db_create_user(username, password):
    if not db_pool: return None
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id, username, avatar_color",
                username, pw_hash
            )
            return dict(row)
    except asyncpg.UniqueViolationError:
        return None

async def db_get_user(username):
    if not db_pool: return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, username, password_hash, avatar_color FROM users WHERE username=$1",
            username
        )
        return dict(row) if row else None

async def db_save_message(sender_id, sender_name, text, file_data=None,
                          file_name=None, file_type=None, reply_to=None):
    if not db_pool: return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO messages (sender_id, sender_name, text, file_data, file_name, file_type, reply_to)
            VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, created_at
        """, sender_id, sender_name, text, file_data, file_name, file_type, reply_to)
        return dict(row) if row else None

async def db_get_history(limit=MAX_HISTORY):
    if not db_pool: return []
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, sender_name, text, file_data, file_name, file_type,
                   reply_to, created_at
            FROM messages ORDER BY id DESC LIMIT $1
        """, limit)
        return [dict(r) for r in reversed(rows)]

async def db_mark_read(message_id, user_id):
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO reads (message_id, user_id) VALUES ($1, $2)
            ON CONFLICT DO NOTHING
        """, message_id, user_id)

async def db_get_reads(message_ids):
    if not db_pool or not message_ids: return {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT message_id, COUNT(*) as cnt FROM reads
            WHERE message_id = ANY($1::int[]) GROUP BY message_id
        """, message_ids)
        return {r["message_id"]: r["cnt"] for r in rows}

async def db_update_last_seen(user_id):
    if not db_pool: return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_seen=NOW() WHERE id=$1", user_id)

# ============ WEBSOCKET СЕРВЕР ============
clients = {}  # {websocket: {"id": int, "name": str, "color": str}}

async def process_request(path, request_headers):
    if "Upgrade" not in request_headers.get("Connection", ""):
        return http.HTTPStatus.OK, [], b"Blaze Messenger is running\n"
    return None

async def broadcast(message: dict, exclude=None):
    if not clients: return
    data = json.dumps(message, ensure_ascii=False)
    tasks = [send_safe(ws, data) for ws in list(clients.keys())
             if ws != exclude and ws.open]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def send_safe(ws, data):
    try:
        await ws.send(data)
    except Exception:
        pass

async def broadcast_users():
    users = [{"name": c["name"], "color": c["color"]} for c in clients.values()]
    await broadcast({"type": "users", "users": users})

def now_str():
    return datetime.datetime.now().strftime("%H:%M")

async def handle_auth(websocket, first_msg):
    """Обработка логина/регистрации"""
    action = first_msg.get("action")
    username = first_msg.get("username", "").strip()[:20]
    password = first_msg.get("password", "")

    if not username or not password:
        await websocket.send(json.dumps({
            "type": "auth_error", "text": "Логин и пароль обязательны"
        }, ensure_ascii=False))
        return None

    if action == "register":
        user = await db_create_user(username, password)
        if not user:
            await websocket.send(json.dumps({
                "type": "auth_error", "text": "Такой пользователь уже есть"
            }, ensure_ascii=False))
            return None
    elif action == "login":
        user = await db_get_user(username)
        if not user or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
            await websocket.send(json.dumps({
                "type": "auth_error", "text": "Неверный логин или пароль"
            }, ensure_ascii=False))
            return None
    else:
        # Гостевой режим (для отладки без БД)
        if not db_pool:
            user = {"id": None, "username": username, "avatar_color": "#f97316"}
        else:
            await websocket.send(json.dumps({
                "type": "auth_error", "text": "Укажите action: login или register"
            }, ensure_ascii=False))
            return None

    token = jwt.encode({
        "user_id": user["id"], "username": user["username"],
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=30)
    }, JWT_SECRET, algorithm=JWT_ALGO)

    return {
        "id": user["id"],
        "name": user["username"],
        "color": user.get("avatar_color", "#f97316"),
        "token": token
    }

async def handler(websocket):
    """Главный обработчик подключения"""
    user = None
    try:
        raw = await websocket.recv()
        first = json.loads(raw)
        user = await handle_auth(websocket, first)
        if not user:
            await websocket.close()
            return

        # Уникальность ника среди онлайн
        base_name = user["name"]
        final_name = base_name
        i = 1
        while final_name in [c["name"] for c in clients.values()]:
            i += 1
            final_name = f"{base_name}_{i}"
        user["name"] = final_name

        clients[websocket] = user
        print(f"[+] {user['name']} (id={user['id']}). Всего: {len(clients)}")

        # Приветствие + токен
        await websocket.send(json.dumps({
            "type": "auth_ok",
            "user": {"id": user["id"], "name": user["name"], "color": user["color"]},
            "token": user["token"]
        }, ensure_ascii=False))

        # История
        history = await db_get_history()
        msg_ids = [m["id"] for m in history]
        reads = await db_get_reads(msg_ids)
        for m in history:
            m["created_at"] = m["created_at"].isoformat()
            m["read_by"] = reads.get(m["id"], 0)
        await websocket.send(json.dumps({
            "type": "history", "messages": history
        }, ensure_ascii=False))

        # Уведомить остальных
        await broadcast({
            "type": "system",
            "text": f"{user['name']} присоединился",
            "time": now_str()
        }, exclude=websocket)
        await broadcast_users()

        # Основной цикл
        async for message in websocket:
            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")

            # === ОБЫЧНОЕ СООБЩЕНИЕ ===
            if mtype == "message":
                text = str(msg.get("text", ""))[:4000]
                file_data = msg.get("file_data")
                file_name = msg.get("file_name")
                file_type = msg.get("file_type")
                reply_to = msg.get("reply_to")

                if not text.strip() and not file_data:
                    continue
                if file_data and len(file_data) > MAX_FILE_SIZE * 1.4:
                    await websocket.send(json.dumps({
                        "type": "error", "text": "Файл слишком большой (макс 5 МБ)"
                    }, ensure_ascii=False))
                    continue

                saved = await db_save_message(
                    user["id"], user["name"], text,
                    file_data, file_name, file_type, reply_to
                )
                msg_id = saved["id"] if saved else 0
                created = saved["created_at"].isoformat() if saved else datetime.datetime.now().isoformat()

                await broadcast({
                    "type": "message",
                    "id": msg_id,
                    "sender_id": user["id"],
                    "nickname": user["name"],
                    "color": user["color"],
                    "text": text,
                    "file_data": file_data,
                    "file_name": file_name,
                    "file_type": file_type,
                    "reply_to": reply_to,
                    "time": now_str(),
                    "created_at": created,
                    "read_by": 0
                })

            # === ИНДИКАТОР «ПЕЧАТАЕТ...» ===
            elif mtype == "typing":
                await broadcast({
                    "type": "typing",
                    "nickname": user["name"]
                }, exclude=websocket)

            # === ПРОЧИТАНО ===
            elif mtype == "read":
                msg_id = msg.get("message_id")
                if msg_id and user["id"]:
                    await db_mark_read(msg_id, user["id"])
                    await broadcast({
                        "type": "read",
                        "message_id": msg_id,
                        "user_id": user["id"]
                    }, exclude=websocket)

            # === PING (keep-alive) ===
            elif mtype == "ping":
                await websocket.send(json.dumps({"type": "pong"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[!] Ошибка: {e}")
        import traceback; traceback.print_exc()
    finally:
        if websocket in clients:
            name = clients[websocket]["name"]
            uid = clients[websocket]["id"]
            del clients[websocket]
            print(f"[-] {name}. Всего: {len(clients)}")
            if uid:
                await db_update_last_seen(uid)
            await broadcast({
                "type": "system",
                "text": f"{name} покинул чат",
                "time": now_str()
            })
            await broadcast_users()

async def main():
    await init_db()
    port = int(os.environ.get("PORT", 8765))
    print(f"🚀 Blaze Messenger на порту {port}")
    async with websockets.serve(
        handler, "0.0.0.0", port,
        process_request=process_request,
        max_size=10 * 1024 * 1024,
        ping_interval=20,
        ping_timeout=20
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())