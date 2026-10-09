import asyncio
import websockets
import json
import datetime
import os
import http

clients = {}

async def process_request(path, request_headers):
    """Health-check для облачных платформ (Render/Railway требуют HTTP-ответ)"""
    if "Upgrade" not in request_headers.get("Connection", ""):
        return http.HTTPStatus.OK, [], b"OK\n"
    return None

async def broadcast(message: dict, exclude=None):
    if not clients:
        return
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

async def handler(websocket):
    nickname = None
    try:
        raw = await websocket.recv()
        reg = json.loads(raw)
        nickname = reg.get("nickname", "Аноним").strip()[:20] or "Аноним"

        base, i = nickname, 1
        while nickname in clients.values():
            i += 1
            nickname = f"{base}_{i}"

        clients[websocket] = nickname
        print(f"[+] {nickname}. Всего: {len(clients)}")

        now = lambda: datetime.datetime.now().strftime("%H:%M")

        await websocket.send(json.dumps({
            "type": "system",
            "text": f"Добро пожаловать, {nickname}!",
            "time": now()
        }, ensure_ascii=False))

        await broadcast({"type": "system",
                         "text": f"{nickname} присоединился",
                         "time": now()}, exclude=websocket)
        await broadcast_users()

        async for message in websocket:
            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "message":
                text = str(msg.get("text", ""))[:2000]
                if not text.strip():
                    continue
                await broadcast({
                    "type": "message",
                    "nickname": nickname,
                    "text": text,
                    "time": now()
                })
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[!] {e}")
    finally:
        if websocket in clients:
            del clients[websocket]
            print(f"[-] {nickname}. Всего: {len(clients)}")
            await broadcast({"type": "system",
                             "text": f"{nickname} покинул чат",
                             "time": datetime.datetime.now().strftime("%H:%M")})
            await broadcast_users()

async def broadcast_users():
    await broadcast({"type": "users", "users": list(clients.values())})

async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"🚀 Сервер на порту {port}")
    async with websockets.serve(
        handler, "0.0.0.0", port,
        process_request=process_request
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())