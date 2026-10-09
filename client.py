import asyncio
import websockets
import json
import threading
import sys

SERVER_URL = "ws://localhost:8765"   # ← замени на адрес сервера

class ChatClient:
    def __init__(self, url, nickname):
        self.url = url
        self.nickname = nickname
        self.ws = None

    async def receive_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                t = msg.get("type")

                if t == "message":
                    print(f"\n[{msg['time']}] {msg['nickname']}: {msg['text']}")
                elif t == "system":
                    print(f"\n*** {msg['text']} ***")
                elif t == "users":
                    print(f"\n👥 Онлайн ({len(msg['users'])}): {', '.join(msg['users'])}")

                print("> ", end="", flush=True)
        except websockets.exceptions.ConnectionClosed:
            print("\n❌ Соединение закрыто")
            sys.exit(0)

    async def send_loop(self):
        loop = asyncio.get_event_loop()
        while True:
            text = await loop.run_in_executor(None, input, "> ")
            if not text.strip():
                continue
            if text.strip().lower() in ("/exit", "/quit"):
                await self.ws.close()
                break
            await self.ws.send(json.dumps({
                "type": "message",
                "text": text
            }, ensure_ascii=False))

    async def run(self):
        async with websockets.connect(self.url) as ws:
            self.ws = ws
            # Регистрация
            await ws.send(json.dumps({"nickname": self.nickname}, ensure_ascii=False))
            print(f"✅ Подключено к {self.url}")
            print("Введите сообщение (или /exit для выхода):\n")

            await asyncio.gather(
                self.receive_loop(),
                self.send_loop()
            )

def main():
    url = input(f"Адрес сервера [{SERVER_URL}]: ").strip() or SERVER_URL
    nick = input("Ваш ник: ").strip() or "Гость"
    client = ChatClient(url, nick)
    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\n👋 Пока!")

if __name__ == "__main__":
    main()