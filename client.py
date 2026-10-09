import socket
import threading
import tkinter as tk
from tkinter import scrolledtext, simpledialog, messagebox

HOST = '127.0.0.1'
PORT = 9090


class MessengerClient:
    def __init__(self):
        self.socket = None
        self.username = None
        self.running = False

        # Окно входа
        self.root = tk.Tk()
        self.root.title("Мессенджер")
        self.root.geometry("600x500")

        self.build_login_ui()

    def build_login_ui(self):
        """Экран входа"""
        self.login_frame = tk.Frame(self.root)
        self.login_frame.pack(expand=True)

        tk.Label(self.login_frame, text="Введите ваше имя:",
                 font=("Arial", 14)).pack(pady=10)

        self.name_entry = tk.Entry(self.login_frame, font=("Arial", 12), width=25)
        self.name_entry.pack(pady=5)
        self.name_entry.bind('<Return>', lambda e: self.connect())

        tk.Button(self.login_frame, text="Подключиться",
                  font=("Arial", 12), command=self.connect).pack(pady=10)

    def connect(self):
        """Подключение к серверу"""
        username = self.name_entry.get().strip()
        if not username:
            messagebox.showwarning("Ошибка", "Введите имя!")
            return

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((HOST, PORT))
            self.socket.send(username.encode('utf-8'))
            self.username = username
            self.running = True

            self.login_frame.destroy()
            self.build_chat_ui()

            # Поток для приёма сообщений
            threading.Thread(target=self.receive_messages, daemon=True).start()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось подключиться:\n{e}")

    def build_chat_ui(self):
        """Основной интерфейс чата"""
        # Заголовок
        header = tk.Label(self.root, text=f"Чат — {self.username}",
                          font=("Arial", 12, "bold"), bg="#4a76a8", fg="white")
        header.pack(fill=tk.X)

        # Область сообщений
        self.chat_area = scrolledtext.ScrolledText(
            self.root, wrap=tk.WORD, font=("Arial", 11), state='disabled'
        )
        self.chat_area.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.chat_area.tag_config('system', foreground='gray')

        # Нижняя панель
        bottom = tk.Frame(self.root)
        bottom.pack(fill=tk.X, padx=5, pady=5)

        self.msg_entry = tk.Entry(bottom, font=("Arial", 11))
        self.msg_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.msg_entry.bind('<Return>', lambda e: self.send_message())

        tk.Button(bottom, text="Отправить", command=self.send_message,
                  bg="#4a76a8", fg="white", font=("Arial", 10)).pack(side=tk.RIGHT)

        self.root.protocol("WM_DELETE_WINDOW", self.disconnect)

    def send_message(self):
        """Отправка сообщения"""
        message = self.msg_entry.get().strip()
        if not message or not self.running:
            return

        try:
            self.socket.send(message.encode('utf-8'))
            self.msg_entry.delete(0, tk.END)
            self.append_message(f"Вы: {message}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось отправить:\n{e}")
            self.disconnect()

    def receive_messages(self):
        """Приём сообщений от сервера"""
        while self.running:
            try:
                data = self.socket.recv(4096).decode('utf-8')
                if not data:
                    break
                self.append_message(data)
            except Exception:
                break

        if self.running:
            self.append_message("[СИСТЕМА] Соединение потеряно", 'system')
            self.running = False

    def append_message(self, message, tag=None):
        """Добавление сообщения в чат"""
        self.chat_area.config(state='normal')
        if tag:
            self.chat_area.insert(tk.END, message + "\n", tag)
        else:
            self.chat_area.insert(tk.END, message + "\n")
        self.chat_area.config(state='disabled')
        self.chat_area.see(tk.END)

    def disconnect(self):
        """Отключение от сервера"""
        self.running = False
        try:
            if self.socket:
                self.socket.close()
        except Exception:
            pass
        self.root.destroy()

    def run(self):
        self.root.mainloop()


if __name__ == '__main__':
    MessengerClient().run()