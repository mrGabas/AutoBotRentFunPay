# main.py
import tkinter as tk
from tkinter import messagebox, simpledialog, filedialog, ttk
import threading
from queue import Queue
import uuid
import csv
import shutil
import logging
from datetime import datetime, timedelta
import pytz
import os

import config
import db_handler
from ui import UIManager  # Импортируем обновленный UIManager
from utils import background_checker, format_timedelta, format_display_time


# --- Слой данных ---
class DataManager:
    """Отвечает за загрузку и хранение данных приложения."""

    def __init__(self):
        self.games = []
        self.accounts = []
        self.rentals = []
        self.history = []

    def load_all_data(self):
        """Загружает все данные из базы данных в память."""
        logging.info("DataManager: Загрузка всех данных из БД...")
        # Загрузка игр и создание карты ID -> Имя
        games_raw = db_handler.db_query("SELECT id, name FROM games ORDER BY name", fetch="all") or []
        self.games = [{"id": g_id, "name": g_name} for g_id, g_name in games_raw]
        game_id_map = {g["id"]: g["name"] for g in self.games}

        # Загрузка аккаунтов
        accounts_raw = db_handler.db_query("SELECT id, login, password, game_id, rented_by FROM accounts",
                                           fetch="all") or []
        self.accounts.clear()
        for acc_id, login, password, game_id, rented_by in accounts_raw:
            self.accounts.append({
                "id": acc_id, "login": login, "password": password, "game_id": game_id,
                "game_name": game_id_map.get(game_id, "N/A"), "rented_by": rented_by
            })

        # Загрузка аренд и истории
        rentals_raw = db_handler.db_query(
            """SELECT r.id,
                      r.client_name,
                      r.start_time,
                      r.end_time,
                      r.remind_time,
                      r.initial_minutes,
                      r.info,
                      r.reminded,
                      r.is_history,
                      a.id,
                      a.login,
                      a.password,
                      g.name
               FROM rentals r
                        LEFT JOIN accounts a ON r.account_id = a.id
                        LEFT JOIN games g ON a.game_id = g.id""",
            fetch="all") or []
        self.rentals.clear()
        self.history.clear()
        for row in rentals_raw:
            item = {
                "id": row[0], "name": row[1],
                "start": datetime.fromisoformat(row[2]) if row[2] else None,
                "end": datetime.fromisoformat(row[3]) if row[3] else None,
                "remind": datetime.fromisoformat(row[4]) if row[4] else None,
                "minutes": row[5], "info": row[6], "reminded": bool(row[7]),
                "account_id": row[9], "account_login": row[10] or "УДАЛЕН",
                "account_password": row[11] or "УДАЛЕН", "game": row[12] or "УДАЛЕНА"
            }
            if row[8] == 1:
                self.history.append(item)
            else:
                self.rentals.append(item)
        logging.info("DataManager: Данные успешно загружены.")


# --- Слой логики ---
class ActionHandler:
    """Обрабатывает все действия пользователя из интерфейса."""

    def __init__(self, master, data_manager, ui_manager, app_controller):
        self.master = master
        self.data = data_manager
        self.ui = ui_manager
        self.app = app_controller

    def add_client(self):
        """Логика добавления новой аренды."""
        try:
            name = self.ui.entry_name.get().strip()
            info = self.ui.entry_info.get().strip()
            game_name = self.ui.game_var.get()
            account_display = self.ui.account_var.get()
            days = int(self.ui.entry_days.get() or 0)
            hours = int(self.ui.entry_hours.get() or 0)
            minutes = int(self.ui.entry_minutes.get() or 0)
            total_minutes = (days * 1440) + (hours * 60) + minutes

            if not all([name, game_name, account_display]) or "Свободных" in account_display:
                messagebox.showerror("Ошибка", "Поля 'Имя клиента', 'Игра' и 'Аккаунт' должны быть заполнены.")
                return
            if total_minutes <= 0:
                messagebox.showerror("Ошибка", "Длительность аренды должна быть больше нуля.")
                return

            start = datetime.now()
            end = start + timedelta(minutes=total_minutes)
            remind = end - timedelta(minutes=5)
            login, password = account_display.split(" / ", 1)
            account_id = next((acc['id'] for acc in self.data.accounts if acc['login'] == login), None)

            if not account_id:
                messagebox.showerror("Ошибка", "Не удалось найти ID аккаунта.")
                return

            db_handler.db_query(
                "INSERT INTO rentals (id, client_name, account_id, start_time, end_time, remind_time, initial_minutes, info) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), name, account_id, start.isoformat(), end.isoformat(), remind.isoformat(),
                 total_minutes, info)
            )
            db_handler.db_query("UPDATE accounts SET rented_by = ? WHERE id = ?", (name, account_id))

            self.ui.clear_input_fields()
            self.app.full_update()
        except ValueError:
            messagebox.showerror("Ошибка", "Дни, часы и минуты должны быть числами.")
        except Exception as e:
            logging.error(f"Ошибка при добавлении клиента: {e}")
            messagebox.showerror("Ошибка", f"Произошла непредвиденная ошибка:\n{e}")

    def remove_selected(self):
        """Перемещает выбранные аренды в историю."""
        selection = self.ui.tree.selection()
        if not selection: return
        if messagebox.askyesno("Подтверждение", "Переместить выбранные аренды в историю?"):
            for rental_id in selection:
                rental_info = db_handler.db_query("SELECT account_id FROM rentals WHERE id = ?", (rental_id,),
                                                  fetch="one")
                if rental_info and rental_info[0]:
                    db_handler.db_query("UPDATE accounts SET rented_by = NULL WHERE id = ?", (rental_info[0],))
                db_handler.db_query("UPDATE rentals SET is_history = 1 WHERE id = ?", (rental_id,))
            self.app.full_update()

    def extend_rental(self):
        """Продлевает выбранную аренду."""
        selection = self.ui.tree.selection()
        if not selection: return
        item_id = selection[0]

        minutes_to_add = self.ui.ask_duration()
        if minutes_to_add is None: return

        res = db_handler.db_query("SELECT end_time, initial_minutes FROM rentals WHERE id = ?", (item_id,), fetch="one")
        if not res: return

        current_end_time = datetime.fromisoformat(res[0])
        new_end = current_end_time + timedelta(minutes=minutes_to_add)
        new_remind = new_end - timedelta(minutes=5)
        new_initial_minutes = (res[1] or 0) + minutes_to_add

        db_handler.db_query(
            "UPDATE rentals SET end_time = ?, remind_time = ?, reminded = 0, pre_reminded = 0, initial_minutes = ? WHERE id = ?",
            (new_end.isoformat(), new_remind.isoformat(), new_initial_minutes, item_id)
        )
        self.app.full_update()

    def add_game(self):
        new_game = simpledialog.askstring("Добавить игру", "Введите название игры:", parent=self.master)
        if new_game and new_game.strip():
            db_handler.db_query("INSERT OR IGNORE INTO games (name) VALUES (?)", (new_game.strip(),))
            self.app.full_update()

    def remove_game(self):
        game_name = self.ui.game_var.get()
        if not game_name: return

        game_id = next((g['id'] for g in self.data.games if g['name'] == game_name), None)
        if not game_id: return

        accounts_count = \
        db_handler.db_query("SELECT COUNT(*) FROM accounts WHERE game_id = ?", (game_id,), fetch="one")[0]
        if accounts_count > 0:
            messagebox.showerror("Ошибка", "Нельзя удалить игру, пока к ней привязаны аккаунты.")
            return

        if messagebox.askyesno("Подтверждение", f"Вы уверены, что хотите удалить игру '{game_name}'?"):
            db_handler.db_query("DELETE FROM games WHERE id = ?", (game_id,))
            self.app.full_update()

    def add_account(self):
        game_name = self.ui.game_var.get()
        if not game_name:
            messagebox.showerror("Ошибка", "Сначала выберите игру.")
            return

        game_id = next((g['id'] for g in self.data.games if g['name'] == game_name), None)
        if not game_id: return

        login = simpledialog.askstring("Добавить аккаунт", "Введите логин:", parent=self.master)
        if not login or not login.strip(): return

        password = simpledialog.askstring("Добавить аккаунт", "Введите пароль:", parent=self.master)
        if not password: return

        db_handler.db_query("INSERT INTO accounts (login, password, game_id) VALUES (?, ?, ?)",
                            (login.strip(), password, game_id))
        self.app.full_update()

    def remove_account(self):
        selection = self.ui.accounts_tree.selection()
        if not selection: return

        for item_id in selection:
            item_values = self.ui.accounts_tree.item(item_id, 'values')
            if item_values[3] == "Занят":
                messagebox.showerror("Ошибка", f"Аккаунт {item_values[1]} занят и не может быть удален.")
                return

        if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить выбранные аккаунты?"):
            for item_id in selection:
                item_values = self.ui.accounts_tree.item(item_id, 'values')
                acc_id = next((acc['id'] for acc in self.data.accounts if acc['login'] == item_values[1]), None)
                if acc_id:
                    db_handler.db_query("DELETE FROM accounts WHERE id = ?", (acc_id,))
            self.app.full_update()

    def backup_database(self):
        backup_path = filedialog.asksaveasfilename(defaultextension=".db",
                                                   initialfile=f"rentals_backup_{datetime.now().strftime('%Y-%m-%d')}.db")
        if not backup_path: return
        try:
            shutil.copy(config.DB_FILE, backup_path)
            messagebox.showinfo("Успех", f"Резервная копия создана:\n{backup_path}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось создать резервную копию:\n{e}")

    # ... другие методы обработчиков (импорт, экспорт, удаление из истории и т.д.) могут быть добавлены сюда ...


# --- Слой представления и управления приложением ---
class RentalApp:
    """Основной класс приложения, управляющий GUI."""

    def __init__(self, master):
        self.master = master
        master.title("Менеджер Аренды (Клиент)")
        master.geometry("1200x800")

        # Эти списки теперь будут "живыми" и доступны для фоновых потоков
        self.rentals = []
        self.history = []
        self.accounts = []
        self.games = []

        self.update_queue = Queue()
        self.ui = UIManager(master, self)  # Передаем self в качестве ActionHandler

        refresh_button = ttk.Button(self.master, text="🔄 Обновить данные", command=self.full_update)
        refresh_button.pack(pady=5)

        self.full_update()
        self.start_gui_tasks()
        master.protocol("WM_DELETE_WINDOW", self.on_closing)
        logging.info("GUI приложение успешно инициализировано.")

    def full_update(self):
        """Полностью обновляет все данные и таблицы в интерфейсе."""
        self.load_all_data_from_db()
        self.ui.update_all_views(self)  # Передаем сам объект приложения
        logging.info("Интерфейс обновлен.")

    def load_all_data_from_db(self):
        """Загружает все данные из базы данных, обновляя списки на месте."""
        games_raw = db_handler.db_query("SELECT id, name FROM games ORDER BY name", fetch="all") or []
        game_id_map = {g_id: g_name for g_id, g_name in games_raw}
        # Обновление списков на месте, чтобы сохранить ссылки для фоновых потоков
        self.games[:] = [{"id": g_id, "name": g_name} for g_id, g_name in games_raw]

        accounts_raw = db_handler.db_query("SELECT id, login, password, game_id, rented_by FROM accounts",
                                           fetch="all") or []
        self.accounts[:] = [{
            "id": acc_id, "login": login, "password": password, "game_id": game_id,
            "game_name": game_id_map.get(game_id, "N/A"), "rented_by": rented_by
        } for acc_id, login, password, game_id, rented_by in accounts_raw]

        rentals_raw = db_handler.db_query("""
                                          SELECT r.id,
                                                 r.client_name,
                                                 r.start_time,
                                                 r.end_time,
                                                 r.remind_time,
                                                 r.initial_minutes,
                                                 r.info,
                                                 r.reminded,
                                                 r.is_history,
                                                 a.id,
                                                 a.login,
                                                 a.password,
                                                 g.name
                                          FROM rentals r
                                                   LEFT JOIN accounts a ON r.account_id = a.id
                                                   LEFT JOIN games g ON a.game_id = g.id
                                          """, fetch="all") or []

        new_rentals = []
        new_history = []
        for row in rentals_raw:
            item = {
                "id": row[0], "name": row[1],
                "start": datetime.fromisoformat(row[2]) if row[2] else None,
                "end": datetime.fromisoformat(row[3]) if row[3] else None,
                "remind": datetime.fromisoformat(row[4]) if row[4] else None,  # Bot remind time
                "minutes": row[5], "info": row[6], "reminded": bool(row[7]),  # Bot reminded flag
                "account_id": row[9], "account_login": row[10] or "УДАЛЕН",
                "account_password": row[11] or "УДАЛЕН", "game": row[12] or "УДАЛЕНА"
            }
            if row[8] == 1:
                new_history.append(item)
            else:
                new_rentals.append(item)

        self.rentals[:] = new_rentals
        self.history[:] = new_history

    def start_gui_tasks(self):
        """Запускает фоновые задачи, необходимые ТОЛЬКО для GUI."""
        gui_checker_thread = threading.Thread(target=background_checker, args=(self.rentals, self.update_queue),
                                              daemon=True)
        gui_checker_thread.start()
        self.process_queue()
        self.update_clock()
        self.refresh_timers()

    def process_queue(self):
        """Обрабатывает очередь GUI для всплывающих уведомлений."""
        try:
            while not self.update_queue.empty():
                message_type, data = self.update_queue.get_nowait()
                if message_type == "reminder":
                    self.master.bell()
                    self.ui.show_non_blocking_notification("Напоминание",
                                                           f"⏰ Аренда для {data.get('name')} закончится через 5 минут!")
        except Exception as e:
            logging.exception(f"Ошибка обработки очереди GUI: {e}")
        finally:
            self.master.after(200, self.process_queue)

    def on_closing(self):
        if messagebox.askokcancel("Выход", "Вы уверены, что хотите выйти?"):
            logging.info("Приложение закрыто.")
            self.master.destroy()

    def update_clock(self):
        self.ui.clock_label.config(text=f"МСК: {datetime.now(pytz.timezone('Europe/Moscow')).strftime('%H:%M:%S')}")
        self.master.after(1000, self.update_clock)

    def refresh_timers(self):
        self.ui.update_rentals_table(self.rentals)
        self.master.after(60000, self.refresh_timers)

    def add_client(self):
        """Логика добавления новой аренды."""
        try:
            name = self.ui.entry_name.get().strip()
            info = self.ui.entry_info.get().strip()
            game_name = self.ui.game_var.get()
            account_display = self.ui.account_var.get()
            days = int(self.ui.entry_days.get() or 0)
            hours = int(self.ui.entry_hours.get() or 0)
            minutes = int(self.ui.entry_minutes.get() or 0)
            total_minutes = (days * 1440) + (hours * 60) + minutes

            if not all([name, game_name, account_display]) or "Свободных" in account_display:
                messagebox.showerror("Ошибка", "Поля 'Имя клиента', 'Игра' и 'Аккаунт' должны быть заполнены.")
                return
            if total_minutes <= 0:
                messagebox.showerror("Ошибка", "Длительность аренды должна быть больше нуля.")
                return

            login, password = account_display.split(" / ", 1)
            account_id = next((acc['id'] for acc in self.accounts if acc['login'] == login), None)

            if not account_id:
                messagebox.showerror("Ошибка", "Не удалось найти ID аккаунта.")
                return

            success = db_handler.create_rental_from_gui(name, account_id, total_minutes, info)
            if success:
                self.ui.clear_input_fields()
                self.full_update()
            else:
                messagebox.showerror("Ошибка БД", "Не удалось создать запись об аренде.")

        except ValueError:
            messagebox.showerror("Ошибка", "Дни, часы и минуты должны быть числами.")
        except Exception as e:
            logging.error(f"Ошибка при добавлении клиента: {e}")
            messagebox.showerror("Ошибка", f"Произошла непредвиденная ошибка:\n{e}")

    def remove_selected(self):
        """Перемещает выбранные аренды в историю."""
        selection = self.ui.tree.selection()
        if not selection: return
        if messagebox.askyesno("Подтверждение", "Переместить выбранные аренды в историю?"):
            for rental_id in selection:
                rental_info = db_handler.db_query("SELECT account_id FROM rentals WHERE id = ?", (rental_id,),
                                                  fetch="one")
                if rental_info and rental_info[0]:
                    db_handler.db_query("UPDATE accounts SET rented_by = NULL WHERE id = ?", (rental_info[0],))
                db_handler.db_query("UPDATE rentals SET is_history = 1 WHERE id = ?", (rental_id,))
            self.full_update()

    def extend_rental(self):
        """Продлевает выбранную аренду."""
        selection = self.ui.tree.selection()
        if not selection: return
        item_id = selection[0]

        minutes_to_add = self.ui.ask_duration()
        if minutes_to_add is None or minutes_to_add <= 0: return

        res = db_handler.db_query("SELECT end_time, initial_minutes FROM rentals WHERE id = ?", (item_id,), fetch="one")
        if not res: return

        current_end_time = datetime.fromisoformat(res[0])
        new_end = current_end_time + timedelta(minutes=minutes_to_add)
        new_bot_remind = new_end - timedelta(minutes=10)
        new_initial_minutes = (res[1] or 0) + minutes_to_add

        db_handler.db_query(
            "UPDATE rentals SET end_time = ?, remind_time = ?, reminded = 0, initial_minutes = ? WHERE id = ?",
            (new_end.isoformat(), new_bot_remind.isoformat(), new_initial_minutes, item_id)
        )
        self.full_update()


if __name__ == "__main__":
    # 1. Настраиваем логирование
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(config.LOG_FILE, 'a', 'utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info("=" * 30)
    logging.info("Запуск GUI клиента...")

    # 2. Проверяем и обновляем БД
    try:
        if not os.path.exists(config.DB_FILE):
            messagebox.showerror("Критическая ошибка", f"Файл базы данных не найден: {config.DB_FILE}")
            exit()
        db_handler.initialize_and_update_db()

        # 3. Запускаем приложение
        root = tk.Tk()
        app = RentalApp(root)
        root.mainloop()

    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске приложения: {e}", exc_info=True)
        messagebox.showerror("Критическая ошибка", f"Не удалось запустить приложение. См. лог-файл.\nОшибка: {e}")
