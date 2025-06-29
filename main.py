# main.py
# Главный файл для запуска GUI-клиента на вашем ПК.
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

# Импорт наших модулей
import config
import db_handler
from ui import UIManager
from utils import background_checker, format_timedelta, format_display_time


class RentalApp:
    """Основной класс приложения, управляющий GUI и действиями пользователя."""

    def __init__(self, master):
        self.master = master
        self.master.title("Менеджер Аренды (Клиент)")
        self.master.geometry("1200x800")

        # Эти списки теперь будут "живыми" и доступны для фоновых потоков
        self.rentals = []
        self.history = []
        self.accounts = []
        self.games = []

        self.update_queue = Queue()  # Для уведомлений в GUI
        self.ui = UIManager(master, self)  # Передаем self в качестве обработчика действий

        # Добавляем кнопку "Обновить"
        refresh_button = ttk.Button(self.master, text="🔄 Обновить данные", command=self.full_update)
        refresh_button.pack(pady=5)

        self.full_update()
        self.start_gui_tasks()
        self.master.protocol("WM_DELETE_WINDOW", self.on_closing)
        logging.info("GUI приложение успешно инициализировано.")

    def full_update(self):
        """Полностью обновляет все данные и таблицы в интерфейсе."""
        self.load_all_data_from_db()
        self.ui.update_all_views(self)
        logging.info("Интерфейс обновлен.")

    def load_all_data_from_db(self):
        """Загружает все данные из базы данных, обновляя списки на месте."""
        games_raw = db_handler.db_query("SELECT id, name FROM games ORDER BY name", fetch="all") or []
        game_id_map = {g_id: g_name for g_id, g_name in games_raw}
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

        new_rentals, new_history = [], []
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

            # Используем функцию из db_handler для создания аренды
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
                # Используем функцию из db_handler для перемещения в историю
                db_handler.move_rental_to_history(rental_id)
            self.full_update()

    def edit_rental(self, _event=None):
        """Редактирование данных выбранной аренды."""
        if not self.ui.tree.selection(): return
        item_id = self.ui.tree.selection()[0]
        rental_to_edit = next((r for r in self.rentals if r.get("id") == item_id), None)
        if not rental_to_edit: return

        self.ui.show_editor_window(rental_to_edit, self.full_update)

    def extend_rental(self):
        """Продлевает выбранную аренду."""
        selection = self.ui.tree.selection()
        if not selection: return
        item_id = selection[0]

        minutes_to_add = self.ui.ask_duration_popup()
        if minutes_to_add is None or minutes_to_add <= 0: return

        # Используем функцию из db_handler для продления
        success = db_handler.extend_rental_from_gui(item_id, minutes_to_add)
        if success:
            self.full_update()
        else:
            messagebox.showerror("Ошибка", "Не удалось продлить аренду.")

    def remove_from_history(self):
        """Удаляет выбранные записи из истории."""
        if not self.ui.history_tree.selection(): return
        if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите НАВСЕГДА удалить выбранные записи?"):
            for item_id in self.ui.history_tree.selection():
                db_handler.db_query("DELETE FROM rentals WHERE id = ?", (item_id,))
            self.full_update()

    def add_game(self):
        new_game = simpledialog.askstring("Добавить игру", "Введите название игры:", parent=self.master)
        if new_game and new_game.strip():
            db_handler.add_game(new_game.strip())
            self.full_update()

    def remove_game(self):
        game_name = self.ui.game_var.get()
        if not game_name: return
        game_id = next((g['id'] for g in self.games if g['name'] == game_name), None)
        if not game_id: return

        if db_handler.remove_game(game_id):
            self.full_update()
        else:
            messagebox.showerror("Ошибка", "Нельзя удалить игру, пока к ней привязаны аккаунты.")

    def add_account(self):
        game_name = self.ui.game_var.get()
        if not game_name:
            messagebox.showerror("Ошибка", "Сначала выберите игру.")
            return
        game_id = next((g['id'] for g in self.games if g['name'] == game_name), None)
        if not game_id: return

        login = simpledialog.askstring("Добавить аккаунт", "Введите логин:", parent=self.master)
        if not login or not login.strip(): return

        password = simpledialog.askstring("Добавить аккаунт", "Введите пароль:", parent=self.master)
        if not password: return

        db_handler.add_account(login.strip(), password, game_id)
        self.full_update()

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
                # Удаляем по логину
                db_handler.remove_account_by_login(item_values[1])
            self.full_update()

    def export_accounts_to_csv(self):
        if not self.accounts:
            messagebox.showinfo("Информация", "Список аккаунтов пуст.")
            return
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV-файлы", "*.csv")],
                                                 title="Сохранить аккаунты как...")
        if not file_path: return
        headers = ["Игра", "Логин", "Пароль", "Статус", "Кем занят"]
        try:
            with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for acc in self.accounts:
                    writer.writerow([
                        acc.get('game_name', 'N/A'), acc.get('login'), acc.get('password'),
                        "Занят" if acc.get("rented_by") else "Свободен", acc.get("rented_by", "-")
                    ])
            messagebox.showinfo("Экспорт завершен", f"Список аккаунтов успешно сохранен в файл:\n{file_path}")
        except IOError as e:
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл. Ошибка:\n{e}")

    def import_accounts_from_csv(self):
        file_path = filedialog.askopenfilename(title="Выберите CSV для импорта", filetypes=[("CSV-файлы", "*.csv")])
        if not file_path: return

        imported, skipped = db_handler.import_accounts_from_csv(file_path)

        if imported is None:  # Ошибка при чтении файла
            messagebox.showerror("Ошибка чтения файла", f"Не удалось прочитать файл. Подробности в логах.")
            return

        messagebox.showinfo("Импорт завершен",
                            f"Успешно добавлено: {imported} акк.\nПропущено (нет игры): {skipped} акк.")
        self.full_update()

    def export_history_to_csv(self):
        if not self.history:
            messagebox.showinfo("Информация", "История аренд пуста.")
            return
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV-файлы", "*.csv")],
                                                 title="Сохранить историю как...")
        if not file_path: return
        headers = ["ID", "Клиент", "Игра", "Длительность", "Начало", "Окончание", "Логин", "Пароль", "Инфо"]
        try:
            with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for item in sorted(self.history, key=lambda x: x['start'], reverse=True):
                    writer.writerow([
                        item.get('id'), item.get('name'), item.get('game'),
                        format_timedelta(timedelta(minutes=item.get('minutes', 0))),
                        item.get('start').strftime('%Y-%m-%d %H:%M:%S') if item.get('start') else '',
                        item.get('end').strftime('%Y-%m-%d %H:%M:%S') if item.get('end') else '',
                        item.get('account_login'), item.get('account_password'), item.get('info')
                    ])
            messagebox.showinfo("Экспорт завершен", f"История успешно сохранена в файл:\n{file_path}")
        except IOError as e:
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл. Ошибка:\n{e}")

    def backup_database(self):
        backup_path = filedialog.asksaveasfilename(title="Сохранить резервную копию", defaultextension=".db",
                                                   filetypes=[("База данных", "*.db")],
                                                   initialfile=f"rentals_backup_{datetime.now().strftime('%Y-%m-%d')}.db")
        if not backup_path: return
        try:
            shutil.copy(config.DB_FILE, backup_path)
            messagebox.showinfo("Успех", f"Резервная копия успешно создана:\n{backup_path}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось создать резервную копию:\n{e}")

    def restore_database(self):
        if not messagebox.askokcancel("ПОДТВЕРЖДЕНИЕ",
                                      "ВНИМАНИЕ!\n\nЭто действие ЗАМЕНИТ все текущие данные.\nПродолжить?"): return
        restore_path = filedialog.askopenfilename(title="Выберите файл для восстановления",
                                                  filetypes=[("База данных", "*.db")])
        if not restore_path: return
        try:
            shutil.copy(restore_path, config.DB_FILE)
            messagebox.showinfo("Успех", "База данных успешно восстановлена.")
            self.full_update()
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось восстановить базу данных:\n{e}")


if __name__ == "__main__":
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if not os.path.exists(os.path.dirname(config.LOG_FILE)):
        os.makedirs(os.path.dirname(config.LOG_FILE))

    file_handler = logging.FileHandler(config.LOG_FILE, 'a', 'utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info("=" * 30)
    logging.info("Запуск GUI клиента...")

    try:
        if not os.path.exists(config.DB_FILE):
            messagebox.showerror("Критическая ошибка", f"Файл базы данных не найден: {config.DB_FILE}")
            exit()
        db_handler.initialize_and_update_db()

        root = tk.Tk()
        app = RentalApp(root)
        root.mainloop()

    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске приложения: {e}", exc_info=True)
        messagebox.showerror("Критическая ошибка", f"Не удалось запустить приложение. См. лог-файл.\nОшибка: {e}")