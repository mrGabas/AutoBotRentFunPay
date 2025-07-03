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
import pysftp

import config
import db_handler
from ui import UIManager
from utils import background_checker, format_timedelta, format_display_time

MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def sftp_connect():
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        return pysftp.Connection(host=config.AZURE_HOST, username=config.AZURE_USER, private_key=config.AZURE_KEY_PATH,
                                 cnopts=cnopts)
    except Exception as e:
        messagebox.showerror("Ошибка SFTP", f"Не удалось подключиться к серверу Azure:\n{e}")
        return None


def download_db():
    logging.info("Попытка скачать базу данных с сервера...")
    sftp = sftp_connect()
    if sftp:
        try:
            sftp.get(config.REMOTE_DB_PATH, config.DB_FILE)
            logging.info("База данных успешно скачана.")
            messagebox.showinfo("Синхронизация", "Актуальная база данных успешно скачана с сервера.")
        except Exception as e:
            logging.error(f"Ошибка при скачивании БД: {e}")
            messagebox.showerror("Ошибка SFTP", f"Не удалось скачать базу данных:\n{e}")
        finally:
            sftp.close()


def upload_db():
    logging.info("Попытка загрузить базу данных на сервер...")
    sftp = sftp_connect()
    if sftp:
        try:
            sftp.put(config.DB_FILE, config.REMOTE_DB_PATH)
            logging.info("База данных успешно загружена на сервер.")
            messagebox.showinfo("Синхронизация", "Изменения успешно сохранены на сервере.")
        except Exception as e:
            logging.error(f"Ошибка при загрузке БД: {e}")
            messagebox.showerror("Ошибка SFTP", f"Не удалось загрузить изменения на сервер:\n{e}")
        finally:
            sftp.close()


class RentalApp:
    def __init__(self, master):
        self.master = master
        self.master.title("Менеджер Аренды (Синхронизация с Azure)")
        self.master.geometry("1200x800")
        self.rentals, self.history, self.accounts, self.games = [], [], [], []
        self.update_queue = Queue()
        self.ui = UIManager(master, self)

        sync_frame = ttk.Frame(self.master)
        sync_frame.pack(fill=tk.X, padx=10, pady=5)
        download_button = ttk.Button(sync_frame, text="🔄 Скачать с сервера", command=self.sync_and_refresh)
        download_button.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 5))
        upload_button = ttk.Button(sync_frame, text="⬆️ Загрузить на сервер", command=upload_db)
        upload_button.pack(side=tk.LEFT, expand=True, fill=tk.X)

        self.sync_and_refresh()
        self.start_gui_tasks()
        self.master.protocol("WM_DELETE_WINDOW", self.on_closing)
        logging.info("GUI приложение успешно инициализировано.")

    def sync_and_refresh(self):
        download_db()
        self.full_update()

    def full_update(self):
        self.load_all_data_from_db()
        self.ui.update_all_views(self)

    def on_game_selection_change(self, *_args):
        self.ui.update_account_menu(self.games, self.accounts)
        self.ui.update_lots_listbox(self.games, self.ui.game_var.get())

    def load_all_data_from_db(self):
        games_raw = db_handler.db_query("SELECT id, name, funpay_offer_ids FROM games ORDER BY name", fetch="all") or []
        self.games[:] = [{"id": g[0], "name": g[1], "offer_ids": g[2]} for g in games_raw]
        game_id_map = {g['id']: g['name'] for g in self.games}
        accounts_raw = db_handler.db_query("SELECT id, login, password, game_id, rented_by FROM accounts",
                                           fetch="all") or []
        self.accounts[:] = [
            {"id": r[0], "login": r[1], "password": r[2], "game_id": r[3], "game_name": game_id_map.get(r[3], "N/A"),
             "rented_by": r[4]} for r in accounts_raw]
        rentals_raw = db_handler.db_query(
            "SELECT r.id, r.client_name, r.start_time, r.end_time, r.initial_minutes, r.info, r.is_history, a.login, a.password, g.name FROM rentals r LEFT JOIN accounts a ON r.account_id = a.id LEFT JOIN games g ON a.game_id = g.id",
            fetch="all") or []
        new_rentals, new_history = [], []
        for row in rentals_raw:
            try:
                start_time = datetime.fromisoformat(row[2]).astimezone(MOSCOW_TZ) if row[2] else None
                end_time = datetime.fromisoformat(row[3]).astimezone(MOSCOW_TZ) if row[3] else None
            except ValueError:  # На случай, если в БД есть старые "наивные" даты
                start_time = pytz.utc.localize(datetime.fromisoformat(row[2])).astimezone(MOSCOW_TZ) if row[2] else None
                end_time = pytz.utc.localize(datetime.fromisoformat(row[3])).astimezone(MOSCOW_TZ) if row[3] else None

            item = {"id": row[0], "name": row[1], "start": start_time, "end": end_time, "minutes": row[4],
                    "info": row[5], "account_login": row[7] or "УДАЛЕН", "account_password": row[8] or "УДАЛЕН",
                    "game": row[9] or "УДАЛЕНА"}
            if row[6] == 1:
                new_history.append(item)
            else:
                new_rentals.append(item)
        self.rentals[:] = new_rentals
        self.history[:] = new_history

    def refresh_timers(self):
        now = datetime.now(MOSCOW_TZ)
        self.ui.update_rentals_table(self.rentals, now)
        self.master.after(60000, self.refresh_timers)

    def update_lots_listbox(self):
        self.ui.update_lots_listbox(self.games, self.ui.game_var.get())

    # Остальной код класса RentalApp остается без изменений...
    def update_lots_listbox(self):
        listbox = self.ui.lots_listbox
        listbox.delete(0, tk.END)
        selected_game_name = self.ui.game_var.get()
        if not selected_game_name: return
        game = next((g for g in self.games if g['name'] == selected_game_name), None)
        if game and game.get('offer_ids'):
            for lot_id in sorted(game['offer_ids'].split(',')):
                if lot_id: listbox.insert(tk.END, lot_id)

    def add_lot_to_game(self):
        selected_game_name = self.ui.game_var.get()
        if not selected_game_name:
            messagebox.showerror("Ошибка", "Сначала выберите игру.")
            return
        new_lot_id = self.ui.lot_id_entry.get().strip()
        if not new_lot_id.isdigit():
            messagebox.showerror("Ошибка", "ID лота должен состоять только из цифр.")
            return
        game = next((g for g in self.games if g['name'] == selected_game_name), None)
        if not game: return
        current_ids_str = game.get('offer_ids') or ""
        current_ids = set(current_ids_str.split(',')) if current_ids_str else set()
        if new_lot_id in current_ids:
            messagebox.showwarning("Внимание", "Этот ID лота уже добавлен к игре.")
            return
        current_ids.add(new_lot_id)
        current_ids.discard('')
        new_ids_str = ",".join(sorted(list(current_ids)))
        db_handler.set_game_offer_ids(game['id'], new_ids_str)
        self.ui.lot_id_entry.delete(0, tk.END)
        self.full_update()

    def remove_lot_from_game(self):
        selection = self.ui.lots_listbox.curselection()
        if not selection:
            messagebox.showerror("Ошибка", "Сначала выберите ID лота в списке.")
            return
        lot_id_to_remove = self.ui.lots_listbox.get(selection[0])
        selected_game_name = self.ui.game_var.get()
        game = next((g for g in self.games if g['name'] == selected_game_name), None)
        if not game: return
        current_ids_str = game.get('offer_ids') or ""
        current_ids = set(current_ids_str.split(','))
        current_ids.discard(lot_id_to_remove)
        new_ids_str = ",".join(sorted(list(current_ids)))
        db_handler.set_game_offer_ids(game['id'], new_ids_str)
        self.full_update()

    def update_rental_details(self, rental_id, new_name, new_info):
        db_handler.db_query("UPDATE rentals SET client_name = ?, info = ? WHERE id = ?",
                            (new_name, new_info, rental_id))

    def edit_account(self):
        selection = self.ui.accounts_tree.selection()
        if not selection:
            messagebox.showerror("Ошибка", "Сначала выберите аккаунт для редактирования.")
            return
        account_id = selection[0]
        account_to_edit = next((acc for acc in self.accounts if acc["id"] == int(account_id)), None)
        if account_to_edit:
            self.ui.show_account_editor_window(account_to_edit, self.full_update)
        else:
            messagebox.showerror("Ошибка", "Не удалось найти данные аккаунта.")

    def update_account_details(self, account_id, new_login, new_password):
        db_handler.update_account(account_id, new_login, new_password)

    def start_gui_tasks(self):
        gui_checker_thread = threading.Thread(target=background_checker, args=(self.rentals, self.update_queue),
                                              daemon=True)
        gui_checker_thread.start()
        self.process_queue()
        self.update_clock()
        self.refresh_timers()

    def process_queue(self):
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
        self.ui.clock_label.config(text=f"МСК: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}")
        self.master.after(1000, self.update_clock)

    def add_client(self):
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

            start_time = datetime.now(MOSCOW_TZ)
            end_time = start_time + timedelta(minutes=total_minutes)
            remind_time = end_time - timedelta(minutes=5)
            rental_id = str(uuid.uuid4())
            db_query_params = (rental_id, name, account_id, start_time.isoformat(), end_time.isoformat(),
                               remind_time.isoformat(), total_minutes, info)
            db_handler.db_query(
                "INSERT INTO rentals (id, client_name, account_id, start_time, end_time, remind_time, initial_minutes, info) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                db_query_params)
            db_handler.db_query("UPDATE accounts SET rented_by = ? WHERE id = ?", (name, account_id))
            self.ui.clear_input_fields()
            self.full_update()

        except ValueError:
            messagebox.showerror("Ошибка", "Дни, часы и минуты должны быть числами.")
        except Exception as e:
            logging.error(f"Ошибка при добавлении клиента: {e}")
            messagebox.showerror("Ошибка", f"Произошла непредвиденная ошибка:\n{e}")

    def remove_selected(self):
        selection = self.ui.tree.selection()
        if not selection: return
        if messagebox.askyesno("Подтверждение", "Переместить выбранные аренды в историю?"):
            for rental_id in selection:
                db_handler.move_rental_to_history(rental_id)
            self.full_update()

    def edit_rental(self, _event=None):
        if not self.ui.tree.selection(): return
        item_id = self.ui.tree.selection()[0]
        rental_to_edit = next((r for r in self.rentals if r.get("id") == item_id), None)
        if not rental_to_edit: return
        self.ui.show_editor_window(rental_to_edit, self.full_update)

    def extend_rental(self):
        selection = self.ui.tree.selection()
        if not selection: return
        item_id = selection[0]
        minutes_to_add = self.ui.ask_duration_popup()
        if minutes_to_add is None or minutes_to_add <= 0: return
        success = db_handler.extend_rental_from_gui(item_id, minutes_to_add)
        if success:
            self.full_update()
        else:
            messagebox.showerror("Ошибка", "Не удалось продлить аренду.")

    def remove_from_history(self):
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
            account_data = next((acc for acc in self.accounts if acc['id'] == int(item_id)), None)
            if account_data and account_data.get("rented_by"):
                messagebox.showerror("Ошибка", f"Аккаунт {account_data['login']} занят и не может быть удален.")
                return
        if messagebox.askyesno("Подтверждение", "Вы уверены, что хотите удалить выбранные аккаунты?"):
            for item_id in selection:
                db_handler.db_query("DELETE FROM accounts WHERE id = ?", (int(item_id),))
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
                    writer.writerow([acc.get('game_name', 'N/A'), acc.get('login'), acc.get('password'),
                                     "Занят" if acc.get("rented_by") else "Свободен", acc.get("rented_by", "-")])
            messagebox.showinfo("Экспорт завершен", f"Список аккаунтов успешно сохранен в файл:\n{file_path}")
        except IOError as e:
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл. Ошибка:\n{e}")

    def import_accounts_from_csv(self):
        file_path = filedialog.askopenfilename(title="Выберите CSV для импорта", filetypes=[("CSV-файлы", "*.csv")])
        if not file_path: return
        imported, skipped = db_handler.import_accounts_from_csv(file_path)
        if imported is None:
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
                    writer.writerow([item.get('id'), item.get('name'), item.get('game'),
                                     format_timedelta(timedelta(minutes=item.get('minutes', 0))),
                                     item.get('start').strftime('%Y-%m-%d %H:%M:%S') if item.get('start') else '',
                                     item.get('end').strftime('%Y-%m-%d %H:%M:%S') if item.get('end') else '',
                                     item.get('account_login'), item.get('account_password'), item.get('info')])
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
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    logging.getLogger('pysftp').setLevel(logging.WARNING)

    logging.info("=" * 30)
    logging.info("Запуск GUI клиента...")
    try:
        if not os.path.exists(config.DB_FILE):
            if not messagebox.askokcancel("База данных не найдена",
                                          f"Файл {config.DB_FILE} не найден.\n\nСкачать актуальную базу с сервера?"):
                exit()

        db_handler.initialize_and_update_db()
        root = tk.Tk()
        app = RentalApp(root)
        root.mainloop()

    except Exception as e:
        logging.critical(f"Критическая ошибка при запуске приложения: {e}", exc_info=True)
        messagebox.showerror("Критическая ошибка", f"Не удалось запустить приложение. См. лог-файл.\nОшибка: {e}")