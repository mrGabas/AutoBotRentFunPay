# ui.py
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from datetime import datetime, timedelta
import pytz
from utils import format_timedelta


class UIManager:
    """
    Класс, отвечающий исключительно за создание, настройку и обновление
    всех элементов графического интерфейса (GUI).
    """

    def __init__(self, master, action_handler=None):
        self.master = master
        self.app_actions = action_handler  # Ссылка на ActionHandler для привязки к кнопкам

        # --- Переменные для виджетов ---
        self.entry_name = None
        self.game_var = tk.StringVar(master)
        self.game_menu = None
        self.account_var = tk.StringVar(master)
        self.account_menu = None
        self.entry_days = None
        self.entry_hours = None
        self.entry_minutes = None
        self.entry_info = None
        self.clock_label = None
        self.search_rentals_var = tk.StringVar(master)
        self.tree = None
        self.search_history_var = tk.StringVar(master)
        self.history_tree = None
        self.accounts_tree = None

        self._create_widgets()
        # Привязка события изменения игры к методу обновления списка аккаунтов
        self.game_var.trace_add("write", self._on_game_change)
        # Привязка событий поиска
        self.search_rentals_var.trace_add("write", self._on_rental_search)
        self.search_history_var.trace_add("write", self._on_history_search)

    def update_all_views(self, data_manager):
        """Обновляет все таблицы и меню свежими данными из DataManager."""
        self.update_rentals_table(data_manager.rentals)
        self.update_history_table(data_manager.history)
        self.update_accounts_table(data_manager.accounts)
        self.update_game_menu(data_manager.games)
        # Обновление меню аккаунтов произойдет автоматически через trace
        if self.game_var.get() == "":
            self.update_account_menu(data_manager.games, data_manager.accounts)

    # --- Методы создания виджетов ---

    def _create_widgets(self):
        """Создает все основные виджеты приложения."""
        self._create_input_frame()
        self._create_notebook()

    def _create_input_frame(self):
        """Создает верхнюю панель для ввода данных новой аренды."""
        frame_input = tk.Frame(self.master, pady=10)
        frame_input.pack(fill=tk.X, padx=10, pady=(0, 5))

        tk.Label(frame_input, text="Имя клиента").grid(row=0, column=0, padx=5, pady=2, sticky='w')
        self.entry_name = ttk.Entry(frame_input, width=20)
        self.entry_name.grid(row=1, column=0, padx=5)

        tk.Label(frame_input, text="Игра").grid(row=0, column=1, padx=5, pady=2, sticky='w')
        self.game_menu = ttk.OptionMenu(frame_input, self.game_var, "Выберите игру")
        self.game_menu.grid(row=1, column=1, padx=5, sticky='ew')

        tk.Label(frame_input, text="Аккаунт (свободный)").grid(row=0, column=2, padx=5, pady=2, sticky='w')
        self.account_menu = ttk.OptionMenu(frame_input, self.account_var, "")
        self.account_menu.grid(row=1, column=2, padx=5, sticky='ew')

        time_frame = tk.Frame(frame_input)
        time_frame.grid(row=1, column=3, padx=5, sticky='ns')
        tk.Label(frame_input, text="Длительность").grid(row=0, column=3, padx=5, pady=2, sticky='w')

        tk.Label(time_frame, text="Д").pack(side=tk.LEFT)
        self.entry_days = ttk.Entry(time_frame, width=4)
        self.entry_days.pack(side=tk.LEFT)
        tk.Label(time_frame, text="Ч").pack(side=tk.LEFT, padx=(5, 0))
        self.entry_hours = ttk.Entry(time_frame, width=4)
        self.entry_hours.pack(side=tk.LEFT)
        tk.Label(time_frame, text="М").pack(side=tk.LEFT, padx=(5, 0))
        self.entry_minutes = ttk.Entry(time_frame, width=4)
        self.entry_minutes.pack(side=tk.LEFT)

        tk.Label(frame_input, text="Доп. инфо").grid(row=0, column=4, padx=5, pady=2, sticky='w')
        self.entry_info = ttk.Entry(frame_input, width=25)
        self.entry_info.grid(row=1, column=4, padx=5, sticky='ew')

        ttk.Button(frame_input, text="Добавить аренду", command=lambda: self.app_actions.add_client()).grid(row=1,
                                                                                                            column=5,
                                                                                                            padx=10,
                                                                                                            rowspan=2,
                                                                                                            ipady=10)

        self.clock_label = tk.Label(frame_input, text="", font=("Arial", 10))
        self.clock_label.grid(row=0, column=5, sticky='e', padx=10)

    def _create_notebook(self):
        """Создает систему вкладок."""
        notebook = ttk.Notebook(self.master)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        tab_current = self._create_rentals_tab(notebook)
        notebook.add(tab_current, text="Актуальные аренды")
        tab_history = self._create_history_tab(notebook)
        notebook.add(tab_history, text="История")
        tab_manage = self._create_manage_tab(notebook)
        notebook.add(tab_manage, text="Управление")

    def _create_rentals_tab(self, parent):
        """Создает вкладку с актуальными арендами."""
        tab = ttk.Frame(parent)

        search_frame = ttk.Frame(tab)
        search_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(search_frame, text="Поиск:").pack(side=tk.LEFT, padx=(0, 5))
        search_entry = ttk.Entry(search_frame, textvariable=self.search_rentals_var)
        search_entry.pack(fill=tk.X, side=tk.LEFT, expand=True)

        tree_frame = ttk.Frame(tab)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ("Имя", "Игра", "Длительность", "Окончание", "Осталось", "Логин", "Пароль", "Инфо")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings")

        for col in columns: self.tree.heading(col, text=col)
        self.tree.column("Имя", width=120)
        self.tree.column("Игра", width=120)
        self.tree.column("Длительность", width=80, anchor='center')
        self.tree.column("Окончание", width=130, anchor='center')
        self.tree.column("Осталось", width=80, anchor='center')
        self.tree.column("Логин", width=100)
        self.tree.column("Пароль", width=100)
        self.tree.column("Инфо", width=150)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree.tag_configure('danger', background='#FFCDD2')
        self.tree.tag_configure('normal', background='white')

        buttons_frame = ttk.Frame(tab)
        buttons_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Button(buttons_frame, text="Завершить (в историю)",
                   command=lambda: self.app_actions.remove_selected()).pack(side=tk.LEFT, expand=True, fill=tk.X,
                                                                            padx=(0, 5))
        ttk.Button(buttons_frame, text="Продлить аренду", command=lambda: self.app_actions.extend_rental()).pack(
            side=tk.LEFT, expand=True, fill=tk.X)
        return tab

    def _create_history_tab(self, parent):
        """Создает вкладку с историей аренд."""
        tab = ttk.Frame(parent)

        search_frame = ttk.Frame(tab)
        search_frame.pack(fill=tk.X, padx=10, pady=5)
        ttk.Label(search_frame, text="Поиск:").pack(side=tk.LEFT, padx=(0, 5))
        search_entry = ttk.Entry(search_frame, textvariable=self.search_history_var)
        search_entry.pack(fill=tk.X, side=tk.LEFT, expand=True)

        tree_frame = ttk.Frame(tab)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ("Имя", "Игра", "Длительность", "Окончание", "Логин", "Пароль", "Инфо")
        self.history_tree = ttk.Treeview(tree_frame, columns=columns, show="headings")
        for col in columns: self.history_tree.heading(col, text=col)

        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        buttons_frame = ttk.Frame(tab)
        buttons_frame.pack(fill=tk.X, padx=10, pady=5)
        # Пример привязки: ttk.Button(buttons_frame, text="Удалить", command=lambda: self.app_actions.remove_from_history()).pack()
        return tab

    def _create_manage_tab(self, parent):
        """Создает вкладку управления играми и аккаунтами."""
        tab = ttk.Frame(parent)

        # Управление играми
        games_frame = ttk.LabelFrame(tab, text="Игры", padding=10)
        games_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(games_frame, text="➕ Добавить игру", command=lambda: self.app_actions.add_game()).pack(side=tk.LEFT,
                                                                                                          padx=5)
        ttk.Button(games_frame, text="➖ Удалить игру", command=lambda: self.app_actions.remove_game()).pack(
            side=tk.LEFT, padx=5)

        # Таблица аккаунтов
        accounts_frame = ttk.LabelFrame(tab, text="Аккаунты", padding=10)
        accounts_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        accounts_columns = ("Игра", "Логин", "Пароль", "Статус", "Арендатор")
        self.accounts_tree = ttk.Treeview(accounts_frame, columns=accounts_columns, show="headings")
        for col in accounts_columns: self.accounts_tree.heading(col, text=col)
        self.accounts_tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        scrollbar = ttk.Scrollbar(accounts_frame, orient=tk.VERTICAL, command=self.accounts_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.accounts_tree.configure(yscrollcommand=scrollbar.set)

        # Кнопки управления аккаунтами
        acc_buttons_frame = ttk.Frame(tab)
        acc_buttons_frame.pack(fill=tk.X, padx=10, pady=(0, 5))
        ttk.Button(acc_buttons_frame, text="➕ Добавить аккаунт", command=lambda: self.app_actions.add_account()).pack(
            side=tk.LEFT, expand=True, fill=tk.X, padx=(0, 5))
        ttk.Button(acc_buttons_frame, text="➖ Удалить аккаунт", command=lambda: self.app_actions.remove_account()).pack(
            side=tk.LEFT, expand=True, fill=tk.X)

        # Резервное копирование
        backup_frame = ttk.LabelFrame(tab, text="Резервное копирование", padding=10)
        backup_frame.pack(fill=tk.X, padx=10, pady=10, side=tk.BOTTOM)
        ttk.Button(backup_frame, text="Создать резервную копию",
                   command=lambda: self.app_actions.backup_database()).pack(side=tk.LEFT, expand=True, fill=tk.X,
                                                                            padx=(0, 5))
        # ttk.Button(backup_frame, text="Восстановить из копии", command=...
        return tab

    # --- Методы обновления виджетов ---
    def update_rentals_table(self, rentals_data):
        self.tree.delete(*self.tree.get_children())
        search_term = self.search_rentals_var.get().lower()
        now = datetime.now()
        msk_tz = pytz.timezone("Europe/Moscow")

        filtered_data = [r for r in rentals_data if
                         not search_term or any(search_term in str(val).lower() for val in r.values())]

        for r in sorted(filtered_data, key=lambda r: r.get('end', datetime.max)):
            end_time = r.get('end')
            if not end_time: continue

            time_left = end_time - now
            tag = 'danger' if time_left.total_seconds() < 600 else 'normal'
            end_time_str = end_time.astimezone(msk_tz).strftime('%d.%m %H:%M')
            time_left_str = format_timedelta(time_left)

            self.tree.insert('', 'end', iid=r.get('id'), values=(
                r.get("name"), r.get("game"), f"{r.get('minutes', 0)} мин",
                end_time_str, time_left_str, r.get("account_login"),
                r.get("account_password"), r.get("info")
            ), tags=(tag,))

    def update_history_table(self, history_data):
        self.history_tree.delete(*self.history_tree.get_children())
        search_term = self.search_history_var.get().lower()
        msk_tz = pytz.timezone("Europe/Moscow")

        filtered_data = [h for h in history_data if
                         not search_term or any(search_term in str(val).lower() for val in h.values())]

        for r in sorted(filtered_data, key=lambda r: r.get('end', datetime.min), reverse=True):
            end_time = r.get('end')
            if not end_time: continue
            end_time_str = end_time.astimezone(msk_tz).strftime('%Y-%m-%d %H:%M')

            self.history_tree.insert('', 'end', iid=r.get('id'), values=(
                r.get("name"), r.get("game"), f"{r.get('minutes', 0)} мин", end_time_str,
                r.get("account_login"), r.get("account_password"), r.get("info")
            ))

    def update_accounts_table(self, accounts_data):
        self.accounts_tree.delete(*self.accounts_tree.get_children())
        for acc in sorted(accounts_data, key=lambda x: x['game_name']):
            self.accounts_tree.insert('', 'end', values=(
                acc["game_name"], acc["login"], acc["password"],
                "Занят" if acc.get("rented_by") else "Свободен",
                acc.get("rented_by", "-")
            ))

    def update_game_menu(self, games_data):
        menu = self.game_menu['menu']
        menu.delete(0, 'end')
        game_names = sorted([g['name'] for g in games_data])
        current_game = self.game_var.get()

        for name in game_names:
            menu.add_command(label=name, command=tk._setit(self.game_var, name))

        if game_names and current_game not in game_names:
            self.game_var.set(game_names[0])
        elif not game_names:
            self.game_var.set("")

    def _on_game_change(self, *args):
        if self.app_actions:
            self.update_account_menu(self.app_actions.data.games, self.app_actions.data.accounts)

    def _on_rental_search(self, *args):
        if self.app_actions:
            self.update_rentals_table(self.app_actions.data.rentals)

    def _on_history_search(self, *args):
        if self.app_actions:
            self.update_history_table(self.app_actions.data.history)

    def update_account_menu(self, games_data, accounts_data):
        menu = self.account_menu['menu']
        menu.delete(0, 'end')
        selected_game_name = self.game_var.get()
        if not selected_game_name:
            self.account_var.set("")
            return

        game_id = next((g['id'] for g in games_data if g['name'] == selected_game_name), None)
        free_accounts = [a for a in accounts_data if a['game_id'] == game_id and not a['rented_by']]

        if free_accounts:
            for acc in free_accounts:
                label = f"{acc['login']} / {acc['password']}"
                menu.add_command(label=label, command=tk._setit(self.account_var, label))
            self.account_var.set(f"{free_accounts[0]['login']} / {free_accounts[0]['password']}")
        else:
            self.account_var.set("Свободных аккаунтов нет")

    def clear_input_fields(self):
        self.entry_name.delete(0, tk.END)
        self.entry_info.delete(0, tk.END)
        self.entry_days.delete(0, tk.END)
        self.entry_hours.delete(0, tk.END)
        self.entry_minutes.delete(0, tk.END)

    def ask_duration(self):
        dialog = tk.Toplevel(self.master)
        dialog.title("Продлить на...")
        dialog.transient(self.master)
        dialog.grab_set()
        dialog.geometry("250x150")

        result = {}

        frame = ttk.Frame(dialog, padding="10")
        frame.pack(expand=True, fill=tk.BOTH)

        ttk.Label(frame, text="Дни:").grid(row=0, column=0, sticky="w", pady=2)
        days_entry = ttk.Entry(frame, width=10)
        days_entry.grid(row=0, column=1, pady=2)

        ttk.Label(frame, text="Часы:").grid(row=1, column=0, sticky="w", pady=2)
        hours_entry = ttk.Entry(frame, width=10)
        hours_entry.grid(row=1, column=1, pady=2)

        ttk.Label(frame, text="Минуты:").grid(row=2, column=0, sticky="w", pady=2)
        minutes_entry = ttk.Entry(frame, width=10)
        minutes_entry.grid(row=2, column=1, pady=2)

        def on_ok():
            try:
                days = int(days_entry.get() or 0)
                hours = int(hours_entry.get() or 0)
                minutes = int(minutes_entry.get() or 0)
                result['total'] = (days * 1440) + (hours * 60) + minutes
                dialog.destroy()
            except ValueError:
                messagebox.showerror("Ошибка", "Введите корректные числа", parent=dialog)

        ok_button = ttk.Button(frame, text="OK", command=on_ok)
        ok_button.grid(row=3, columnspan=2, pady=10)

        self.master.wait_window(dialog)
        return result.get('total')

    def show_non_blocking_notification(self, title, message):
        notif_window = tk.Toplevel(self.master)
        notif_window.title(title)
        notif_window.geometry("350x100")
        notif_window.resizable(False, False)

        self.master.update_idletasks()
        x = self.master.winfo_x() + self.master.winfo_width() - 360
        y = self.master.winfo_y() + self.master.winfo_height() - 150
        notif_window.geometry(f"+{x}+{y}")

        tk.Label(notif_window, text=message, wraplength=330, justify="center", padx=10, pady=10).pack(expand=True)
        notif_window.attributes("-topmost", True)
        notif_window.after(10000, notif_window.destroy)
