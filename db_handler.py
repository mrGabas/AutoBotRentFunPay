# db_handler.py
import sqlite3
import logging
from datetime import datetime
import pytz  # <-- Импортируем библиотеку для работы с часовыми поясами
from datetime import timedelta
import uuid
import csv
from config import DB_FILE
from database import db_query, init_database

# Устанавливаем часовой пояс, который будет использоваться во всем проекте
MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def find_game_by_offer_id(offer_id: str):
    """
    Находит игру, к которой привязан указанный ID лота.
    Возвращает (id, name) игры или None.
    """
    all_games = db_query("SELECT id, name, funpay_offer_ids FROM games", fetch="all")
    if not all_games:
        return None

    for game_id, game_name, ids_str in all_games:
        if ids_str:
            if offer_id in ids_str.split(','):
                return game_id, game_name
    return None

def add_offer_id_to_game(game_id: int, offer_ids_to_add):
    if not game_id or not offer_ids_to_add: return
    if not isinstance(offer_ids_to_add, list):
        offer_ids_to_add = [str(offer_ids_to_add)]
    try:
        current_ids_str = db_query("SELECT funpay_offer_ids FROM games WHERE id = ?", (game_id,), fetch="one")
        current_ids = set(current_ids_str[0].split(',')) if (current_ids_str and current_ids_str[0]) else set()

        updated = False
        for offer_id in offer_ids_to_add:
            if str(offer_id) not in current_ids:
                current_ids.add(str(offer_id))
                updated = True

        if updated:
            new_ids_str = ",".join(sorted(list(current_ids), key=int))
            db_query("UPDATE games SET funpay_offer_ids = ? WHERE id = ?", (new_ids_str, game_id))
            logging.info(f"[DB] Обновлен список лотов для игры {game_id}. Новые ID: {offer_ids_to_add}.")
    except Exception as e:
        logging.error(f"[DB] Ошибка при добавлении лотов к игре {game_id}: {e}")

def _check_column_exists(cursor, table_name, column_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return column_name in [row[1] for row in cursor.fetchall()]


def initialize_and_update_db():
    logging.info("Проверка и инициализация базы данных...")
    init_database()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            if not _check_column_exists(cursor, "games", "funpay_offer_ids"):
                cursor.execute("ALTER TABLE games ADD COLUMN funpay_offer_ids TEXT")
            if not _check_column_exists(cursor, "rentals", "funpay_chat_id"):
                cursor.execute("ALTER TABLE rentals ADD COLUMN funpay_chat_id TEXT")
            if not _check_column_exists(cursor, "rentals", "pre_reminded"):
                cursor.execute("ALTER TABLE rentals ADD COLUMN pre_reminded INTEGER DEFAULT 0")
            conn.commit()
            logging.info("Схема базы данных актуальна.")
    except sqlite3.Error as e:
        logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА при обновлении схемы БД: {e}")
        raise


# <<< ИЗМЕНЕНИЕ: Все операции со временем теперь используют MOSCOW_TZ >>>
def create_rental_from_gui(client_name, account_id, total_minutes, info):
    try:
        start_time = datetime.now(MOSCOW_TZ)  # <-- Используем МСК
        end_time = start_time + timedelta(minutes=total_minutes)
        remind_time = end_time - timedelta(minutes=10)  # <-- Напоминание за 10 минут
        rental_id = str(uuid.uuid4())

        db_query(
            "INSERT INTO rentals (id, client_name, account_id, start_time, end_time, remind_time, initial_minutes, info) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rental_id, client_name, account_id, start_time.isoformat(), end_time.isoformat(), remind_time.isoformat(),
             total_minutes, info))
        db_query("UPDATE accounts SET rented_by = ? WHERE id = ?", (client_name, account_id))
        return True
    except Exception as e:
        logging.error(f"Ошибка создания аренды из GUI: {e}")
        return False


def move_rental_to_history(rental_id):
    try:
        rental_info = db_query("SELECT account_id FROM rentals WHERE id = ?", (rental_id,), fetch="one")
        if rental_info and rental_info[0]:
            db_query("UPDATE accounts SET rented_by = NULL WHERE id = ?", (rental_info[0],))
        db_query("UPDATE rentals SET is_history = 1 WHERE id = ?", (rental_id,))
        return True
    except Exception as e:
        logging.error(f"Ошибка перемещения аренды {rental_id} в историю: {e}")
        return False


def extend_rental_from_gui(rental_id, minutes_to_add):
    try:
        res = db_query("SELECT end_time, initial_minutes FROM rentals WHERE id = ?", (rental_id,), fetch="one")
        if not res: return False

        current_end_time = datetime.fromisoformat(res[0])
        new_end = current_end_time + timedelta(minutes=minutes_to_add)
        new_remind = new_end - timedelta(minutes=5)
        new_initial_minutes = (res[1] or 0) + minutes_to_add

        db_query(
            "UPDATE rentals SET end_time = ?, remind_time = ?, reminded = 0, pre_reminded = 0, initial_minutes = ? WHERE id = ?",
            (new_end.isoformat(), new_remind.isoformat(), new_initial_minutes, rental_id))
        return True
    except Exception as e:
        logging.error(f"Ошибка продления аренды {rental_id} из GUI: {e}")
        return False


def add_game(game_name):
    return db_query("INSERT OR IGNORE INTO games (name) VALUES (?)", (game_name,))


def remove_game(game_id):
    if db_query("SELECT COUNT(*) FROM accounts WHERE game_id = ?", (game_id,), fetch="one")[0] > 0:
        return False
    db_query("DELETE FROM games WHERE id = ?", (game_id,))
    return True


def add_account(login, password, game_id):
    db_query("INSERT INTO accounts (login, password, game_id) VALUES (?, ?, ?)", (login, password, game_id))


def update_account(account_id, new_login, new_password):
    db_query("UPDATE accounts SET login = ?, password = ? WHERE id = ?", (new_login, new_password, account_id))
    logging.info(f"Аккаунт ID:{account_id} успешно обновлен. Новый логин: {new_login}")


def remove_account_by_login(login):
    db_query("DELETE FROM accounts WHERE login = ?", (login,))


def import_accounts_from_csv(file_path):
    game_map = {g[1]: g[0] for g in db_query("SELECT id, name FROM games", fetch="all")}
    new_accounts, skipped_count = [], 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 3: continue
                game_name, login, password = row[0].strip(), row[1].strip(), row[2].strip()
                if game_name in game_map:
                    new_accounts.append((login, password, game_map[game_name]))
                else:
                    skipped_count += 1
    except Exception as e:
        logging.error(f"Ошибка чтения CSV для импорта: {e}")
        return None, None
    if new_accounts:
        db_query("INSERT INTO accounts (login, password, game_id) VALUES (?, ?, ?)", new_accounts, many=True)
    return len(new_accounts), skipped_count


def get_user_rental_info(username):
    return db_query(
        "SELECT end_time FROM rentals WHERE client_name = ? AND is_history = 0 ORDER BY end_time DESC LIMIT 1",
        (username,), fetch="one")


def get_games_stats():
    return db_query(
        "SELECT g.name, COUNT(a.id) as total, SUM(CASE WHEN a.rented_by IS NULL THEN 1 ELSE 0 END) as free FROM games g LEFT JOIN accounts a ON g.id = a.game_id GROUP BY g.id ORDER BY g.name",
        fetch="all")


def rent_account(game_name, client_name, minutes, chat_id):
    game_id_res = db_query("SELECT id FROM games WHERE name LIKE ?", (f"%{game_name}%",), fetch="one")
    if not game_id_res: return None
    game_id = game_id_res[0]
    free_account = db_query(
        "SELECT id, login, password FROM accounts WHERE game_id = ? AND (rented_by IS NULL OR rented_by = '') LIMIT 1",
        (game_id,), fetch="one")
    if not free_account: return None
    acc_id, login, password = free_account
    now = datetime.now(MOSCOW_TZ)  # <-- Используем МСК
    end_time = now + timedelta(minutes=minutes)
    remind_time = end_time - timedelta(minutes=10)
    rental_id = str(uuid.uuid4())
    db_query(
        "INSERT INTO rentals (id, client_name, account_id, start_time, end_time, remind_time, initial_minutes, funpay_chat_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rental_id, client_name, acc_id, now.isoformat(), end_time.isoformat(), remind_time.isoformat(), minutes,
         str(chat_id)))
    db_query("UPDATE accounts SET rented_by = ? WHERE id = ?", (client_name, acc_id))
    return login, password, game_id


def check_and_process_expired_rentals():
    now_iso = datetime.now(MOSCOW_TZ).isoformat()  # <-- Используем МСК
    expired_rentals = db_query("SELECT id, account_id FROM rentals WHERE end_time <= ? AND is_history = 0", (now_iso,),
                               fetch="all")
    if not expired_rentals: return set()
    freed_game_ids = set()
    for rental_id, account_id in expired_rentals:
        move_rental_to_history(rental_id)
        if account_id:
            game_id_res = db_query("SELECT game_id FROM accounts WHERE id = ?", (account_id,), fetch="one")
            if game_id_res:
                freed_game_ids.add(game_id_res[0])
    return freed_game_ids


def get_rentals_for_reminder():
    now_iso = datetime.now(MOSCOW_TZ).isoformat()  # <-- Используем МСК
    return db_query(
        "SELECT id, client_name, funpay_chat_id FROM rentals WHERE remind_time <= ? AND is_history = 0 AND pre_reminded = 0",
        (now_iso,), fetch="all")


def mark_rental_as_reminded(rental_id):
    db_query("UPDATE rentals SET pre_reminded = 1 WHERE id = ?", (rental_id,))


def get_all_game_names():
    games = db_query("SELECT name FROM games ORDER BY name", fetch="all")
    return [g[0] for g in games] if games else []


def extend_user_rental(username, hours_to_add):
    rental = db_query(
        "SELECT id, end_time, initial_minutes FROM rentals WHERE client_name = ? AND is_history = 0 ORDER BY end_time DESC LIMIT 1",
        (username,), fetch="one")
    if not rental: return None
    rental_id, current_end_iso, initial_minutes = rental
    current_end_time = datetime.fromisoformat(current_end_iso)
    minutes_to_add = hours_to_add * 60
    new_end_time = current_end_time + timedelta(minutes=minutes_to_add)
    new_remind_time = new_end_time - timedelta(minutes=10)
    new_total_minutes = (initial_minutes or 0) + minutes_to_add
    db_query(
        "UPDATE rentals SET end_time = ?, remind_time = ?, initial_minutes = ?, reminded = 0, pre_reminded = 0 WHERE id = ?",
        (new_end_time.isoformat(), new_remind_time.isoformat(), new_total_minutes, rental_id))
    return new_end_time


def set_game_offer_ids(game_id, offer_ids_str):
    db_query("UPDATE games SET funpay_offer_ids = ? WHERE id = ?", (offer_ids_str, game_id))


def add_offer_id_to_game(game_id: int, offer_id: int):
    """
    Добавляет ID нового лота к игре, избегая дубликатов.
    """
    if not game_id or not offer_id:
        return

    try:
        # 1. Получаем текущий список ID
        current_ids_str = db_query("SELECT funpay_offer_ids FROM games WHERE id = ?", (game_id,), fetch="one")
        if not current_ids_str:
            # Если у игры вообще не было списка, создаем новый
            new_ids_str = str(offer_id)
        else:
            # 2. Преобразуем строку в множество, чтобы избежать дублей
            current_ids = set(current_ids_str[0].split(',')) if current_ids_str[0] else set()

            # 3. Добавляем новый ID
            current_ids.add(str(offer_id))

            # 4. Собираем обратно в отсортированную строку
            new_ids_str = ",".join(sorted(list(current_ids), key=int))

        # 5. Сохраняем новый список в БД
        set_game_offer_ids(game_id, new_ids_str)
        logging.info(f"[DB] Лот {offer_id} успешно привязан к игре {game_id}.")

    except Exception as e:
        logging.error(f"[DB] Ошибка при добавлении лота {offer_id} к игре {game_id}: {e}")