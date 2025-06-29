# db_handler.py
import sqlite3
import logging
from datetime import datetime, timedelta
import uuid
from config import DB_FILE
from database import db_query, init_database

def _check_column_exists(cursor, table_name, column_name):
    """Проверяет наличие колонки в таблице."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns

def initialize_and_update_db():
    """Инициализирует БД и обновляет схему, если это необходимо."""
    logging.info("Проверка и инициализация базы данных...")
    init_database()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            if not _check_column_exists(cursor, "games", "funpay_offer_ids"):
                logging.info("Обновление схемы: добавление 'funpay_offer_ids' в таблицу 'games'.")
                cursor.execute("ALTER TABLE games ADD COLUMN funpay_offer_ids TEXT")
            if not _check_column_exists(cursor, "rentals", "funpay_chat_id"):
                cursor.execute("ALTER TABLE rentals ADD COLUMN funpay_chat_id TEXT")
            conn.commit()
            logging.info("Схема базы данных актуальна.")
    except sqlite3.Error as e:
        logging.critical(f"КРИТИЧЕСКАЯ ОШИБКА при обновлении схемы БД: {e}")
        raise

def rent_account(game_name, client_name, minutes, chat_id):
    """Сдает аккаунт в аренду. Возвращает (login, password, game_id) или None."""
    game_id_res = db_query("SELECT id FROM games WHERE name LIKE ?", (f"%{game_name}%",), fetch="one")
    if not game_id_res:
        logging.warning(f"Попытка аренды несуществующей игры: {game_name}")
        return None
    game_id = game_id_res[0]

    free_account = db_query(
        "SELECT id, login, password FROM accounts WHERE game_id = ? AND (rented_by IS NULL OR rented_by = '') LIMIT 1",
        (game_id,),
        fetch="one"
    )
    if not free_account:
        logging.warning(f"Нет свободных аккаунтов для игры '{game_name}'.")
        return None

    acc_id, login, password = free_account
    now = datetime.now()
    end_time = now + timedelta(minutes=minutes)
    remind_time = end_time - timedelta(minutes=10)

    rental_id = str(uuid.uuid4())
    db_query(
        "INSERT INTO rentals (id, client_name, account_id, start_time, end_time, remind_time, initial_minutes, funpay_chat_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (rental_id, client_name, acc_id, now.isoformat(), end_time.isoformat(), remind_time.isoformat(), minutes, str(chat_id))
    )
    db_query("UPDATE accounts SET rented_by = ? WHERE id = ?", (client_name, acc_id))
    logging.info(f"Аккаунт {login} ({game_name}) сдан клиенту {client_name}. Game_id: {game_id}")
    return login, password, game_id

def check_and_process_expired_rentals():
    """Проверяет истекшие аренды, освобождает аккаунты и возвращает set с game_id освобожденных аккаунтов."""
    now_iso = datetime.now().isoformat()
    expired_rentals = db_query(
        "SELECT id, account_id FROM rentals WHERE end_time <= ? AND is_history = 0",
        (now_iso,),
        fetch="all"
    )
    if not expired_rentals:
        return set()

    freed_game_ids = set()
    for rental_id, account_id in expired_rentals:
        db_query("UPDATE rentals SET is_history = 1 WHERE id = ?", (rental_id,))
        if account_id:
            game_id_res = db_query("SELECT game_id FROM accounts WHERE id = ?", (account_id,), fetch="one")
            if game_id_res:
                freed_game_ids.add(game_id_res[0])
            db_query("UPDATE accounts SET rented_by = NULL WHERE id = ?", (account_id,))
            logging.info(f"Аренда {rental_id} истекла. Аккаунт ID:{account_id} освобожден.")
    return freed_game_ids

def get_game_id_by_name(game_name):
    """Получает ID игры по ее имени."""
    result = db_query("SELECT id FROM games WHERE name = ?", (game_name,), fetch="one")
    return result[0] if result else None

def set_game_offer_ids(game_id, offer_ids_str):
    """Сохраняет строку с ID лотов FunPay для конкретной игры."""
    db_query("UPDATE games SET funpay_offer_ids = ? WHERE id = ?", (offer_ids_str, game_id))
    logging.info(f"Для game_id {game_id} установлены лоты: {offer_ids_str}")

def get_all_game_names():
    """Возвращает список имен всех игр."""
    games = db_query("SELECT name FROM games ORDER BY name", fetch="all")
    return [g[0] for g in games] if games else []

def get_games_stats():
    """Возвращает статистику по играм (название, всего акк, свободно акк)."""
    return db_query("""
        SELECT g.name,
               COUNT(a.id) as total,
               SUM(CASE WHEN a.rented_by IS NULL THEN 1 ELSE 0 END) as free
        FROM games g
        LEFT JOIN accounts a ON g.id = a.game_id
        GROUP BY g.id
        ORDER BY g.name
    """, fetch="all")

def get_user_rental_info(username):
    """Получает информацию об активной аренде пользователя."""
    return db_query(
        "SELECT end_time FROM rentals WHERE client_name = ? AND is_history = 0 ORDER BY end_time DESC LIMIT 1",
        (username,),
        fetch="one"
    )

def extend_user_rental(username, hours_to_add):
    """Продлевает активную аренду пользователя."""
    rental = db_query(
        "SELECT id, end_time, initial_minutes FROM rentals WHERE client_name = ? AND is_history = 0 ORDER BY end_time DESC LIMIT 1",
        (username,),
        fetch="one"
    )
    if not rental:
        return None

    rental_id, current_end_iso, initial_minutes = rental
    current_end_time = datetime.fromisoformat(current_end_iso)
    minutes_to_add = hours_to_add * 60

    new_end_time = current_end_time + timedelta(minutes=minutes_to_add)
    new_remind_time = new_end_time - timedelta(minutes=10)
    new_total_minutes = (initial_minutes or 0) + minutes_to_add

    db_query(
        "UPDATE rentals SET end_time = ?, remind_time = ?, initial_minutes = ?, reminded = 0, pre_reminded = 0 WHERE id = ?",
        (new_end_time.isoformat(), new_remind_time.isoformat(), new_total_minutes, rental_id)
    )
    logging.info(f"Аренда для {username} продлена на {hours_to_add} ч. Новое время окончания: {new_end_time.isoformat()}")
    return new_end_time

# --- Обработчики для бота FunPay ---

def set_game_offer_ids(game_id, offer_ids_str):
    """Сохраняет строку с ID лотов FunPay для конкретной игры."""
    db_query("UPDATE games SET funpay_offer_ids = ? WHERE id = ?", (offer_ids_str, game_id))
