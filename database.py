# database.py

import sqlite3
import logging
from config import DB_FILE

def init_database():
    """Создает таблицы в БД, если они не существуют."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS games
                           (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)
                           ''')
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS accounts
                           (id INTEGER PRIMARY KEY AUTOINCREMENT, login TEXT NOT NULL, password TEXT NOT NULL, 
                            game_id INTEGER NOT NULL, rented_by TEXT,
                            FOREIGN KEY (game_id) REFERENCES games (id) ON DELETE CASCADE)
                           ''')
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS rentals
                           (id TEXT PRIMARY KEY, client_name TEXT NOT NULL, account_id INTEGER, 
                            start_time TEXT NOT NULL, end_time TEXT NOT NULL, remind_time TEXT NOT NULL,
                            initial_minutes INTEGER, info TEXT, reminded INTEGER DEFAULT 0, is_history INTEGER DEFAULT 0,
                            FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE SET NULL)
                           ''')
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Failed to initialize database: {e}")
        # Мы логируем ошибку, а обработка (показ messagebox) будет в main.py
        raise e

def db_query(query, params=(), fetch=None):
    """Универсальная функция для выполнения запросов к БД."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute(query, params)
            conn.commit()
            if fetch == "one": return cursor.fetchone()
            if fetch == "all": return cursor.fetchall()
            return cursor
    except sqlite3.Error as e:
        logging.error(f"DB Error: '{e}'\nQuery: '{query}'\nParams: '{params}'")
        return None