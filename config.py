# config.py
import os
import sys
from dotenv import load_dotenv
load_dotenv()
# --- ОБЩИЕ НАСТРОЙКИ ---
if getattr(sys, 'frozen', False):
    # Если приложение скомпилировано в .exe
    SAVE_FOLDER = os.path.dirname(sys.executable)
else:
    # Если запускается как .py скрипт
    SAVE_FOLDER = os.path.dirname(os.path.abspath(__file__))

DB_FILE = os.path.join(SAVE_FOLDER, "rentals.db")
LOG_FILE = os.path.join(SAVE_FOLDER, 'rentals_app.log')


# --- НАСТРОЙКИ СЕРВЕРНОГО БОТА ---
GOLDEN_KEY = os.getenv("GOLDEN_KEY")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

# --- ДАННЫЕ ДЛЯ TELEGRAM ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_ADMIN_CHAT_ID = "1123028915"

# --- НАСТРОЙКИ УПРАВЛЕНИЯ ЛОТАМИ ---
USE_EXPIRATION_GRACE_PERIOD = True
EXPIRATION_GRACE_PERIOD_MINUTES = 10
RENTAL_KEYWORDS = ['аренда', 'час', 'часа', 'часов', 'h', 'day', 'days', 'день', 'дня', 'дней']

# Использовать задержку перед повторной активацией лота после окончания аренды?
USE_EXPIRATION_GRACE_PERIOD = True
# Длительность задержки в минутах.
EXPIRATION_GRACE_PERIOD_MINUTES = 10