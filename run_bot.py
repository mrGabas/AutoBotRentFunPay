# run_bot.py
import logging
import os
import threading
import time
from queue import Queue

from FunPayAPI.account import Account
import config
import db_handler
from bot_handler import funpay_bot_listener, expired_rentals_checker, sync_games_with_funpay_offers
import telegram_bot  # Импортируем наш новый модуль


def main():
    # 1. Настройка логирования
    log_dir = os.path.dirname(config.LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(config.LOG_FILE, 'a', 'utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.getLogger('apscheduler').setLevel(logging.WARNING)

    logging.info("=" * 30)
    logging.info("Начало запуска серверного бота...")

    # 2. Инициализация и обновление БД
    try:
        db_handler.initialize_and_update_db()
    except Exception as e:
        logging.critical(f"Не удалось инициализировать БД. Выход. Ошибка: {e}")
        return

    # 3. Инициализация аккаунта FunPay
    try:
        if not config.GOLDEN_KEY or not config.USER_AGENT:
            raise ValueError("GOLDEN_KEY или USER_AGENT не указаны в config.py")
        account = Account(golden_key=config.GOLDEN_KEY, user_agent=config.USER_AGENT)

        # <<< ИСПРАВЛЕНИЕ: Добавлена инициализация аккаунта для загрузки данных >>>
        account.get()
        # <<< КОНЕЦ ИСПРАВЛЕНИЯ >>>

        logging.info(f"Авторизация на FunPay как '{account.username}' (ID: {account.id}).")
    except Exception as e:
        logging.critical(f"Не удалось авторизоваться на FunPay. Проверьте токен. Ошибка: {e}")
        return

    # 4. Первичная синхронизация лотов с БД
    try:
        sync_games_with_funpay_offers(account)
    except Exception as e:
        logging.error(f"Произошла ошибка во время первичной синхронизации лотов: {e}")

    # 5. Запуск фоновых потоков

    # Поток для прослушивания событий FunPay
    funpay_thread = threading.Thread(target=funpay_bot_listener, args=(account, None), daemon=True)
    funpay_thread.start()
    logging.info("Поток прослушивания FunPay запущен.")

    # Поток для проверки истекших аренд и статусов лотов
    checker_thread = threading.Thread(target=expired_rentals_checker, args=(account,), daemon=True)
    checker_thread.start()
    logging.info("Поток проверки статусов запущен.")

    # Запуск Telegram-бота
    telegram_bot.start_bot()

    try:
        while True:
            time.sleep(3600)  # Главный поток спит, пока остальные работают
    except KeyboardInterrupt:
        logging.info("Получен сигнал о завершении работы...")
        # Остановка Telegram-бота при выходе
        telegram_bot.stop_bot()
        logging.info("Серверный бот остановлен.")


if __name__ == "__main__":
    main()