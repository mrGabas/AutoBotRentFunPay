import logging
import os
import threading
import time

from FunPayAPI.account import Account
import config
import db_handler
from bot_handler import funpay_bot_listener, expired_rentals_checker, sync_games_with_funpay_offers
import telegram_bot
import shared  # <-- Импортируем наш новый модуль


def main():
    # Настройка логирования
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(config.LOG_FILE, 'a', 'utf-8')
    file_handler.setFormatter(log_formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)

    logging.info("=" * 30)
    logging.info("Начало запуска серверного бота...")

    db_handler.initialize_and_update_db()

    try:
        # ИНИЦИАЛИЗИРУЕМ АККАУНТ И КЛАДЕМ ЕГО В ОБЩЕЕ ХРАНИЛИЩЕ
        shared.funpay_account = Account(golden_key=config.GOLDEN_KEY, user_agent=config.USER_AGENT)
        shared.funpay_account.get()
        logging.info(f"Авторизация на FunPay как '{shared.funpay_account.username}' (ID: {shared.funpay_account.id}).")
    except Exception as e:
        logging.critical(f"Не удалось авторизоваться на FunPay. Проверьте токен. Ошибка: {e}")
        return

    # Запускаем первоначальную синхронизацию в отдельном потоке
    sync_thread = threading.Thread(target=sync_games_with_funpay_offers, args=(shared.funpay_account,), daemon=True)
    sync_thread.start()

    # Запускаем остальные потоки, передавая им тот же объект аккаунта
    funpay_thread = threading.Thread(target=funpay_bot_listener, args=(shared.funpay_account,), daemon=True)
    funpay_thread.start()
    logging.info("Поток прослушивания FunPay запущен.")

    checker_thread = threading.Thread(target=expired_rentals_checker, args=(shared.funpay_account,), daemon=True)
    checker_thread.start()
    logging.info("Поток проверки статусов запущен.")

    # Запускаем Telegram-бота
    telegram_bot.start_telegram_bot()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("Получен сигнал о завершении работы...")
        telegram_bot.stop_telegram_bot()
        logging.info("Серверный бот остановлен.")


if __name__ == "__main__":
    main()