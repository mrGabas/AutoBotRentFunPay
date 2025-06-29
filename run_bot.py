# run_bot.py
import logging
import time
import threading
from FunPayAPI.account import Account

import db_handler
from bot_handler import funpay_bot_listener, expired_rentals_checker, sync_games_with_funpay_offers, update_offer_status_for_game
from config import LOG_FILE
from config import GOLDEN_KEY, USER_AGENT

if __name__ == "__main__":
    # 1. Настройка логирования
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(LOG_FILE, 'a', 'utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.info("=" * 30)
    logging.info("СЕРВЕРНЫЙ БОТ ЗАПУСКАЕТСЯ...")

    # 2. Инициализация и обновление БД
    try:
        db_handler.initialize_and_update_db()
    except Exception as e:
        logging.critical(f"Не удалось инициализировать БД. Бот остановлен.", exc_info=True)
        exit()

    # 3. Инициализация аккаунта FunPay
    try:
        logging.info("Инициализация FunPay аккаунта...")
        account = Account(golden_key=GOLDEN_KEY, user_agent=USER_AGENT)
        account.get()
        logging.info(f"Успешная авторизация как: {account.username}.")
    except Exception as e:
        logging.critical(f"Не удалось авторизоваться в FunPay. Бот остановлен.", exc_info=True)
        exit()

    # 4. Разовая синхронизация лотов при запуске
    sync_games_with_funpay_offers(account)

    # 5. Запуск фоновых процессов
    # Поток для прослушивания событий FunPay (новые заказы, сообщения)
    listener_thread = threading.Thread(target=funpay_bot_listener, args=(account, None), daemon=True)
    listener_thread.start()

    # Поток для проверки истекших аренд и управления лотами
    expiry_thread = threading.Thread(target=expired_rentals_checker, args=(account,), daemon=True)
    expiry_thread.start()

    logging.info("Все фоновые процессы запущены. Бот в рабочем режиме.")

    # 6. Главный цикл для поддержания работы скрипта
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logging.info("Получен сигнал на остановку. Завершение работы...")