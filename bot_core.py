# bot.py (или bot_core.py, как у вас)
import logging
import re
import time
import threading
from FunPayAPI.account import Account
from FunPayAPI.updater.runner import Runner
from FunPayAPI.common.enums import EventTypes
from config_bot_bot import GOLDEN_KEY, USER_AGENT
import db_handler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def background_checker():
    logging.info("Фоновый процесс проверки аренд запущен.")
    while True:
        try:
            logging.info("Проверка истекших аренд...")
            processed_accounts = db_handler.check_and_process_expired_rentals()

            if processed_accounts:
                logins = [acc['login'] for acc in processed_accounts]
                logging.info(f"Найдено и обработано {len(processed_accounts)} истекших аренд. "
                             f"Аккаунты, которые были освобождены: {', '.join(logins)}")
            else:
                logging.info("Истекших аренд не найдено.")

        except Exception as e:
            logging.error(f"Ошибка в фоновом процессе: {e}")

        time.sleep(300)  # Пауза 5 минут


def main():
    if not GOLDEN_KEY or "вставьте" in GOLDEN_KEY:
        logging.error("Пожалуйста, укажите ваш GOLDEN_KEY в файле config_bot.py")
        return
    if not USER_AGENT or "Mozilla" not in USER_AGENT:
        logging.error("Пожалуйста, укажите ваш USER_AGENT в файле config.py")
        return

    logging.info("Инициализация аккаунта...")
    try:
        account = Account(golden_key=GOLDEN_KEY, user_agent=USER_AGENT)
        account.get()
        logging.info(f"Успешная авторизация как: {account.username}.")
    except Exception as e:
        logging.error(f"Ошибка при инициализации аккаунта: {e}")
        return

    checker_thread = threading.Thread(target=background_checker, daemon=True)
    checker_thread.start()

    runner = Runner(account)
    logging.info("Запуск прослушивания событий FunPay...")

    for event in runner.listen():
        try:
            if event.type == EventTypes.NEW_MESSAGE:
                # ... (весь ваш код обработки команд остается тут) ...
                message = event.message
                if message.author_id == account.id or not message.text:
                    continue
                # и так далее...

        except Exception as e:
            logging.error(f"Ошибка при обработке события: {e}")


if __name__ == "__main__":
    main()