# bot_handler.py
import logging
import re
import time
import threading
from datetime import datetime
import pytz

from FunPayAPI.account import Account
from FunPayAPI.updater.runner import Runner
from FunPayAPI.common.enums import EventTypes, SubCategoryTypes
from config import RENTAL_KEYWORDS, USE_EXPIRATION_GRACE_PERIOD, EXPIRATION_GRACE_PERIOD_MINUTES
import db_handler
from telegram_bot import send_telegram_notification, send_telegram_alert
import localization
from utils import format_timedelta
import state_manager

# Глобальная переменная для поочередной проверки игр
game_check_index = 0


def sync_games_with_funpay_offers(account: Account):
    """
    Сканирует все лоты на FunPay и сопоставляет их с играми в БД.
    Отправляет уведомления в Telegram о ходе процесса.
    """
    send_telegram_notification("🚀 Начинаю полную синхронизацию лотов с FunPay...")
    logging.info("[SYNC] Запуск синхронизации игр с лотами FunPay.")
    try:
        db_games = db_handler.db_query("SELECT id, name FROM games", fetch="all")
        if not db_games:
            logging.warning("[SYNC] В базе данных нет игр.")
            send_telegram_notification("⚠️ В базе данных нет игр для синхронизации.")
            return

        all_offers = account.get_user(account.id).get_lots()
        if not all_offers:
            logging.warning("[SYNC] Не удалось получить лоты с FunPay.")
            send_telegram_notification("❌ Не удалось получить список лотов с FunPay.")
            return

        logging.info(f"[SYNC] Найдено {len(all_offers)} лотов на аккаунте. Начинаю анализ.")

        total_found = 0
        for game_id, game_name in db_games:
            found_ids = []
            for offer in all_offers:
                offer_text = (offer.description or "").lower()
                if offer.subcategory and offer.subcategory.category:
                    offer_text += " " + offer.subcategory.category.name.lower()

                if game_name.lower() in offer_text and any(kw in offer_text for kw in RENTAL_KEYWORDS):
                    found_ids.append(str(offer.id))

            if found_ids:
                db_handler.add_offer_id_to_game(game_id, found_ids)
                total_found += len(found_ids)

        send_telegram_notification(f"✅ Синхронизация лотов завершена. Найдено и обновлено {total_found} ID.")

        logging.info("[SYNC_CHECK] Запуск проверки статусов всех лотов.")
        for game_id, _ in db_games:
            update_offer_status_for_game(account, game_id)
            time.sleep(2)  # Небольшая задержка между запросами
        send_telegram_notification("ℹ️ Проверка и обновление статусов лотов на FunPay завершены.")

    except Exception as e:
        logging.exception(f"[SYNC] Ошибка во время синхронизации: {e}")
        send_telegram_alert(f"❌ Произошла ошибка во время синхронизации:\n`{e}`")


def update_offer_status_for_game(account: Account, game_id: int):
    """Обновляет статус лотов для игры, учитывая глобальные переключатели."""
    if not game_id: return
    try:
        game_data = db_handler.db_query("""
            SELECT g.funpay_offer_ids, 
                   (SELECT COUNT(*) FROM accounts a WHERE a.game_id = g.id AND a.rented_by IS NULL)
            FROM games g WHERE g.id = ?
        """, (game_id,), fetch="one")
        if not (game_data and game_data[0]): return

        offer_ids_str, free_accounts = game_data
        offer_ids = {int(i.strip()) for i in offer_ids_str.split(',') if i.strip().isdigit()}

        for offer_id in offer_ids:
            try:
                fields = account.get_lot_fields(offer_id)
                is_active = fields.active

                # Логика АКТИВАЦИИ лота
                if free_accounts > 0 and not is_active:
                    # Включаем лот ТОЛЬКО ЕСЛИ разрешено глобально
                    if state_manager.are_lots_enabled:
                        logging.info(f"[LOT_MANAGER] Активация лота {offer_id}.")
                        fields.active = True
                        account.save_lot(fields)
                        send_telegram_notification(f"✅ Лот {offer_id} АКТИВИРОВАН.")
                        time.sleep(3)
                    else:
                        logging.info(f"[LOT_MANAGER] Активация лота {offer_id} пропущена (управление отключено).")

                # Логика ДЕАКТИВАЦИИ лота
                elif free_accounts == 0 and is_active:
                    logging.info(f"[LOT_MANAGER] Деактивация лота {offer_id} (нет свободных аккаунтов).")
                    fields.active = False
                    account.save_lot(fields)
                    send_telegram_notification(f"⛔️ Лот {offer_id} ДЕАКТИВИРОВАН.")
                    time.sleep(3)
            except Exception as e:
                logging.error(f"[LOT_MANAGER] Ошибка обработки лота {offer_id}: {e}")
    except Exception as e:
        logging.exception(f"[LOT_MANAGER] Ошибка обновления статуса лотов для game_id {game_id}.")

def _force_deactivate_all_lots(account: Account):
    """
    Находит все лоты из БД и принудительно деактивирует их.
    """
    logging.warning("[FORCE_DEACTIVATE] ЗАПУСК ПРИНУДИТЕЛЬНОЙ ДЕАКТИВАЦИИ ВСЕХ ЛОТОВ.")
    all_offer_ids = set()
    try:
        games_with_ids = db_handler.db_query("SELECT funpay_offer_ids FROM games WHERE funpay_offer_ids IS NOT NULL",
                                             fetch="all")
        for (ids_str,) in games_with_ids:
            if ids_str:
                all_offer_ids.update([int(i.strip()) for i in ids_str.split(',') if i.strip().isdigit()])

        if not all_offer_ids:
            send_telegram_notification("ℹ️ Не найдено лотов для деактивации.")
            return

        deactivated_count = 0
        for offer_id in all_offer_ids:
            try:
                fields = account.get_lot_fields(offer_id)
                if fields.active:
                    fields.active = False
                    account.save_lot(fields)
                    logging.info(f"[FORCE_DEACTIVATE] Лот {offer_id} успешно деактивирован.")
                    deactivated_count += 1
                    time.sleep(3)
            except Exception as e:
                logging.error(f"[FORCE_DEACTIVATE] Не удалось отключить лот {offer_id}: {e}")

        send_telegram_notification(f"✅ Принудительная деактивация завершена. Отключено: {deactivated_count} лот(ов).")

    except Exception as e:
        send_telegram_alert(f"Критическая ошибка при принудительной деактивации лотов: {e}")

def expired_rentals_checker(account: Account):
    """Фоновый процесс проверки статусов."""
    logging.info("[SYNC_CHECKER] Запущен объединенный проверщик статусов.")
    game_ids = [g[0] for g in db_handler.db_query("SELECT id FROM games", fetch="all")]
    game_check_index = 0
    while True:
        try:
            if state_manager.force_deactivate_all_lots_requested:
                _force_deactivate_all_lots(account)
                state_manager.force_deactivate_all_lots_requested = False  # Сбрасываем флаг
            # <<< ИСПРАВЛЕНИЕ: Убрана ошибочная проверка state_manager.deactivate_all_lots_requested >>>
            if not state_manager.is_bot_enabled:
                time.sleep(30)
                continue

            # Обработка истекших аренд
            freed_game_ids = db_handler.check_and_process_expired_rentals()
            for game_id in freed_game_ids:
                update_offer_status_for_game(account, game_id)

            # Поочередная проверка статусов лотов для отлова ручных изменений
            if game_ids:
                if game_check_index >= len(game_ids):
                    game_check_index = 0

                current_game_id = game_ids[game_check_index]
                if current_game_id not in freed_game_ids:
                    update_offer_status_for_game(account, current_game_id)

                game_check_index += 1
        except Exception as e:
            logging.exception(f"Ошибка в процессе фоновой синхронизации статусов.")
        time.sleep(60)

def expired_rentals_checker(account: Account):
    """
    Фоновый процесс, который:
    1. Проверяет и отправляет 10-минутные напоминания.
    2. Проверяет и обрабатывает истекшие аренды.
    3. Применяет 10-минутную задержку перед повторной активацией лота.
    4. Выполняет принудительное отключение лотов по команде.
    5. Поочередно проверяет по одной игре для синхронизации статусов лотов.
    """
    logging.info("[CHECKER] Запущен объединенный проверщик статусов.")
    game_ids = [g[0] for g in db_handler.db_query("SELECT id FROM games", fetch="all")]
    game_check_index = 0

    while True:
        try:
            # 1. Выполнение команды на принудительное отключение лотов
            if state_manager.force_deactivate_all_lots_requested:
                _force_deactivate_all_lots(account)
                state_manager.force_deactivate_all_lots_requested = False

            if not state_manager.is_bot_enabled:
                time.sleep(30)
                continue

            # 2. Проверка и отправка 10-минутных напоминаний
            reminders_to_send = db_handler.get_rentals_for_reminder()
            if reminders_to_send:
                logging.info(f"[CHECKER_REMINDER] Найдено {len(reminders_to_send)} аренд для отправки напоминаний.")
                for rental_id, client_name, chat_id in reminders_to_send:
                    lang = 'ru'
                    reminder_text = localization.get_text('RENTAL_ENDING_SOON', lang)
                    try:
                        account.send_message(chat_id, reminder_text, chat_name=client_name)
                        db_handler.mark_rental_as_reminded(rental_id)
                        logging.info(
                            f"[CHECKER_REMINDER] Напоминание для аренды {rental_id} успешно отправлено в чат {chat_id}.")
                    except Exception as e:
                        logging.error(
                            f"[CHECKER_REMINDER] Не удалось отправить напоминание для аренды {rental_id}: {e}")
                    time.sleep(2)

            # 3. Обработка истекших аренд
            freed_game_ids = db_handler.check_and_process_expired_rentals()
            if freed_game_ids:
                logging.info(f"[CHECKER_EXPIRED] Освобождены аккаунты для игр (game_ids): {freed_game_ids}.")
                for game_id in freed_game_ids:
                    # Применяем задержку, если она включена в конфиге
                    if USE_EXPIRATION_GRACE_PERIOD:
                        delay = EXPIRATION_GRACE_PERIOD_MINUTES * 60
                        logging.info(
                            f"[CHECKER_GRACE] Установлена пауза {EXPIRATION_GRACE_PERIOD_MINUTES} мин. перед активацией лотов для game_id {game_id}.")
                        threading.Timer(delay, update_offer_status_for_game, args=[account, game_id]).start()
                    else:
                        # Если задержка выключена, активируем сразу
                        update_offer_status_for_game(account, game_id)

            # 4. Поочередная проверка статусов лотов для отлова ручных изменений
            if game_ids:
                if game_check_index >= len(game_ids):
                    game_check_index = 0

                current_game_id = game_ids[game_check_index]
                if current_game_id not in freed_game_ids:
                    update_offer_status_for_game(account, current_game_id)

                game_check_index += 1
        except Exception as e:
            logging.exception(f"Ошибка в процессе фоновой синхронизации статусов.")
        time.sleep(60)

def funpay_bot_listener(account, update_queue):
    """Основной обработчик событий FunPay."""
    runner = Runner(account)
    logging.info("FunPay обработчик событий запущен.")
    while True:
        try:
            for event in runner.listen():
                if not state_manager.is_bot_enabled:
                    if event.type == EventTypes.NEW_ORDER or event.type == EventTypes.NEW_MESSAGE:
                        logging.info(f"[BOT_DISABLED] Получено событие {event.type}, но бот выключен. Игнорирую.")
                    time.sleep(5)
                    continue
                if event.type == EventTypes.NEW_ORDER:
                    order = event.order
                    logging.info(f"[BOT] Обнаружен новый заказ #{order.id} от {order.buyer_username}.")
                    all_games = {g[1]: g[0] for g in db_handler.db_query("SELECT id, name FROM games", fetch="all")}
                    detected_game_name = next((name for name in all_games if name.lower() in order.description.lower()),
                                              None)

                    if not detected_game_name and order.subcategory and order.subcategory.category:
                        detected_game_name = order.subcategory.category.name

                    if detected_game_name and detected_game_name in all_games:
                        game_id = all_games[detected_game_name]
                        # Убеждаемся, что у заказа есть ID лота
                        if hasattr(order, 'offer') and hasattr(order.offer, 'id'):
                            logging.info(f"[BOT] Обнаружен ID лота: {order.offer.id} для игры '{detected_game_name}'.")
                            db_handler.add_offer_id_to_game(game_id, order.offer.id)
                        else:
                            logging.warning(f"[BOT] Не удалось получить ID лота из заказа #{order.id}.")
                    description_lower = order.description.lower()

                    # Проверяем, содержит ли описание ключевые слова для аренды
                    if not any(keyword in description_lower for keyword in RENTAL_KEYWORDS):
                        logging.info(f"[BOT] Заказ #{order.id} проигнорирован (не является арендой).")
                        continue

                    all_games_in_db = db_handler.get_all_game_names()
                    detected_game_name = next((game for game in all_games_in_db if game.lower() in description_lower),
                                              None)
                    if not detected_game_name and order.subcategory and order.subcategory.category:
                        detected_game_name = order.subcategory.category.name

                    if not detected_game_name:
                        send_telegram_alert(f"Не удалось определить ИГРУ для заказа `#{order.id}`.")
                        continue

                    match = re.search(r'(\d+)\s*(час|часа|часов|ч|д|дней|день|day|days)', description_lower)
                    if not match:
                        send_telegram_alert(f"Не удалось определить СРОК для заказа `#{order.id}`.")
                        continue

                    time_value = int(match.group(1))
                    time_unit = match.group(2)
                    total_minutes = (time_value * 1440) if time_unit in ['д', 'дней', 'день', 'day', 'days'] else (
                            time_value * 60)
                    total_minutes *= order.amount

                    rental_data = db_handler.rent_account(detected_game_name, order.buyer_username, total_minutes,
                                                          order.chat_id)

                    if rental_data:
                        login, password, game_id = rental_data
                        lang = 'ru'
                        response_text = localization.get_text('RENTAL_SUCCESS', lang).format(
                            game_name=detected_game_name, login=login, password=password,
                            total_hours=round(total_minutes / 60, 1))
                        account.send_message(order.chat_id, response_text, chat_name=order.buyer_username)
                        update_offer_status_for_game(account, game_id)
                    else:
                        lang = 'ru'
                        response_text = localization.get_text('NO_ACCOUNTS_AVAILABLE_USER', lang)
                        account.send_message(order.chat_id, response_text, chat_name=order.buyer_username)
                        send_telegram_alert(
                            f"НЕТ СВОБОДНЫХ АККАУНТОВ для '{detected_game_name}' по заказу `#{order.id}`.")

                # ИСПРАВЛЕНИЕ: Добавлен блок обработки команд из чата
                elif event.type == EventTypes.NEW_MESSAGE:
                    message = event.message
                    # Игнорируем свои же сообщения
                    if message.author_id == account.id or not message.text:
                        continue

                    logging.info(f"[BOT] Новое сообщение от '{message.author}': {message.text}")
                    lang = 'ru'  # или можно добавить логику определения языка
                    cmd_text = message.text.lower().strip()

                    # Команда !помощь
                    if cmd_text == '!помощь' or cmd_text == '!help':
                        logging.info(f"[BOT] Получена команда !помощь от {message.author}")
                        help_text = localization.get_text('HELP_MESSAGE', lang)
                        account.send_message(message.chat_id, help_text, chat_name=message.author)

                    # Команда !игры
                    elif cmd_text == '!игры' or cmd_text == '!games':
                        logging.info(f"[BOT] Получена команда !игры от {message.author}")
                        stats = db_handler.get_games_stats()
                        if not stats:
                            response = localization.get_text('NO_GAMES_AVAILABLE', lang)
                        else:
                            response = localization.get_text('GAMES_HEADER', lang) + "\n"
                            response += "\n".join([f"• {name}: {total} / {free}" for name, total, free in stats])
                        account.send_message(message.chat_id, response, chat_name=message.author)

                    # Команда !время
                    elif cmd_text == '!время' or cmd_text == '!time':
                        logging.info(f"[BOT] Получена команда !время от {message.author}")
                        rental_info = db_handler.get_user_rental_info(message.author)
                        if not rental_info:
                            response = localization.get_text('NO_ACTIVE_RENTALS', lang)
                        else:
                            end_time_str = rental_info[0]
                            end_time = datetime.fromisoformat(end_time_str)
                            now = datetime.now()
                            if end_time < now:
                                response = localization.get_text('RENTAL_EXPIRED', lang)
                            else:
                                remaining = end_time - now
                                msk_tz = pytz.timezone('Europe/Moscow')
                                utc_tz = pytz.utc
                                end_time_msk = end_time.astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
                                end_time_utc = end_time.astimezone(utc_tz).strftime('%Y-%m-%d %H:%M:%S')
                                response = localization.get_text('RENTAL_INFO', lang).format(
                                    remaining_time=format_timedelta(remaining),
                                    end_time_msk=end_time_msk,
                                    end_time_utc=end_time_utc
                                )
                        account.send_message(message.chat_id, response, chat_name=message.author)

                    # Команда !продлить
                    elif cmd_text.startswith('!продлить') or cmd_text.startswith('!extend'):
                        logging.info(f"[BOT] Получена команда !продлить от {message.author}")
                        parts = cmd_text.split()
                        if len(parts) < 2 or not parts[1].isdigit():
                            response = localization.get_text('INVALID_EXTEND_FORMAT', lang)
                        else:
                            hours_to_add = int(parts[1])
                            new_end_time = db_handler.extend_user_rental(message.author, hours_to_add)
                            if not new_end_time:
                                response = localization.get_text('NO_RENTAL_TO_EXTEND', lang)
                            else:
                                msk_tz = pytz.timezone('Europe/Moscow')
                                utc_tz = pytz.utc
                                end_time_msk = new_end_time.astimezone(msk_tz).strftime('%Y-%m-%d %H:%M:%S')
                                end_time_utc = new_end_time.astimezone(utc_tz).strftime('%Y-%m-%d %H:%M:%S')
                                response = localization.get_text('EXTEND_SUCCESS', lang).format(
                                    hours=hours_to_add,
                                    end_time_msk=end_time_msk,
                                    end_time_utc=end_time_utc
                                )
                        account.send_message(message.chat_id, response, chat_name=message.author)

        except Exception as e:
            logging.exception(f"[BOT_LISTENER] Критическая ошибка в главном цикле.")
            send_telegram_alert(f"Критическая ошибка в FunPay Listener:\n\n{e}")
        time.sleep(15)