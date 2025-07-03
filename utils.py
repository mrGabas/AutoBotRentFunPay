import time
from datetime import datetime, timedelta
import pytz
import logging

MOSCOW_TZ = pytz.timezone('Europe/Moscow')


def background_checker(rentals, update_queue):
    reminded_rentals = set()
    while True:
        try:
            now = datetime.now(MOSCOW_TZ)
            for rental in rentals:
                rental_id = rental.get("id")
                if not rental_id or rental_id in reminded_rentals: continue

                end_time = rental.get("end")
                if not isinstance(end_time, datetime): continue  # Пропускаем, если время не установлено

                time_left = end_time - now
                if 0 < time_left.total_seconds() < 300:  # 5 минут
                    update_queue.put(("reminder", rental))
                    reminded_rentals.add(rental_id)
        except Exception as e:
            logging.error(f"Ошибка в фоновом процессе: {e}", exc_info=True)
        time.sleep(60)


def format_timedelta(td):
    """Форматирует timedelta в человекочитаемую строку."""
    if td.total_seconds() < 0:
        return "Истекло"
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if days > 0:
        return f"{days} д. {hours} ч."
    elif hours > 0:
        return f"{hours} ч. {minutes} м."
    else:
        return f"{minutes} м."


def format_display_time(aware_dt):
    """
    Форматирует datetime для отображения.
    Если дата не совпадает с сегодняшней в этом часовом поясе, добавляет число и месяц.
    """
    now_in_same_tz = datetime.now(aware_dt.tzinfo)
    if now_in_same_tz.date() == aware_dt.date():
        return aware_dt.strftime("%H:%M")
    else:
        return aware_dt.strftime("%d.%m %H:%M")