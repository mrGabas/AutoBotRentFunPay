# utils.py
from datetime import datetime
import time


def background_checker(rentals_list, queue_obj):
    """Фоновый процесс для проверки аренд."""
    while True:
        now = datetime.now()
        # Мы работаем с оригинальным списком, чтобы изменения были видны
        for r in rentals_list:
            # Проверка на полное истечение срока
            if "end" in r and now >= r["end"]:
                # Если аренда истекла, но еще не в истории, отправляем на обработку
                # Эта проверка нужна, чтобы не спамить истекшими задачами
                if not r.get("expired_sent"):
                    queue_obj.put(("expired", r))
                    r["expired_sent"] = True  # Ставим флаг, что уже отправили
                continue

            # --- ГЛАВНОЕ ИЗМЕНЕНИЕ ЗДЕСЬ ---
            # Проверка на время напоминания
            if "remind" in r and now >= r["remind"] and not r.get("reminded", False):
                queue_obj.put(("reminder", r))
                # СРАЗУ ЖЕ ставим отметку в объекте, с которым работаем.
                # Теперь на следующей проверке `not r.get("reminded", False)` вернет False,
                # и повторное уведомление не будет отправлено.
                r["reminded"] = True

        time.sleep(20)  # Пауза между проверками


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