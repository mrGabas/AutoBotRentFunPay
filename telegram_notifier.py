# telegram_notifier.py
import telegram
import logging
import asyncio
import sys
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_ADMIN_CHAT_ID


async def send_async(bot, chat_id, message_text):
    """Асинхронная корутина, которая непосредственно отправляет сообщение."""
    # Используем MarkdownV2 для лучшей поддержки форматирования
    await bot.send_message(chat_id=chat_id, text=message_text, parse_mode='Markdown')


def _send_message(chat_id, message_text):
    """Общая синхронная функция-обертка для отправки сообщений."""
    if not TELEGRAM_BOT_TOKEN or "СЮДА" in TELEGRAM_BOT_TOKEN or \
            not chat_id or "СЮДА" in str(chat_id):
        logging.warning("[TELEGRAM] Токен или Chat ID не настроены. Уведомление не отправлено.")
        return False
    try:
        bot = telegram.Bot(token=TELEGRAM_BOT_TOKEN)
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        asyncio.run(send_async(bot, chat_id, message_text))

        logging.info(f"[TELEGRAM] Уведомление успешно отправлено в чат {chat_id}.")
        return True
    except Exception as e:
        logging.error(f"[TELEGRAM] Не удалось отправить уведомление в чат {chat_id}: {e}")
        return False


def send_telegram_notification(message_text):
    """Отправляет обычное уведомление администратору."""
    return _send_message(TELEGRAM_ADMIN_CHAT_ID, message_text)


def send_telegram_alert(message_text):
    """Отправляет важное уведомление (тревогу) администратору."""
    # Оборачиваем текст ошибки в блок кода для безопасной отправки
    alert_message = f"️⚠️ *ТРЕВОГА, НУЖНО ВМЕШАТЕЛЬСТВО* ⚠️\n\nПроизошла ошибка:\n```\n{message_text}\n```"
    return _send_message(TELEGRAM_ADMIN_CHAT_ID, alert_message)
