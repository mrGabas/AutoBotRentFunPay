# telegram_bot.py
import logging
from telegram import Update, Bot, ParseMode
from telegram.ext import Updater, CommandHandler, CallbackContext, JobQueue
from telegram.error import TelegramError
from queue import Queue

import db_handler
import config
import state_manager
from utils import format_timedelta
from datetime import datetime

# --- Глобальные переменные ---
BOT_INSTANCE: Bot = None
UPDATER_INSTANCE: Updater = None
TG_SEND_QUEUE: Queue = None


# --- Функции для отправки сообщений из других модулей ---

def send_telegram_notification(message: str):
    """Отправляет обычное уведомление администратору."""
    if TG_SEND_QUEUE:
        TG_SEND_QUEUE.put({'type': 'info', 'text': message})


def send_telegram_alert(message: str):
    """Отправляет важное уведомление (alert) администратору."""
    if TG_SEND_QUEUE:
        TG_SEND_QUEUE.put({'type': 'alert', 'text': message})


# --- Логика работы самого бота ---

def _send_message_from_queue(context: CallbackContext):
    """Обрабатывает очередь сообщений и отправляет их."""
    if not TG_SEND_QUEUE.empty():
        item = TG_SEND_QUEUE.get()
        text = item.get('text', 'Пустое сообщение')
        if item.get('type') == 'alert':
            text = f"🚨 **ВНИМАНИЕ** 🚨\n\n{text}"

        try:
            context.bot.send_message(
                chat_id=config.TELEGRAM_ADMIN_CHAT_ID,
                text=text,
                parse_mode=ParseMode.HTML
            )
        except TelegramError as e:
            logging.error(f"[TG_BOT_SENDER] Не удалось отправить сообщение: {e}")
        finally:
            TG_SEND_QUEUE.task_done()


def start_bot():
    """Инициализирует и запускает Telegram-бота и очередь отправки."""
    global BOT_INSTANCE, UPDATER_INSTANCE, TG_SEND_QUEUE
    if not config.TELEGRAM_BOT_TOKEN:
        logging.warning("[TG_BOT] Токен Telegram-бота не указан. Бот не будет запущен.")
        return

    try:
        if not config.TELEGRAM_ADMIN_CHAT_ID:
            logging.error("[TG_BOT] TELEGRAM_ADMIN_CHAT_ID не указан. Бот не может работать.")
            return

        BOT_INSTANCE = Bot(token=config.TELEGRAM_BOT_TOKEN)
        UPDATER_INSTANCE = Updater(bot=BOT_INSTANCE, use_context=True)
        dp = UPDATER_INSTANCE.dispatcher

        # Создаем очередь для отправки сообщений и запускаем ее обработчик
        TG_SEND_QUEUE = Queue()
        job_queue: JobQueue = UPDATER_INSTANCE.job_queue
        job_queue.run_repeating(_send_message_from_queue, interval=1, first=0)

        # Регистрация обработчиков команд
        dp.add_handler(CommandHandler("start", start_command))
        dp.add_handler(CommandHandler("enable", enable_bot_command))
        dp.add_handler(CommandHandler("disable", disable_bot_command))
        dp.add_handler(CommandHandler("status", status_command))
        dp.add_handler(CommandHandler("enable_lots", enable_lots_command))
        dp.add_handler(CommandHandler("disable_lots", disable_lots_command))
        dp.add_handler(CommandHandler("stats", stats_command))
        dp.add_handler(CommandHandler("rentals", rentals_command))
        dp.add_handler(CommandHandler("games", games_command))

        UPDATER_INSTANCE.start_polling()
        logging.info("[TG_BOT] Telegram-бот и обработчик очереди успешно запущены.")

    except (TelegramError, ValueError) as e:
        logging.error(f"[TG_BOT] Ошибка запуска Telegram-бота: {e}")


def stop_bot():
    """Останавливает Telegram-бота."""
    if UPDATER_INSTANCE:
        UPDATER_INSTANCE.stop()
        logging.info("[TG_BOT] Telegram-бот остановлен.")


# ... (декоратор admin_only и все команды остаются такими же, как в прошлой версии) ...
def admin_only(func):
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        if str(update.effective_user.id) != str(config.TELEGRAM_ADMIN_CHAT_ID):
            update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return func(update, context, *args, **kwargs)

    return wrapped


@admin_only
def start_command(update: Update, context: CallbackContext):
    user_name = update.effective_user.first_name
    help_text = (
        f"👋 Привет, {user_name}!\n\n"
        "<b>Управление ботом:</b>\n"
        "/enable - ✅ Включить бота (авторежим).\n"
        "/disable - ⛔️ Выключить бота (ручной режим).\n\n"
        "<b>Управление лотами:</b>\n"
        "/enable_lots - ✅ Разрешить боту включать лоты.\n"
        "/disable_lots - 🚫 Запретить боту включать лоты.\n\n"
        "<b>Информация:</b>\n"
        "/status - ℹ️ Узнать текущий статус.\n"
        "/stats - Общая статистика.\n"
        "/rentals - Активные аренды.\n"
        "/games - Статистика по играм."
    )
    update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


@admin_only
def enable_bot_command(update: Update, context: CallbackContext):
    state_manager.is_bot_enabled = True
    logging.info("[TG_BOT] Бот ВКЛЮЧЕН администратором.")
    update.message.reply_text("✅ Бот включен. Автоматическая обработка заказов возобновлена.")


@admin_only
def disable_bot_command(update: Update, context: CallbackContext):
    state_manager.is_bot_enabled = False
    logging.warning("[TG_BOT] Бот ВЫКЛЮЧЕН администратором.")
    update.message.reply_text("⛔️ Бот выключен. Новые события FunPay будут игнорироваться.")


@admin_only
def status_command(update: Update, context: CallbackContext):
    bot_status = "✅ Включен (авто)" if state_manager.is_bot_enabled else "⛔️ Выключен (ручной)"
    lot_status = "✅ Включено" if state_manager.are_lots_enabled else "🚫 Отключено"
    message = (
        f"<b>Текущий статус:</b>\n\n"
        f"Состояние бота: {bot_status}\n"
        f"Управление лотами: {lot_status}"
    )
    update.message.reply_text(message, parse_mode=ParseMode.HTML)


@admin_only
def enable_lots_command(update: Update, context: CallbackContext):
    state_manager.are_lots_enabled = True
    logging.info("[TG_BOT] Управление лотами ВКЛЮЧЕНО.")
    update.message.reply_text(
        "✅ Управление лотами включено. Бот будет автоматически активировать лоты при наличии свободных аккаунтов.")


@admin_only
def disable_lots_command(update: Update, context: CallbackContext):
    state_manager.are_lots_enabled = False
    logging.warning("[TG_BOT] Управление лотами ВЫКЛЮЧЕНО.")
    update.message.reply_text(
        "🚫 Управление лотами выключено. Бот больше не будет поднимать лоты. Для принудительного отключения всех активных лотов перезапустите серверного бота.")


@admin_only
def stats_command(update: Update, context: CallbackContext):
    try:
        total = db_handler.db_query("SELECT COUNT(*) FROM accounts", fetch="one")[0]
        rented = db_handler.db_query("SELECT COUNT(*) FROM accounts WHERE rented_by IS NOT NULL", fetch="one")[0]
        stats_text = (
            f"📊 <b>Общая статистика</b>\n\n"
            f"Всего аккаунтов: <b>{total}</b>\n"
            f"✅ Свободно: <b>{total - rented}</b>\n"
            f"❌ Занято: <b>{rented}</b>"
        )
        update.message.reply_text(stats_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка получения статистики: {e}")


@admin_only
def rentals_command(update: Update, context: CallbackContext):
    try:
        rentals = db_handler.db_query("""
                                      SELECT r.client_name, g.name, r.end_time, a.login
                                      FROM rentals r
                                               JOIN accounts a ON r.account_id = a.id
                                               JOIN games g ON a.game_id = g.id
                                      WHERE r.is_history = 0
                                      ORDER BY r.end_time ASC
                                      """, fetch="all")
        if not rentals:
            update.message.reply_text("✅ Активных аренд нет.")
            return

        message = "📋 <b>Список активных аренд:</b>\n\n"
        for client, game, end_time_iso, login in rentals:
            remaining = datetime.fromisoformat(end_time_iso) - datetime.now()
            message += (
                f"👤 <i>{client}</i> ({game})\n"
                f"   Аккаунт: <code>{login}</code>\n"
                f"   Осталось: <b>{format_timedelta(remaining)}</b>\n\n"
            )
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка получения аренд: {e}")


@admin_only
def games_command(update: Update, context: CallbackContext):
    try:
        stats = db_handler.get_games_stats()
        if not stats:
            update.message.reply_text("В базе данных нет игр.")
            return

        message = "🎮 <b>Статистика по играм (Всего / Свободно):</b>\n\n"
        for name, total, free in stats:
            message += f"• <i>{name}</i>:  <code>{total} / {free}</code>\n"
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка получения игр: {e}")