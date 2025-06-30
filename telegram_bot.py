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

# --- Глобальные переменные и функции отправки (без изменений) ---
BOT_INSTANCE: Bot = None
UPDATER_INSTANCE: Updater = None
TG_SEND_QUEUE: Queue = None

def send_telegram_notification(message: str):
    if TG_SEND_QUEUE:
        TG_SEND_QUEUE.put({'type': 'info', 'text': message})

def send_telegram_alert(message: str):
    if TG_SEND_QUEUE:
        TG_SEND_QUEUE.put({'type': 'alert', 'text': message})

def _send_message_from_queue(context: CallbackContext):
    if not TG_SEND_QUEUE.empty():
        item = TG_SEND_QUEUE.get_nowait()
        text = item.get('text', 'Пустое сообщение')
        if item.get('type') == 'alert':
            text = f"🚨 <b>ВНИМАНИЕ</b> 🚨\n\n{text}"
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
    global BOT_INSTANCE, UPDATER_INSTANCE, TG_SEND_QUEUE
    if not config.TELEGRAM_BOT_TOKEN:
        logging.warning("[TG_BOT] Токен не указан. Бот не будет запущен.")
        return
    try:
        if not config.TELEGRAM_ADMIN_CHAT_ID:
            logging.error("[TG_BOT] TELEGRAM_ADMIN_CHAT_ID не указан.")
            return

        BOT_INSTANCE = Bot(token=config.TELEGRAM_BOT_TOKEN)
        UPDATER_INSTANCE = Updater(bot=BOT_INSTANCE, use_context=True)
        dp = UPDATER_INSTANCE.dispatcher

        TG_SEND_QUEUE = Queue()
        job_queue: JobQueue = UPDATER_INSTANCE.job_queue
        job_queue.run_repeating(_send_message_from_queue, interval=1, first=0)

        # Регистрация всех команд
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
    except Exception as e:
        logging.error(f"[TG_BOT] Ошибка запуска Telegram-бота: {e}")

def stop_bot():
    if UPDATER_INSTANCE:
        UPDATER_INSTANCE.stop()
        logging.info("[TG_BOT] Telegram-бот остановлен.")

def admin_only(func):
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        if str(update.effective_user.id) != str(config.TELEGRAM_ADMIN_CHAT_ID):
            update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return func(update, context, *args, **kwargs)
    return wrapped

# --- Команды ---

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
        "/disable_lots - 🚫 Принудительно выключить ВСЕ лоты.\n\n"
        "<b>Информация:</b>\n"
        "/status - ℹ️ Узнать текущий статус.\n"
        "/stats - Общая статистика.\n"
        "/rentals - Активные аренды.\n"
        "/games - Статистика по играм."
    )
    update.message.reply_text(help_text, parse_mode=ParseMode.HTML)

# <<< ИЗМЕНЕНИЕ: Логика команды disable_lots >>>
@admin_only
def disable_lots_command(update: Update, context: CallbackContext):
    """Запрещает активацию лотов И запрашивает принудительное отключение."""
    state_manager.are_lots_enabled = False
    state_manager.force_deactivate_all_lots_requested = True # <-- Включаем флаг
    logging.warning("[TG_BOT] Администратор запросил принудительное отключение всех лотов.")
    update.message.reply_text("🚫 Управление лотами выключено. Запущен процесс принудительной деактивации всех активных лотов. Это может занять до минуты.")

# ... (остальные команды без изменений) ...
@admin_only
def enable_bot_command(update: Update, context: CallbackContext):
    state_manager.is_bot_enabled = True
    update.message.reply_text("✅ Бот включен.")

@admin_only
def disable_bot_command(update: Update, context: CallbackContext):
    state_manager.is_bot_enabled = False
    update.message.reply_text("⛔️ Бот выключен.")

@admin_only
def status_command(update: Update, context: CallbackContext):
    bot_status = "✅ Включен (авто)" if state_manager.is_bot_enabled else "⛔️ Выключен (ручной)"
    lot_status = "✅ Включено" if state_manager.are_lots_enabled else "🚫 Отключено"
    update.message.reply_text(f"<b>Текущий статус:</b>\n\nСостояние бота: {bot_status}\nУправление лотами: {lot_status}", parse_mode=ParseMode.HTML)

@admin_only
def enable_lots_command(update: Update, context: CallbackContext):
    state_manager.are_lots_enabled = True
    update.message.reply_text("✅ Управление лотами включено.")

@admin_only
def stats_command(update: Update, context: CallbackContext):
    try:
        total = db_handler.db_query("SELECT COUNT(*) FROM accounts", fetch="one")[0]
        rented = db_handler.db_query("SELECT COUNT(*) FROM accounts WHERE rented_by IS NOT NULL", fetch="one")[0]
        update.message.reply_text(f"📊 <b>Общая статистика</b>\n\nВсего: <b>{total}</b>\nСвободно: <b>{total - rented}</b>\nЗанято: <b>{rented}</b>", parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {e}")

@admin_only
def rentals_command(update: Update, context: CallbackContext):
    try:
        rentals = db_handler.db_query("SELECT r.client_name, g.name, r.end_time, a.login FROM rentals r JOIN accounts a ON r.account_id = a.id JOIN games g ON a.game_id = g.id WHERE r.is_history = 0 ORDER BY r.end_time ASC", fetch="all")
        if not rentals: return update.message.reply_text("✅ Активных аренд нет.")
        message = "📋 <b>Список активных аренд:</b>\n\n"
        for client, game, end_time_iso, login in rentals:
            remaining = datetime.fromisoformat(end_time_iso) - datetime.now()
            message += f"👤 <i>{client}</i> ({game})\n   Аккаунт: <code>{login}</code>\n   Осталось: <b>{format_timedelta(remaining)}</b>\n\n"
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {e}")

@admin_only
def games_command(update: Update, context: CallbackContext):
    try:
        stats = db_handler.get_games_stats()
        if not stats: return update.message.reply_text("В базе данных нет игр.")
        message = "🎮 <b>Статистика по играм (Всего / Свободно):</b>\n\n"
        for name, total, free in stats:
            message += f"• <i>{name}</i>:  <code>{total} / {free}</code>\n"
        update.message.reply_text(message, parse_mode=ParseMode.HTML)
    except Exception as e:
        update.message.reply_text(f"❌ Ошибка: {e}")