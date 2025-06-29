# telegram_bot.py
import logging
from telegram import Update, Bot
from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram.error import TelegramError

import db_handler
import config
import state_manager  # <-- Импортируем наш переключатель
from utils import format_timedelta
from datetime import datetime

# --- Глобальные переменные ---
BOT_INSTANCE = None
UPDATER_INSTANCE = None


# --- Основные функции ---

def start_bot():
    global BOT_INSTANCE, UPDATER_INSTANCE
    if not config.TELEGRAM_BOT_TOKEN:
        logging.warning("[TG_BOT] Токен Telegram-бота не указан. Бот не будет запущен.")
        return

    try:
        BOT_INSTANCE = Bot(token=config.TELEGRAM_BOT_TOKEN)
        if not config.TELEGRAM_ADMIN_CHAT_ID:
            logging.error("[TG_BOT] TELEGRAM_ADMIN_CHAT_ID не указан. Бот не может работать.")
            return

        UPDATER_INSTANCE = Updater(bot=BOT_INSTANCE, use_context=True)
        dp = UPDATER_INSTANCE.dispatcher

        # <<< ИЗМЕНЕНИЕ: Добавляем новые команды >>>
        dp.add_handler(CommandHandler("start", start_command))
        dp.add_handler(CommandHandler("stats", stats_command))
        dp.add_handler(CommandHandler("rentals", rentals_command))
        dp.add_handler(CommandHandler("games", games_command))
        dp.add_handler(CommandHandler("enable", enable_bot_command))
        dp.add_handler(CommandHandler("disable", disable_bot_command))
        dp.add_handler(CommandHandler("status", status_command))
        # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

        UPDATER_INSTANCE.start_polling()
        logging.info("[TG_BOT] Telegram-бот успешно запущен.")

    except (TelegramError, ValueError) as e:
        logging.error(f"[TG_BOT] Ошибка запуска Telegram-бота: {e}")


# ... (функция stop_bot и декоратор admin_only остаются без изменений) ...
def stop_bot():
    if UPDATER_INSTANCE:
        UPDATER_INSTANCE.stop()
        logging.info("[TG_BOT] Telegram-бот остановлен.")


def admin_only(func):
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        if str(user_id) != str(config.TELEGRAM_ADMIN_CHAT_ID):
            logging.warning(f"[TG_BOT] Доступ запрещен для пользователя {user_id}.")
            update.message.reply_text("⛔️ У вас нет прав для выполнения этой команды.")
            return
        return func(update, context, *args, **kwargs)

    return wrapped


# --- Обработчики команд ---

@admin_only
def start_command(update: Update, context: CallbackContext):
    user_name = update.effective_user.first_name
    # <<< ИЗМЕНЕНИЕ: Обновляем текст приветствия >>>
    help_text = (
        f"👋 Привет, {user_name}!\n\n"
        "Бот для управления арендами. Доступные команды:\n\n"
        "**Управление ботом:**\n"
        "/enable - ✅ Включить автоматический режим.\n"
        "/disable - ⛔️ Выключить (ручной режим).\n"
        "/status - ℹ️ Узнать текущий статус бота.\n\n"
        "**Статистика:**\n"
        "/stats - Общая статистика.\n"
        "/rentals - Активные аренды.\n"
        "/games - Статистика по играм."
    )
    update.message.reply_text(help_text, parse_mode='Markdown')
    # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>


# <<< ИЗМЕНЕНИЕ: Новые команды для управления состоянием >>>
@admin_only
def enable_bot_command(update: Update, context: CallbackContext):
    """Включает бота."""
    state_manager.is_bot_enabled = True
    logging.info("[TG_BOT] Бот ВКЛЮЧЕН администратором.")
    update.message.reply_text("✅ Бот включен. Автоматическая обработка заказов возобновлена.")


@admin_only
def disable_bot_command(update: Update, context: CallbackContext):
    """Выключает бота."""
    state_manager.is_bot_enabled = False
    logging.warning("[TG_BOT] Бот ВЫКЛЮЧЕН администратором. Переход в ручной режим.")
    update.message.reply_text("⛔️ Бот выключен. Новые заказы и сообщения FunPay будут игнорироваться.")


@admin_only
def status_command(update: Update, context: CallbackContext):
    """Показывает текущий статус бота."""
    if state_manager.is_bot_enabled:
        update.message.reply_text("✅ Бот сейчас включен (автоматический режим).")
    else:
        update.message.reply_text("⛔️ Бот сейчас выключен (ручной режим).")


# <<< КОНЕЦ ИЗМЕНЕНИЯ >>>


# ... (остальные команды stats_command, rentals_command, games_command остаются без изменений) ...
@admin_only
def stats_command(update: Update, context: CallbackContext):
    """Показывает общую статистику по аккаунтам."""
    try:
        total_accounts = db_handler.db_query("SELECT COUNT(*) FROM accounts", fetch="one")[0]
        rented_accounts = db_handler.db_query("SELECT COUNT(*) FROM accounts WHERE rented_by IS NOT NULL", fetch="one")[
            0]
        free_accounts = total_accounts - rented_accounts

        total_rentals = db_handler.db_query("SELECT COUNT(*) FROM rentals WHERE is_history = 0", fetch="one")[0]

        stats_text = (
            "📊 **Общая статистика**\n\n"
            f"Всего аккаунтов: *{total_accounts}*\n"
            f"✅ Свободно: *{free_accounts}*\n"
            f"❌ Занято: *{rented_accounts}*\n\n"
            f"Активных аренд: *{total_rentals}*"
        )
        update.message.reply_text(stats_text, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"[TG_BOT] Ошибка при получении статистики: {e}")
        update.message.reply_text("❌ Не удалось получить статистику.")


@admin_only
def rentals_command(update: Update, context: CallbackContext):
    """Показывает список активных аренд."""
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

        message = "📋 **Список активных аренд:**\n\n"
        now = datetime.now()
        for client, game, end_time_iso, login in rentals:
            end_time = datetime.fromisoformat(end_time_iso)
            remaining = end_time - now
            message += (
                f"👤 *{client}* ({game})\n"
                f"   Аккаунт: `{login}`\n"
                f"   Осталось: *{format_timedelta(remaining)}*\n\n"
            )

        update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"[TG_BOT] Ошибка при получении списка аренд: {e}")
        update.message.reply_text("❌ Не удалось получить список аренд.")


@admin_only
def games_command(update: Update, context: CallbackContext):
    """Показывает статистику по играм."""
    try:
        stats = db_handler.get_games_stats()
        if not stats:
            update.message.reply_text("В базе данных нет игр.")
            return

        message = "🎮 **Статистика по играм (Всего / Свободно):**\n\n"
        for name, total, free in stats:
            message += f"• *{name}*:  `{total} / {free}`\n"

        update.message.reply_text(message, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"[TG_BOT] Ошибка при получении статистики по играм: {e}")
        update.message.reply_text("❌ Не удалось получить статистику по играм.")