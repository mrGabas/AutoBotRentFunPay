# localization.py
TEXTS = {
    'ru': {
        'RENTAL_SUCCESS': "✅ Вы успешно арендовали *{game_name}* на {total_hours} ч.\n\nДанные для входа:\n`логин: {login}`\n`пароль: {password}`\n\nПриятной игры! Чтобы проверить оставшееся время, напишите `!время`.",
        'NO_ACCOUNTS_AVAILABLE_USER': "😕 К сожалению, сейчас нет свободных аккаунтов для этой игры. Попробуйте позже.",
        'LANG_SET_RU': "✅ Язык установлен на русский.",
        'LANG_SET_EN': "✅ Language set to English.",
        'HELP_MESSAGE': "🤖 Доступные команды:\n`!игры` - Показать список игр и наличие свободных аккаунтов.\n`!время` - Показать оставшееся время аренды.\n`!продлить <часы>` - Продлить аренду на указанное количество часов (например, `!продлить 2`).\n`!помощь` - Показать это сообщение.",
        'GAMES_HEADER': "🎮 *Наши игры (Всего / Свободно):*",
        'NO_GAMES_AVAILABLE': "На данный момент нет доступных игр.",
        'NO_ACTIVE_RENTALS': "🤔 У вас нет активных аренд.",
        'RENTAL_EXPIRED': "⌛️ Ваша аренда истекла.",
        'RENTAL_INFO': "⏳ Оставшееся время аренды: *{remaining_time}*.\nОкончание (МСК): *{end_time_msk}*.\nОкончание (UTC): *{end_time_utc}*.",
        'INVALID_EXTEND_FORMAT': "⚠️ Неверный формат. Используйте: `!продлить <количество часов>`. Например: `!продлить 2`.",
        'NO_RENTAL_TO_EXTEND': "🤔 Не найдено активной аренды для продления.",
        'EXTEND_SUCCESS': "✅ Аренда успешно продлена на *{hours} ч.*\nНовое время окончания (МСК): *{end_time_msk}*.\nНовое время окончания (UTC): *{end_time_utc}*.",
        'RENTAL_ENDING_SOON': "⏳ Внимание! Ваша аренда закончится менее чем через 10 минут.",
    },
    'en': {
        'RENTAL_SUCCESS': "✅ You have successfully rented *{game_name}* for {total_hours} h.\n\nLogin details:\n`login: {login}`\n`password: {password}`\n\nEnjoy the game! To check remaining time, type `!time`.",
        'NO_ACCOUNTS_AVAILABLE_USER': "😕 Unfortunately, there are no free accounts for this game right now. Please try again later.",
        'LANG_SET_RU': "✅ Язык установлен на русский.",
        'LANG_SET_EN': "✅ Language set to English.",
        'HELP_MESSAGE': "🤖 Available commands:\n`!games` - Show list of games and available accounts.\n`!time` - Show remaining rental time.\n`!extend <hours>` - Extend rental by the specified number of hours (e.g., `!extend 2`).\n`!help` - Show this message.",
        'GAMES_HEADER': "🎮 *Our Games (Total / Free):*",
        'NO_GAMES_AVAILABLE': "No games available at the moment.",
        'NO_ACTIVE_RENTALS': "🤔 You have no active rentals.",
        'RENTAL_EXPIRED': "⌛️ Your rental has expired.",
        'RENTAL_INFO': "⏳ Remaining rental time: *{remaining_time}*.\nEnds (MSK): *{end_time_msk}*.\nEnds (UTC): *{end_time_utc}*.",
        'INVALID_EXTEND_FORMAT': "⚠️ Invalid format. Use: `!extend <hours>`. For example: `!extend 2`.",
        'NO_RENTAL_TO_EXTEND': "🤔 No active rental found to extend.",
        'EXTEND_SUCCESS': "✅ Rental successfully extended by *{hours} h.*\nNew end time (MSK): *{end_time_msk}*.\nNew end time (UTC): *{end_time_utc}*.",
        'RENTAL_ENDING_SOON': "⏳ Attention! Your rental will expire in less than 10 minutes.",
    }
}

def get_text(key, lang='ru'):
    """Возвращает текст по ключу для указанного языка."""
    return TEXTS.get(lang, TEXTS['ru']).get(key, f"<{key}>")