# state_manager.py
# Этот файл хранит глобальное состояние бота.

# True = бот активен, False = "ручной режим"
is_bot_enabled = True

# True = бот может активировать лоты. False = не может.
are_lots_enabled = True

# Флаг для ОДНОРАЗОВОГО запроса на принудительную деактивацию всех лотов.
force_deactivate_all_lots_requested = False