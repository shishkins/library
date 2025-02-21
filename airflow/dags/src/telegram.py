import os
import time

from airflow.models import Variable
from telebot import TeleBot
from telebot.types import InlineKeyboardMarkup
from telebot.util import smart_split


def check_chat(chat_id: int, token: str = None) -> str:
    """
    Проверка доступности чата/канала Telegram

    :param chat_id: ID чата. Формат: -100XXXXXXXXXX
    :param token: Токен авторизации бота. По умолчанию: ALERT_BOT_TOKEN

    :return: Наименование чата
    """

    if token is None:
        token = os.getenv('ALERT_BOT_TOKEN') or Variable.get('ALERT_BOT_TOKEN')

    bot = TeleBot(token)
    chat_name = bot.get_chat(chat_id).title
    print('Чат:', chat_name)

    return chat_name


def send_message(chat_id: int,
                 message: str,
                 markup: InlineKeyboardMarkup = None,
                 token: str = None):
    """
    Отправка сообщения в Telegram чат/канал.
    Если сообщение больше 4096 символов, то отправляется несколько сообщений.

    :param chat_id: ID чата. Формат: -100XXXXXXXXXX
    :param message: Сообщение. Поддерживает HTML теги
    :param markup: Кнопки в сообщении
    :param token: Токен авторизации бота
    """

    if token is None:
        token = os.getenv('ALERT_BOT_TOKEN') or Variable.get('ALERT_BOT_TOKEN')

    bot = TeleBot(token)

    for msg in smart_split(message):
        bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode='HTML',
            disable_web_page_preview=True,
            reply_markup=markup,
            timeout=60
        )
        time.sleep(3)
