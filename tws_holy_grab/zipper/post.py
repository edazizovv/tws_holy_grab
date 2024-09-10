#
import json
import html
import time
import requests


#
import pandas
import telebot
from googletrans import Translator


#


#
def format_tweet_link_ru(reply_to_id, author, tweet_id, reply_to_link):
    if (reply_to_id == '') or (reply_to_id is None):
        # formatted_link = '[твитнул(а)](https://twitter.com/{0}/status/{1})'.format(author, tweet_id)
        # formatted_link = '<a href="{0}">твитнул(а)</a>'.format('https://twitter.com/{0}/status/{1}'.format(
        #     author, tweet_id
        # ))
        formatted_link = ''
    else:
        # formatted_link = '[ответил(а)](https://twitter.com/{0}/status/{1}) на {2}'.format(author, tweet_id, reply_to_link)
        # formatted_link = '<a href="{0}">ответил(а)</a> на {1}'.format('https://twitter.com/{0}/status/{1}'.format(
        #     author, tweet_id
        # ), reply_to_link)
        formatted_link = 'отвечает на {0}'.format(reply_to_link)
    return formatted_link


def format_tweet_link_en(reply_to_id, author, tweet_id, reply_to_link):
    if (reply_to_id == '') or (reply_to_id is None):
        # formatted_link = '[твитнул(а)](https://twitter.com/{0}/status/{1})'.format(author, tweet_id)
        # formatted_link = '<a href="{0}">твитнул(а)</a>'.format('https://twitter.com/{0}/status/{1}'.format(
        #     author, tweet_id
        # ))
        formatted_link = ''
    else:
        # formatted_link = '[ответил(а)](https://twitter.com/{0}/status/{1}) на {2}'.format(author, tweet_id, reply_to_link)
        # formatted_link = '<a href="{0}">ответил(а)</a> на {1}'.format('https://twitter.com/{0}/status/{1}'.format(
        #     author, tweet_id
        # ), reply_to_link)
        formatted_link = 'responds to {0}'.format(reply_to_link)
    return formatted_link


def sanitize_for_markdown(text):
    text = text.replace(
        '\\', '\\\\').replace(
        '`', '\\`').replace(
        '*', '\\*').replace(
        '_', '\\_').replace(
        '{', '\\{').replace(
        '}', '\\}').replace(
        '[', '\\[').replace(
        ']', '\\]').replace(
        '(', '\\(').replace(
        ')', '\\)').replace(
        '#', '\\#').replace(
        '+', '\\+').replace(
        '-', '\\-').replace(
        '.', '\\.').replace(
        '!', '\\!')
    return text


def format_author(author):

    with open('./authors_namelist.json', 'r') as file:
        names = json.load(file)

    # formatted = '[{0}]({1})'.format(names[author], 'https://twitter.com/{0}'.format(names[author]))
    # formatted = '<a href="{1}">{0}</a>'.format(names[author], 'https://twitter.com/{0}'.format(names[author]))
    formatted = '@{0}'.format(names[author])
    return formatted


def translate_text(text, in_lang, out_lang):

    translator = Translator()

    if in_lang == 'und':
        return translator.translate(text, dest=out_lang).text
    else:
        try:
            return translator.translate(text, src=in_lang, dest=out_lang).text
        except ValueError:
            return translator.translate(text, src='en', dest=out_lang).text


def format_media(media):
    if media:
        formatted = '<img src="{0}" alt="">'.format(media)
    else:
        formatted = ''
    return formatted


def format_message(author, date, text, channel, tweet_link): # , media):
    return """{0}:\n\n{1}""".format(author, text)


def tg_post(conn, tg_token, author, date, text, in_lang, out_lang, channel, tweet_id, reply_to_id, reply_to_link, media, format_lang):

    bot = telebot.TeleBot(tg_token, parse_mode=None)

    author_formatted = format_author(author=author)

    if in_lang == out_lang:
        text_formatted = text
    else:
        text_formatted = translate_text(text=text, in_lang=in_lang, out_lang=out_lang)

    text_formatted = html.escape(text_formatted)

    if format_lang == 'ru':
        tweet_link_formatted = format_tweet_link_ru(reply_to_id, author, tweet_id, reply_to_link)
    elif format_lang == 'en':
        tweet_link_formatted = format_tweet_link_en(reply_to_id, author, tweet_id, reply_to_link)
    else:
        raise KeyError("Problems?")

    # media_formatted = format_media(media)

    message = format_message(author=author_formatted, date=date, text=text_formatted, channel=channel,
                             tweet_link=tweet_link_formatted) # , media=media_formatted)


    # with open('C:/Users/Edward/Desktop/image_name.jpg', 'wb') as handler:
    #     handler.write(img_data)

    if (reply_to_id == '') or (reply_to_id is None):
        if (media == '') or (media is None):
            posted_message = bot.send_message(chat_id=channel, text=message, parse_mode='HTML')
        else:
            # posted_message = bot.send_photo(chat_id=channel, photo=media, caption=message, parse_mode='HTML')
            img_data = requests.get(media).content
            posted_message = bot.send_photo(chat_id=channel, photo=img_data, caption=message, parse_mode='HTML')
    else:
        if 'twitter.com' in reply_to_link:
            if (media == '') or (media is None):
                posted_message = bot.send_message(chat_id=channel, text=message, parse_mode='HTML')
            else:
                img_data = requests.get(media).content
                posted_message = bot.send_photo(chat_id=channel, photo=img_data, caption=message, parse_mode='HTML')
        elif 't.me' in reply_to_link:
            sub = reply_to_link[reply_to_link.index(channel.replace('@', '')) + len(channel.replace('@', '')) + 1:]
            post_id = sub[:sub.index('"')]
            if (media == '') or (media is None):
                posted_message = bot.send_message(chat_id=channel, text=message, parse_mode='HTML',
                                                  reply_to_message_id=post_id)
            else:
                img_data = requests.get(media).content
                posted_message = bot.send_photo(chat_id=channel, photo=img_data, caption=message, parse_mode='HTML',
                                                reply_to_message_id=post_id)
        else:
            raise Exception("Problems?")

    posted_link = 'https://t.me/{0}/{1}'.format(posted_message.chat.username, posted_message.id)

    query = """
    SELECT *
    FROM preprint
    WHERE id = '{0}'
    ;
    """.format(tweet_id, posted_link, channel)

    row_to_add = pandas.read_sql(con=conn, sql=query)
    row_to_add['post_link'] = posted_link
    row_to_add['channel'] = channel
    row_to_add['status'] = 'posted'

    row_to_add.to_sql(name='history', con=conn, if_exists='append', index=False, method='multi')

    time.sleep(5)

