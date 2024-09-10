#
import os
import glob
import datetime

#
import pandas


#


#
def clean(conn):

    query = '''
    SELECT MAX(datetime) as MX
    FROM preprint
    ;
    '''

    mx_datetime = pandas.read_sql(sql=query, con=conn).values[0, 0]
    cut_date = mx_datetime + datetime.timedelta(days=-1)

    query = """
    DELETE
    FROM preprint
    WHERE datetime <= '{0}'
    ;
    """.format(cut_date)

    conn.execute(query)

    query = """
    UPDATE history 
    SET status = 'discarded'
    WHERE datetime <= '{0}'
    AND status = 'parsed'
    ;
    """.format(cut_date)

    conn.execute(query)

    for file in glob.glob('./TweetScrapper/Data/tweet/*'):
        os.remove(file)
    for file in glob.glob('./TweetScrapper/Data/user/*'):
        os.remove(file)


def simple_selector(conn, channel, format_lang):

    query = """
    SELECT p.*
    FROM 
    preprint as p
    LEFT JOIN
    (
    SELECT *
    FROM history
    WHERE channel = '{0}'
    ) as h
    ON 
    p.id = h.id
    WHERE (h.status != 'posted') OR (h.status IS NULL)
    ;
    """.format(channel)

    df_t0 = pandas.read_sql(sql=query, con=conn)

    if df_t0.shape[0] > 0:

        df_t1 = df_t0[df_t0['favorite_count'] == df_t0['favorite_count'].max()]
        df_t2 = df_t1[df_t1['retweet_count'] == df_t1['retweet_count'].max()]
        df_t3 = df_t2[df_t2['reply_count'] == df_t2['reply_count'].max()]
        df_t4 = df_t3[df_t3['quote_count'] == df_t3['quote_count'].max()]

        if df_t1.shape[0] == 1:
            id, author, date, text, lang, reply_to_id, media = df_t1.iloc[0, :][['id', 'author', 'datetime', 'text', 'lang', 'reply_to_id', 'media']]
        elif df_t2.shape[0] == 1:
            id, author, date, text, lang, reply_to_id, media = df_t2.iloc[0, :][['id', 'author', 'datetime', 'text', 'lang', 'reply_to_id', 'media']]
        elif df_t3.shape[0] == 1:
            id, author, date, text, lang, reply_to_id, media = df_t3.iloc[0, :][['id', 'author', 'datetime', 'text', 'lang', 'reply_to_id', 'media']]
        elif df_t4.shape[0] == 1:
            id, author, date, text, lang, reply_to_id, media = df_t4.iloc[0, :][['id', 'author', 'datetime', 'text', 'lang', 'reply_to_id', 'media']]
        else:
            id, author, date, text, lang, reply_to_id, media = df_t4.sample(n=1, axis=0).iloc[0, :][['id', 'author', 'datetime', 'text', 'lang', 'reply_to_id', 'media']]

        # reply_to_id = '1533274964790546432'

        if (reply_to_id == '') or (reply_to_id is None):
            reply_to_link = None
        else:

            query = """
            SELECT author, id, post_link
            FROM history
            WHERE id = '{0}'
            AND channel = '{1}'
            ;
            """.format(reply_to_id, channel)

            link_result = pandas.read_sql(sql=query, con=conn)

            if format_lang == 'ru':
                format_word = 'твит'
            elif format_lang == 'en':
                format_word = 'tweet'
            else:
                raise KeyError("Problems?")

            if link_result.shape[0] == 0:
                # reply_to_link = '[твит](https://twitter.com/{0}/status/{1})'.format(author, reply_to_id)
                reply_to_link = '<a href="{0}">{1}</a>'.format('https://twitter.com/{0}/status/{1}'.format(
                    author, reply_to_id
                ), format_word)
            else:
                post_link = link_result.iloc[0, :]['post_link']
                # reply_to_link = '[твит]({0})'.format(post_link)
                reply_to_link = '<a href="{0}">{1}</a>'.format(post_link, format_word)

        return author, date, text, lang, id, reply_to_id, reply_to_link, media

    else:

        return None, None, None, None, None, None, None, None
