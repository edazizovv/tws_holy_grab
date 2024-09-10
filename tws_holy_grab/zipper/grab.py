#
import os
import glob
import json


#
import numpy
import pandas


#
def standard_schedule(authors, start_date, end_date):

    for author in authors:
        ts_console_command(author=author, start_date=start_date, end_date=end_date)


def ts_console_command(author, start_date, end_date):

    os.chdir('./TweetScraper')
    os.system('scrapy crawl TweetScraper -a query="from:{0} since:{1} until:{2}"'.format(
        author, start_date, end_date
    ))
    os.chdir('..')


def parse_ts():

    files = glob.glob('TweetScraper/Data/tweet/*')

    dictlist = []

    for file in files:
        json_string = open(file, 'r', encoding="utf8").read()
        json_dict = json.loads(json_string)
        dictlist.append(json_dict)
        # os.remove(file)

    df = pandas.DataFrame(dictlist)

    # df = df.replace({'\n': ' '}, regex=True)  # remove linebreaks in the dataframe
    # df = df.replace({'\t': ' '}, regex=True)  # remove tabs in the dataframe
    # df = df.replace({'\r': ' '}, regex=True)  # remove carriage return in the dataframe

    # df['raw_data'] = df['raw_data'].astype(dtype=str)
    # df['raw_data'] = df['raw_data'].str.replace('\n', ' ', regex=True)
    # df['raw_data'] = df['raw_data'].str.replace('\t', ' ', regex=True)
    # df['raw_data'] = df['raw_data'].str.replace('\r', ' ', regex=True)

    df['id'] = df['raw_data'].apply(lambda x: x['id_str']).astype(dtype=str)
    df['author'] = df['raw_data'].apply(lambda x: x['user_id_str']).astype(dtype=str)
    df['datetime'] = df['raw_data'].apply(lambda x: x['created_at'])
    df['text'] = df['raw_data'].apply(lambda x: x['full_text']).astype(dtype=str)
    df['retweet_count'] = df['raw_data'].apply(lambda x: x['retweet_count']).astype(dtype=int)
    df['favorite_count'] = df['raw_data'].apply(lambda x: x['favorite_count']).astype(dtype=int)
    df['reply_count'] = df['raw_data'].apply(lambda x: x['reply_count']).astype(dtype=int)
    df['quote_count'] = df['raw_data'].apply(lambda x: x['quote_count']).astype(dtype=int)
    df['lang'] = df['raw_data'].apply(lambda x: x['lang']).astype(dtype=str)
    df['reply_to_id'] = df['raw_data'].apply(lambda x: x['in_reply_to_status_id_str'])
    # df['status'] = 'parsed'
    # df['post_link'] = ''

    df['media'] = df['raw_data'].apply(lambda x: (
        x['entities']['media'][0]['media_url_https'] if 'media_url_https' in x['entities']['media'][
            0].keys() else None) if 'media' in x['entities'].keys() else None)

    df['datetime'] = pandas.to_datetime(df['datetime'])
    # df['status'] = df['status'].astype(dtype=str)

    df = df[['id', 'author', 'datetime', 'text',
             'retweet_count', 'favorite_count', 'reply_count', 'quote_count', 'lang', 'reply_to_id', 'media']]

    with open('./authors_namelist.json', 'r') as file:
        names = json.load(file)

    df = df[numpy.isin(df['author'], list(names.keys()))]

    return df


def grab_ts_local(authors, start_date, end_date, conn):

    standard_schedule(authors=authors, start_date=start_date, end_date=end_date)
    df = parse_ts()
    load_to_local_db(df=df, conn=conn)


def load_to_local_db(df, conn):

    query = '''
    SELECT id
    FROM preprint
    WHERE id IN ({0})
    ;
    '''.format(','.join(["'{0}'".format(x) for x in df['id'].values.tolist()]))

    df_existing = pandas.read_sql(sql=query, con=conn)
    df_remain = df.query("id not in [{0}]".format(', '.join(
        ["'{0}'".format(x) for x in df_existing['id'].values.tolist()])))
    df_remain.to_sql(name='preprint', con=conn, if_exists='append', index=False, method='multi')
    # df_remain.to_sql(name='history', con=conn, if_exists='append', index=False, method='multi')
