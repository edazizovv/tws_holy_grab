#
import json
import datetime


#
import pandas
from apscheduler.schedulers.background import BlockingScheduler
from sqlalchemy import create_engine


#
from zipper.grab import grab_ts_local
from zipper.select import clean, simple_selector
from zipper.post import tg_post


#
sched = BlockingScheduler()


@sched.scheduled_job('interval', id='GRAB-HOLY', minutes=10)
def total():

    with open('./db_auth.json', 'r') as file:
        db_auth = json.load(file)
        user, password, host, dbname = db_auth['user'], db_auth['password'], db_auth['host'], db_auth['dbname']

    conn = create_engine("postgresql+psycopg2://{0}:{1}@{2}/{3}".format(
        user, password, host, dbname
    )).connect()

    authors = pandas.read_csv('./authors.csv').values[:, 0]
    start_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')

    grab_ts_local(authors=authors, start_date=start_date, end_date=end_date, conn=conn)
    clean(conn=conn)


sched.start()

# load()
# post()
# total()
