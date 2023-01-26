# Time helper

from datetime import datetime
import time
from dateutil.relativedelta import relativedelta

def format_timestamp(timestamp, format ="%Y/%m/%d %H:%M:%S"):
    if not timestamp:
        return 'Not a Timestamp: %s' % str(timestamp)

    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime(format)

def format_timestamp_as_hour_time(timestamp):
    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime("%d %H:%M")

def get_datetime_for_timestamp(timestamp) -> datetime:
    return datetime.fromtimestamp(timestamp)

def get_timestamp_for_datekey(datekey):
    return int(datetime.strptime(datekey, '%Y-%m').timestamp())

def get_datekey_for_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m")

def timestamp_now():
    return round(time.time())

def get_timestamp_month_integer(timestamp):
    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime("%m")

def get_timestamp_year_integer(timestamp):
    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime("%Y")

def add_month(date_key, month_count = 1) -> str:
    return get_datekey_for_timestamp((get_datetime_for_timestamp(get_timestamp_for_datekey(date_key)) + relativedelta(months=month_count)).timestamp())
