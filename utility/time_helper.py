# Time helper

from datetime import datetime
from dateutil import tz
import time

def format_timestamp_as_local(timestamp):
    if not timestamp:
        return 'Not a Timestamp: %s' % str(timestamp)

    dt_local = datetime.fromtimestamp(timestamp, tz.tzlocal())
    return dt_local.strftime("%Y/%m/%d %H:%M:%S")

def format_timestamp_as_hour_time(timestamp):
    dt_local = datetime.fromtimestamp(timestamp, tz.tzlocal())
    return dt_local.strftime("%d %H:%M")

def get_datetime_for_timestamp(timestamp) -> datetime:
    return datetime.fromtimestamp(timestamp, tz.tzlocal())

def timestamp_now():
    return round(time.time())

def get_timestamp_month_integer(timestamp):
    dt_local = datetime.fromtimestamp(timestamp, tz.tzlocal())
    return dt_local.strftime("%m")

def get_timestamp_year_integer(timestamp):
    dt_local = datetime.fromtimestamp(timestamp, tz.tzlocal())
    return dt_local.strftime("%Y")