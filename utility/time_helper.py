# Time helper

from datetime import datetime
import time
from dateutil.relativedelta import relativedelta


def format_timestamp(timestamp, format="%Y/%m/%d %H:%M:%S"):
    if not timestamp:
        return "Not a Timestamp: %s" % str(timestamp)

    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime(format)


def format_timestamp_as_hour_time(timestamp):
    dt_local = datetime.fromtimestamp(timestamp)
    return dt_local.strftime("%d %H:%M")


def get_datetime_for_timestamp(timestamp) -> datetime:
    return datetime.fromtimestamp(timestamp)


def get_timestamp_for_datekey(datekey):
    return int(datetime.strptime(datekey, "%Y-%m").timestamp())


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


def add_month(date_key, month_count=1) -> str:
    return get_datekey_for_timestamp(
        (
            get_datetime_for_timestamp(get_timestamp_for_datekey(date_key))
            + relativedelta(months=month_count)
        ).timestamp()
    )


def get_date_keys_for_timestamp_range(ts_start, ts_end):
    date_key_start = get_datekey_for_timestamp(ts_start)
    date_key_end = get_datekey_for_timestamp(ts_end)

    return_range = []
    iterator = date_key_start

    # fill in the inbetween bits
    while iterator != date_key_end and get_timestamp_for_datekey(
        iterator
    ) < get_timestamp_for_datekey(date_key_end):
        return_range.append(iterator)
        iterator = add_month(iterator)

    return_range.append(date_key_end)

    return return_range


def get_week_number_for_timestamp(ts):
    return datetime.fromtimestamp(ts).isocalendar()[1]


def get_monday_timestamp_for_week_number(week_number):
    string_week = "%s-W%d" % (datetime.now().year, week_number)
    date_string = datetime.datetime.strptime(string_week + "-1", "%Y-W%W-%w")
