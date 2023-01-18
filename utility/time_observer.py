import datetime

class TimeObserver:

    DATE_FORMAT = '%Y-%m-%d'

    @classmethod
    def get_timestamp_from_date_string(cls, the_datetime):
        return round(datetime.datetime.strptime(the_datetime, TimeObserver.DATE_FORMAT).timestamp())

    @classmethod
    def get_now_date_string(cls):
        return datetime.date.today().strftime(TimeObserver.DATE_FORMAT)
