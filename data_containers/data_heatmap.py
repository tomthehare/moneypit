import logging

from data_containers.color import Color
from utility.time_helper import get_datetime_for_timestamp, get_timestamp_month_integer, get_timestamp_year_integer
import pandas

MAX_LIGHTNESS = .4

class DataHeatmap:

    def __init__(self):
        self.the_matrix = {}
        self.by_category = {}

    def get_dates(self):
        return self.the_matrix.keys()

    def get_value(self, date_key, category):
        return self.the_matrix.get(date_key)[category]

    def get_rgb(self, date_key, cat):
        return self.by_category[cat][date_key].get_rgb_string()

    def init_from_raw(self, data, categories):
        logging.debug(str(categories))

        # Find min timestamp
        ts_min = None
        for datapoint in data:
            if not ts_min or ts_min > datapoint['Timestamp']:
                ts_min = datapoint['Timestamp']

        # Find max timestamp
        ts_max = None
        for datapoint in data:
            if not ts_max or ts_max < datapoint['Timestamp']:
                ts_max = datapoint['Timestamp']

        date_start = get_datetime_for_timestamp(ts_min)
        date_end = get_datetime_for_timestamp(ts_max)

        month_range = pandas.date_range(
            date_start.strftime('%Y-%m'),
            date_end.strftime('%Y-%m'),
            freq='MS'  # months?
        ).tolist()

        all_categories_dict = {}
        for cat in categories:
            all_categories_dict[cat[1]] = 0

        for month in month_range:
            year_and_month_key = month.strftime('%Y-%m')
            if year_and_month_key not in self.the_matrix:
                self.the_matrix[year_and_month_key] = all_categories_dict.copy()

        # {'CategoryName': 'amazon', 'MoneySpent': -9.99, 'Timestamp'
        for datapoint in data:
            category = datapoint['CategoryName']
            money_spent = datapoint['MoneySpent']
            timestamp = datapoint['Timestamp']

            if category not in [a[1] for a in categories]:
                continue

            month_integer = get_timestamp_month_integer(timestamp)
            year_integer = get_timestamp_year_integer(timestamp)

            # Why am I doing this here and not the database?  Sqlite can't do it.  No dates.

            date_key = str(year_integer) + '-' + str(month_integer)

            self.the_matrix[date_key][category] += abs(money_spent)

        # Clean up the massive decimal places
        for date in self.the_matrix:
            for cat in self.the_matrix[date]:
                self.the_matrix[date][cat] = round(self.the_matrix.get(date)[cat], 2)
                if cat not in self.by_category:
                    self.by_category[cat] = {}

                self.by_category[cat][date] = Color(Color.WHITE)

        for cat in self.by_category:
            entire_list = []
            small = None
            big = None

            for date in self.by_category[cat]:
                money_value = abs(self.the_matrix[date][cat])
                entire_list.append(money_value)
                if not small or money_value < small:
                    small = money_value

                if not big or money_value > big:
                    big = money_value

            for date in self.by_category[cat]:
                money_value = abs(self.the_matrix[date][cat])
                if money_value > 0 and big > small:
                    color = Color(Color.RED)

                    color.set_lightness(abs(1 - ((money_value - small) / (big - small)) * MAX_LIGHTNESS))

                    self.by_category[cat][date] = color




