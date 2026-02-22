import logging

from data_containers.color import Color
from utility.money_helper import format_money
from utility.time_helper import (
    get_timestamp_month_integer,
    get_timestamp_year_integer,
    get_date_keys_for_timestamp_range,
)

# Hue range for the spending gradient (HSL, 0–1 scale)
# 0.08 = warm yellow-orange, 0.0 = red
_HUE_LOW = 0.08
_HUE_HIGH = 0.0
# Lightness range: dim for low spend, bright for high spend
_LIGHTNESS_LOW = 0.15
_LIGHTNESS_HIGH = 0.55
# Saturation stays high so colors are vivid on a dark background
_SATURATION = 0.9
# Surplus color: teal-green
_HUE_SURPLUS = 0.45
_LIGHTNESS_SURPLUS = 0.38


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

    def get_total_for_date(self, date_key, only_positive=False, only_negative=False):
        if only_negative and only_positive:
            raise Exception("You cant have both positive only and negative only")

        category_data = self.the_matrix.get(date_key)

        total = 0
        for category in category_data:
            if only_positive and category_data[category] > 0:
                total = total + category_data[category]
            elif only_negative and category_data[category] < 0:
                total = total + category_data[category]
            elif not only_negative and not only_positive:
                total = total + category_data[category]

        return format_money(total)

    def init_from_raw(self, data, categories):
        # Find min timestamp
        ts_min = None
        for datapoint in data:
            if ts_min is None or ts_min > datapoint["Timestamp"]:
                ts_min = datapoint["Timestamp"]

        # Find max timestamp
        ts_max = None
        for datapoint in data:
            if ts_max is None or ts_max < datapoint["Timestamp"]:
                ts_max = datapoint["Timestamp"]

        if not ts_min and not ts_max:
            return

        month_range = get_date_keys_for_timestamp_range(ts_min, ts_max)

        all_categories_dict = {}
        for category in categories:
            all_categories_dict[category[1]] = 0

        for year_and_month_key in month_range:
            # year_and_month_key = month.strftime('%Y-%m')
            if year_and_month_key not in self.the_matrix:
                self.the_matrix[year_and_month_key] = all_categories_dict.copy()

        # {'CategoryName': 'amazon', 'MoneySpent': -9.99, 'Timestamp'
        for datapoint in data:
            category = datapoint["CategoryName"]
            money_spent = datapoint["MoneySpent"]
            timestamp = datapoint["Timestamp"]

            if category not in [a[1] for a in categories]:
                continue

            month_integer = get_timestamp_month_integer(timestamp)
            year_integer = get_timestamp_year_integer(timestamp)

            # Why am I doing this here and not the database?  Sqlite can't do it.  No dates.

            date_key = str(year_integer) + "-" + str(month_integer)

            self.the_matrix[date_key][category] += money_spent

        # Clean up the massive decimal places
        for date in self.the_matrix:
            for category in self.the_matrix[date]:
                self.the_matrix[date][category] = round(
                    self.the_matrix.get(date)[category], 2
                )
                if category not in self.by_category:
                    self.by_category[category] = {}

                self.by_category[category][date] = Color(Color.WHITE)

        for category in self.by_category:
            entire_list = []
            least_money_spent = None
            most_money_spent = None

            for date in self.by_category[category]:
                money_value = self.the_matrix[date][category]
                entire_list.append(money_value)
                if least_money_spent is None or (
                    money_value < 0 and money_value > least_money_spent
                ):
                    least_money_spent = money_value

                if most_money_spent is None or (
                    money_value < 0 and money_value < most_money_spent
                ):
                    most_money_spent = money_value

            for date in self.by_category[category]:
                money_value = self.the_matrix[date][category]
                if money_value < 0 and most_money_spent < least_money_spent:
                    # Normalise: 0.0 = smallest spend in this category, 1.0 = largest
                    ratio = (money_value - least_money_spent) / (most_money_spent - least_money_spent)

                    hue = _HUE_LOW + ratio * (_HUE_HIGH - _HUE_LOW)
                    lightness = _LIGHTNESS_LOW + ratio * (_LIGHTNESS_HIGH - _LIGHTNESS_LOW)

                    color = Color(Color.RED)
                    color.hue = hue
                    color.saturation = _SATURATION
                    color.set_lightness(lightness)

                    self.by_category[category][date] = color
                elif money_value < 0 and most_money_spent == least_money_spent:
                    # Only one data point — show a mid-intensity amber
                    color = Color(Color.RED)
                    color.hue = _HUE_LOW
                    color.saturation = _SATURATION
                    color.set_lightness((_LIGHTNESS_LOW + _LIGHTNESS_HIGH) / 2)
                    self.by_category[category][date] = color
                elif money_value > 0:
                    color = Color(Color.GREEN)
                    color.hue = _HUE_SURPLUS
                    color.saturation = _SATURATION
                    color.set_lightness(_LIGHTNESS_SURPLUS)
                    self.by_category[category][date] = color
