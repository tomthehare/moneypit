from flask import Flask, request, jsonify, render_template, render_template_string, Response
from datetime import datetime
from json2html import *
import pytz
from utility.time_helper import format_timestamp_as_local, get_timestamp_month_integer, get_timestamp_year_integer
import logging

from database.sqlite_client import SqliteClient

app = Flask(__name__)
db_client = SqliteClient("database/tx.db")

_logger = logging.getLogger('moneypit')
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logFormatter)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(handler)


def get_adjusted_offset_seconds():
    now = datetime.now(pytz.timezone('America/New_York'))
    return now.utcoffset().total_seconds()

@app.route('/')
def hello():
    _logger.info('Hey there!')
    return 'hey there fella ' + str(get_adjusted_offset_seconds())

@app.route('/moneypit/heatmap')
def heatmap():
    date_start = 1656561600
    date_end = 1673931600

    # I think I need to set up the "matrix" of data up here.  Figure out what months are in the
    # date range, and fill in all categories and all months.

    results = db_client.get_data_for_time_slice(date_start, date_end)

    data = {}

    #{'CategoryName': 'amazon', 'MoneySpent': -9.99, 'Timestamp'
    # need to get a unique list of the categories first?  Maybe from the DB...
    categories = db_client.get_categories()

    for datapoint in results:
        category = datapoint['CategoryName']
        money_spent = datapoint['MoneySpent']
        timestamp = datapoint['Timestamp']

        month_integer = get_timestamp_month_integer(timestamp)
        year_integer = get_timestamp_year_integer(timestamp)

        # Why am I doing this here and not the database?  Sqlite can't do it.  No dates.

        date_key = str(year_integer) + '-' + str(month_integer)

        if category not in data:
            data[category] = {}

        if date_key not in data[category]:
            data[category][date_key] = 0

        data[category][date_key] += money_spent


    return render_template(
            "heatmap.html",
            date_start=format_timestamp_as_local(date_start),
            date_end=format_timestamp_as_local(date_end),
            data=data
        )

# @app.route('/scatter', methods=['GET'])
# def scatter():
#     hours_back = request.args.get("hours_back", default=24, type=int)
#
#     date_start = round(time.time() - (3600 * hours_back))
#     date_end = round(time.time())
#
#     temp_plot_object = graph_helper.get_temperature_graph_object(date_start, date_end)
#     humid_plot_object = graph_helper.get_humidity_graph_object(date_start, date_end)
#
#     summary_details = get_summary_dictionary()
#
#     _logger.info(json.dumps(summary_details, indent=2))
#
#     if not summary_details:
#         inside_temperature = '?'
#         outside_temperature = '?'
#     else:
#         inside_temperature = summary_details['Inside Temperature']['InsideDegreesF']
#         outside_temperature = summary_details['Outside Temperature']['OutsideDegreesF']
#         inside_humidity = summary_details['Inside Humidity']['InsidePercentage']
#         outside_humidity = summary_details['Outside Humidity']['OutsidePercentage']
#
#     fan_config = read_fan_config()
#     watering_queue = get_watering_queue_detailed()
#     valve_config = get_valve_config_dict()
#
#     fan_data_object = graph_helper.get_fan_data(date_start, date_end, 3600)
#     fan_on_off_data = client.read_fan_data(date_start, date_end)
#
#     fan_events = []
#     for event_hash, ts_on, ts_off in fan_on_off_data:
#         if ts_off is None:
#             ts_off = round(time.time())
#
#         minutes_on_total = round(((ts_off - ts_on) / 60))
#
#         hours_on = math.floor(minutes_on_total / 60)
#         minutes_remainder = minutes_on_total % 60
#
#         human_time_string = ""
#         if hours_on > 0:
#             human_time_string = "%d hours " % hours_on
#
#         human_time_string = human_time_string + ("%d minutes" % minutes_remainder)
#
#         fan_events.append(
#             {
#                 'ts_on': format_timestamp_as_local(ts_on),
#                 'ts_off': format_timestamp_as_local(ts_off),
#                 'on_minutes': human_time_string
#             }
#         )
#
#     fan_events.reverse()
#
#     return render_template(
#         "scatter.html",
#         date_start=format_timestamp_as_local(date_start),
#         date_end=format_timestamp_as_local(date_end),
#         temp_plot=temp_plot_object,
#         humid_plot=humid_plot_object,
#         inside_temp=inside_temperature,
#         outside_temp=outside_temperature,
#         delta_temp=round(inside_temperature - outside_temperature, 1),
#         inside_humidity=inside_humidity,
#         outside_humidity=outside_humidity,
#         delta_humidity=round(inside_humidity - outside_humidity, 1),
#         fan_temp=fan_config['fan_temp'],
#         watering_queue=watering_queue,
#         valve_config_list=valve_config,
#         fan_data_object=fan_data_object,
#         fan_events=fan_events
#     )