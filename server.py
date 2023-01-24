from flask import Flask, request, render_template
from datetime import datetime
from json2html import *
import pytz

from data_containers.data_heatmap import DataHeatmap
from utility.money_helper import format_money
from utility.time_helper import format_timestamp_as_local, get_timestamp_for_datekey, add_month
import coloredlogs, logging

from database.sqlite_client import SqliteClient
from utility.time_observer import TimeObserver

app = Flask(__name__)
db_client = SqliteClient("database/tx.db")

_logger = logging.getLogger('moneypit')
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logFormatter)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(handler)

coloredlogs.install(level='DEBUG')

IGNORED_CATEGORIES = ['check']

def get_adjusted_offset_seconds():
    now = datetime.now(pytz.timezone('America/New_York'))
    return now.utcoffset().total_seconds()

@app.route('/moneypit/heatmap/months')
def heatmap_months():
    ts_start = int(request.args.get('ts_start', 1656561600))
    ts_end = int(request.args.get('ts_end', 1673931600))

    filtered_categories = get_filtered_categories()

    results = db_client.get_data_for_time_slice(ts_start, ts_end)

    heatmap_data_container = DataHeatmap()
    heatmap_data_container.init_from_raw(results, filtered_categories)

    return render_template(
            "heatmap.html",
            date_start=format_timestamp_as_local(ts_start, "%B %d, %Y"),
            date_end=format_timestamp_as_local(ts_end, "%B %d, %Y"),
            heatmap_data_container=heatmap_data_container,
            categories=sorted([a[1] for a in filtered_categories])
        )

@app.route('/moneypit/heatmap/transactions')
def heatmap_month_transactions():
    date_key = request.args.get('date-key')
    category = request.args.get('category')
    ts_start = TimeObserver.get_timestamp_from_date_string(date_key, '%Y-%m')
    ts_end = TimeObserver.get_timestamp_from_date_string(add_month(date_key), '%Y-%m')

    logging.debug("%s to %s" % (ts_start, ts_end))

    results = db_client.get_data_for_time_slice(ts_start, ts_end, category)

    for result in results:
        result['Timestamp'] = format_timestamp_as_local(result['Timestamp'], '%Y/%m/%d')
        result['MoneySpent'] = format_money(abs(result['MoneySpent']))

    return render_template(
            "transactions.html",
            date_start=format_timestamp_as_local(ts_start, '%Y/%m/%d'),
            date_end=format_timestamp_as_local(ts_end, '%Y/%m/%d'),
            data=results,
            category=category
        )

def get_filtered_categories():
    categories = db_client.get_categories()

    return [a for a in categories if a[1] not in IGNORED_CATEGORIES]
