from flask import Flask, request, render_template
from datetime import datetime
from json2html import *
import pytz

from data_containers.data_heatmap import DataHeatmap
from utility.time_helper import format_timestamp_as_local
import coloredlogs, logging

from database.sqlite_client import SqliteClient

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

    categories = db_client.get_categories()

    filtered_categories = [a for a in categories if a[1] not in IGNORED_CATEGORIES]

    results = db_client.get_data_for_time_slice(ts_start, ts_end)

    heatmap_data_container = DataHeatmap()
    heatmap_data_container.init_from_raw(results, filtered_categories)

    return render_template(
            "heatmap.html",
            date_start=format_timestamp_as_local(ts_start),
            date_end=format_timestamp_as_local(ts_end),
            heatmap_data_container=heatmap_data_container,
            categories=sorted([a[1] for a in filtered_categories])
        )
