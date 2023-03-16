from flask import Flask, request, render_template
from datetime import datetime
from json2html import *
import pytz
from flask_wtf import FlaskForm
from wtforms import FileField, SubmitField

from categories.categorizer import Categorizer
from data_containers.data_heatmap import DataHeatmap
from data_containers.input_file import InputFile
from utility.money_helper import format_money
from utility.time_helper import format_timestamp, get_timestamp_for_datekey, add_month, get_datekey_for_timestamp, \
    timestamp_now
import coloredlogs, logging

from database.sqlite_client import SqliteClient
from utility.time_observer import TimeObserver

app = Flask(__name__)
app.config['SECRET_KEY'] = 'supersecretkey'
db_client = SqliteClient("database/tx.db")

_logger = logging.getLogger('moneypit')
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logFormatter)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(handler)

coloredlogs.install(level='DEBUG')

IGNORED_CATEGORIES = ['credit card payment', 'transfer']

def get_adjusted_offset_seconds():
    now = datetime.now(pytz.timezone('America/New_York'))
    return now.utcoffset().total_seconds()

@app.route('/moneypit/heatmap/months')
def heatmap_months():
    date_key_now = get_datekey_for_timestamp(timestamp_now())

    ts_start = int(request.args.get('ts_start', get_timestamp_for_datekey(add_month(date_key_now, -6))))
    ts_end = int(request.args.get('ts_end',  get_timestamp_for_datekey(add_month(date_key_now, 1))))

    filtered_categories = get_filtered_categories()

    logging.debug('looking up data with %d -> %d' % (ts_start, ts_end))
    results = db_client.get_data_for_time_slice(ts_start, ts_end)

    heatmap_data_container = DataHeatmap()
    heatmap_data_container.init_from_raw(results, filtered_categories)

    return render_template(
            "heatmap.html",
            date_start=format_timestamp(ts_start, "%B %d, %Y"),
            date_end=format_timestamp(ts_end, "%B %d, %Y"),
            heatmap_data_container=heatmap_data_container,
            categories=sorted([a[1] for a in filtered_categories])
        )

@app.route('/moneypit/heatmap/transactions')
def heatmap_month_transactions():
    return render_transactions_page(request.args.get('date-key'), request.args.get('category'))

def render_transactions_page(date_key, category):
    ts_start = TimeObserver.get_timestamp_from_date_string(date_key, '%Y-%m')
    ts_end = TimeObserver.get_timestamp_from_date_string(add_month(date_key), '%Y-%m')

    logging.debug("%s to %s" % (ts_start, ts_end))

    results = db_client.get_data_for_time_slice(ts_start, ts_end, category)

    for result in results:
        result['Timestamp'] = format_timestamp(result['Timestamp'], '%Y/%m/%d')
        result['MoneySpent'] = format_money(abs(result['MoneySpent']))

    return render_template(
        "transactions.html",
        date_start=format_timestamp(ts_start, "%B %d, %Y"),
        date_end=format_timestamp(ts_end, "%B %d, %Y"),
        ts_start=ts_start,
        ts_end=ts_end,
        data=results,
        category=category,
        categories_list=db_client.get_categories(),
        date_key=date_key
    )

@app.route('/moneypit/transaction/category', methods=["POST"])
def change_tx_category():
    tx_id = request.form['tx-id']
    date_key = request.form['date-key']
    category_id = request.form['category-id']
    current_category = request.form['current-category']

    db_client.update_category(tx_id, category_id)

    return render_transactions_page(date_key, current_category)

class UploadFileForm(FlaskForm):
    file = FileField('File')
    submit = SubmitField('Upload file')

@app.route('/moneypit/transaction/upload', methods=["POST", "GET"])
def upload_file():
    latest_source_dates = db_client.get_latest_source_dates()

    form = UploadFileForm()
    if form.validate_on_submit():
        file = form.file.data
        filepath = '/tmp/' + file.filename
        file.save(filepath)

        input_file = InputFile(db_client)

        input_file.insert_file(filepath)

        return render_file_transactions_page()
    return render_template("file_input.html", form=form, latest_source_dates=latest_source_dates)

def render_file_transactions_page():
    open_transactions = db_client.get_uncategorized_transactions()

    categorizer = Categorizer(db_client)

    open_transactions_categorized = []
    for tx_id, denomination, memo, date, source in open_transactions:
        category_guess = categorizer.guess_best_category(memo)

        category_name = ''
        if category_guess is not None:
            category_name = category_guess['category_name']
        open_transactions_categorized.append((tx_id, denomination, memo, date, source, category_name))

    return render_template(
        "resolve_categories.html",
        open_txs=open_transactions_categorized,
        categories_list=db_client.get_categories(),
    )

@app.route('/moneypit/transactions/uncategorized')
def show_uncategorized_transactions():
    return render_file_transactions_page()

def get_filtered_categories():
    categories = db_client.get_categories()

    return [a for a in categories if a[1] not in IGNORED_CATEGORIES]

@app.route('/moneypit/transactions/category', methods=['POST'])
def save_categories():
    category_ids = request.form.getlist('category-id')
    tx_ids = request.form.getlist('tx-id')

    iter = 0
    while iter < len(category_ids):
        db_client.update_category(int(tx_ids[iter]), int(category_ids[iter]))
        iter = iter + 1

    return heatmap_months()

@app.route('/moneypit/categories', methods=["GET", "POST"])
def manage_categories():

    if request.method == 'POST':
        category_input = request.form['category-input']

        if category_input:
            db_client.insert_category(category_input)

    categories = db_client.get_categories()

    return render_template(
        "categories.html",
        data=[a[1] for a in categories]
    )

