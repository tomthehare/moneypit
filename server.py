import os
import shutil
import sys

from flask import Flask, request, render_template, redirect, jsonify, session, url_for, flash, send_file, after_this_request
from datetime import datetime
from json2html import *
import pytz
from flask_wtf import FlaskForm
from flask_wtf.file import FileRequired
from wtforms import FileField, SubmitField, SelectField

from categories.categorizer import Categorizer
from data_containers.data_heatmap import DataHeatmap
from data_containers.input_file import InputFile
from utility.money_helper import format_money
from utility.time_helper import (
    format_timestamp,
    get_timestamp_for_datekey,
    add_month,
    get_datekey_for_timestamp,
    get_date_keys_for_timestamp_range,
    timestamp_now,
    get_week_number_for_timestamp,
    get_monday_timestamp_for_week_number,
)
import coloredlogs, logging

from database.sqlite_client import SqliteClient
from utility.time_observer import TimeObserver

app = Flask(__name__, static_url_path="/moneypit/static")
app.config["SECRET_KEY"] = "supersecretkey"
db_client = SqliteClient("sqlite/tx.db")

_logger = logging.getLogger("moneypit")
logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logFormatter)
_logger.setLevel(logging.DEBUG)
_logger.addHandler(handler)

coloredlogs.install(level="DEBUG")

IGNORED_CATEGORIES = ["credit card payment", "account transfers"]


@app.context_processor
def inject_uncategorized_count():
    return {"uncategorized_count": db_client.get_uncategorized_transactions_count()}


@app.template_filter("add_month_filter")
def add_month_filter(date_key, month_count=1):
    return add_month(date_key, month_count)


def get_adjusted_offset_seconds():
    now = datetime.now(pytz.timezone("America/New_York"))
    return now.utcoffset().total_seconds()


@app.route("/moneypit/heatmap/months")
def heatmap_months():
    core_expenses_param = request.args.get("core_expenses", "")
    exclude_core = core_expenses_param == "Exclude"
    only_core = core_expenses_param == "Only"

    date_key_now = get_datekey_for_timestamp(timestamp_now())

    if request.args.get("ts_start"):
        ts_start_key = request.args.get("ts_start")
        ts_start = get_timestamp_for_datekey(ts_start_key)
    else:
        ts_start_key = add_month(date_key_now, -6)
        ts_start = get_timestamp_for_datekey(ts_start_key)

    if request.args.get("ts_end"):
        ts_end_key = request.args.get("ts_end")
        ts_end = get_timestamp_for_datekey(ts_end_key)
    else:
        ts_end_key = add_month(date_key_now, 1)
        ts_end = get_timestamp_for_datekey(ts_end_key)

    core_expense_names = db_client.get_core_expense_category_names()
    additional_ignored_categories = []
    if exclude_core:
        additional_ignored_categories = core_expense_names

    filtered_categories = get_filtered_categories(
        additional_ignored_categories,
        only_core_expenses=only_core,
        core_expense_names=core_expense_names,
    )

    logging.debug("looking up data with %d -> %d" % (ts_start, ts_end))
    results = db_client.get_data_for_time_slice(ts_start, ts_end)

    heatmap_data_container = DataHeatmap()
    heatmap_data_container.init_from_raw(results, filtered_categories)

    return render_template(
        "heatmap.html",
        date_start=format_timestamp(ts_start, "%B %d, %Y"),
        date_end=format_timestamp(ts_end, "%B %d, %Y"),
        ts_start=ts_start_key,
        ts_end=ts_end_key,
        heatmap_data_container=heatmap_data_container,
        categories=sorted([a[1] for a in filtered_categories]),
        core_expenses_param=core_expenses_param,
    )


@app.route("/moneypit/graphs")
def graphs():
    date_key_now = get_datekey_for_timestamp(timestamp_now())

    if request.args.get("ts_start"):
        ts_start_key = request.args.get("ts_start")
        ts_start = get_timestamp_for_datekey(ts_start_key)
    else:
        ts_start_key = add_month(date_key_now, -6)
        ts_start = get_timestamp_for_datekey(ts_start_key)

    if request.args.get("ts_end"):
        ts_end_key = request.args.get("ts_end")
        ts_end = get_timestamp_for_datekey(ts_end_key)
    else:
        ts_end_key = add_month(date_key_now, 1)
        ts_end = get_timestamp_for_datekey(ts_end_key)

    filtered_categories = get_filtered_categories()
    results = db_client.get_data_for_time_slice(ts_start, ts_end)

    heatmap_data_container = DataHeatmap()
    heatmap_data_container.init_from_raw(results, filtered_categories)

    # Aggregate each category's total spend across the whole date range
    category_totals = {}
    for date_key in heatmap_data_container.get_dates():
        for cat in heatmap_data_container.the_matrix[date_key]:
            val = heatmap_data_container.the_matrix[date_key][cat]
            category_totals[cat] = category_totals.get(cat, 0) + val

    # Sankey: node 0 = "Total Spend", nodes 1..N = expense categories only
    expense_cats = {k: round(abs(v), 2) for k, v in category_totals.items() if v < 0}

    nodes = ["Total Spend"] + list(expense_cats.keys())

    source_indices = []
    target_indices = []
    values = []
    link_labels = []

    total_spend = sum(expense_cats.values())

    for i, (cat, amt) in enumerate(expense_cats.items()):
        source_indices.append(0)
        target_indices.append(i + 1)
        values.append(amt)
        link_labels.append(f"${amt:,.2f}")

    import json
    sankey_data = json.dumps({
        "nodes": nodes,
        "sources": source_indices,
        "targets": target_indices,
        "values": values,
        "link_labels": link_labels,
    })

    # Time series: per-month per-category amounts for stacked area chart
    # Sort categories by total descending so largest bands are at bottom
    month_range = get_date_keys_for_timestamp_range(ts_start, ts_end)
    time_series_months = month_range
    time_series_categories = sorted(expense_cats.keys(), key=lambda c: expense_cats[c], reverse=True)
    time_series_data = {
        cat: [
            round(abs(heatmap_data_container.the_matrix.get(m, {}).get(cat, 0)), 2)
            for m in month_range
        ]
        for cat in time_series_categories
    }
    time_series_json = json.dumps({
        "months": time_series_months,
        "categories": time_series_categories,
        "data": time_series_data,
    })

    return render_template(
        "breakdown.html",
        sankey_data=sankey_data,
        time_series_data=time_series_json,
        date_start=format_timestamp(ts_start, "%B %d, %Y"),
        date_end=format_timestamp(ts_end, "%B %d, %Y"),
        ts_start=ts_start_key,
        ts_end=ts_end_key,
        total_spend=total_spend,
    )


@app.route("/moneypit/heatmap/weeks")
def heatmap_week_transactions():
    core_expenses_param = request.args.get("core_expenses", "")
    exclude_core = core_expenses_param == "Exclude"
    only_core = core_expenses_param == "Only"

    current_week = get_week_number_for_timestamp(timestamp_now())

    weeks_back = request.args.get("weeks-back") or 4

    ts_start = get_monday_timestamp_for_week_number(current_week - weeks_back)

    ts_end = timestamp_now()

    core_expense_names = db_client.get_core_expense_category_names()
    additional_ignored_categories = []
    if exclude_core:
        additional_ignored_categories = core_expense_names

    filtered_categories = get_filtered_categories(
        additional_ignored_categories,
        only_core_expenses=only_core,
        core_expense_names=core_expense_names,
    )

    logging.debug("looking up data with %d -> %d" % (ts_start, ts_end))
    results = db_client.get_data_for_time_slice(ts_start, ts_end)


@app.route("/moneypit/heatmap/transactions")
def heatmap_month_transactions():
    ts_start_key = request.args.get("ts_start")
    ts_end_key = request.args.get("ts_end")
    date_key = request.args.get("date-key")
    category = request.args.get("category")

    if ts_start_key and ts_end_key and category:
        ts_start = get_timestamp_for_datekey(ts_start_key)
        ts_end = get_timestamp_for_datekey(ts_end_key)
        return render_transactions_page(
            category=category,
            ts_start_key=ts_start_key,
            ts_end_key=ts_end_key,
            core_expenses_param=request.args.get("core_expenses", ""),
        )
    elif date_key and category:
        return render_transactions_page(
            date_key=date_key,
            category=category,
            core_expenses_param=request.args.get("core_expenses", ""),
        )
    else:
        return redirect("/moneypit/heatmap/months")


def render_transactions_page(category, date_key=None, ts_start_key=None, ts_end_key=None, core_expenses_param=None):
    if ts_start_key and ts_end_key:
        ts_start = get_timestamp_for_datekey(ts_start_key)
        ts_end = get_timestamp_for_datekey(ts_end_key)
        date_key = ts_start_key
    else:
        ts_start = TimeObserver.get_timestamp_from_date_string(date_key, "%Y-%m")
        ts_end = TimeObserver.get_timestamp_from_date_string(add_month(date_key), "%Y-%m")
        ts_start_key = date_key
        ts_end_key = add_month(date_key)

    logging.debug("%s to %s" % (ts_start, ts_end))

    results = db_client.get_data_for_time_slice(ts_start, ts_end, category)

    for result in results:
        result["Timestamp"] = format_timestamp(result["Timestamp"], "%Y/%m/%d")
        result["MoneySpent"] = format_money(result["MoneySpent"])

    return render_template(
        "transactions.html",
        date_start=format_timestamp(ts_start, "%B %d, %Y"),
        date_end=format_timestamp(ts_end, "%B %d, %Y"),
        ts_start=ts_start,
        ts_end=ts_end,
        data=results,
        category=category,
        categories_list=db_client.get_categories(),
        date_key=date_key,
        ts_start_key=ts_start_key,
        ts_end_key=ts_end_key,
        core_expenses_param=core_expenses_param or "",
    )


@app.route("/moneypit/transaction/category", methods=["POST"])
def change_tx_category():
    tx_id = request.form["tx-id"]
    category_id = request.form["category-id"]
    current_category = request.form["current-category"]
    ts_start_key = request.form.get("ts_start_key")
    ts_end_key = request.form.get("ts_end_key")
    date_key = request.form.get("date-key")
    core_expenses_param = request.form.get("core_expenses_param", "")

    db_client.update_category(tx_id, category_id)

    if ts_start_key and ts_end_key:
        return render_transactions_page(category=current_category, ts_start_key=ts_start_key, ts_end_key=ts_end_key, core_expenses_param=core_expenses_param)
    return render_transactions_page(category=current_category, date_key=date_key, core_expenses_param=core_expenses_param)


@app.route("/moneypit/transaction/delete", methods=["POST"])
def delete_tx():
    tx_id = request.form["txid"]
    current_category = request.form["currentcategory"]
    ts_start_key = request.form.get("ts_start_key")
    ts_end_key = request.form.get("ts_end_key")
    date_key = request.form.get("datekey")
    core_expenses_param = request.form.get("core_expenses_param", "")

    db_client.delete_transaction(tx_id)

    if ts_start_key and ts_end_key:
        return render_transactions_page(category=current_category, ts_start_key=ts_start_key, ts_end_key=ts_end_key, core_expenses_param=core_expenses_param)
    return render_transactions_page(category=current_category, date_key=date_key, core_expenses_param=core_expenses_param)


class UploadFileForm(FlaskForm):
    file = FileField("File", validators=[FileRequired("Please select a file to upload.")])
    submit = SubmitField("Upload file")


SOURCE_CHOICES = [
    ("Chase", "Chase"),
    ("CapitalOne", "Capital One"),
    ("Barclays", "Barclays"),
    ("AmericanExpress", "American Express"),
]


@app.route("/moneypit/backup/download")
def backup_download():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sqlite", "tx.db")
    if not os.path.isfile(db_path):
        return "Database not found", 404
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    filename = f"moneypit-backup-{timestamp}.db"
    temp_path = os.path.join("/tmp", filename)
    shutil.copy2(db_path, temp_path)

    @after_this_request
    def remove_temp(response):
        try:
            os.remove(temp_path)
        except OSError:
            pass
        return response

    return send_file(temp_path, as_attachment=True, download_name=filename)


@app.route("/moneypit/transaction/upload", methods=["POST", "GET"])
def upload_file():
    latest_source_dates = db_client.get_latest_source_dates()

    form = UploadFileForm()
    if form.validate_on_submit():
        file = form.file.data
        filepath = "/tmp/" + file.filename
        file.save(filepath)

        input_file = InputFile(db_client)
        try:
            input_file.insert_file(filepath)
            flash("File uploaded successfully", "success")
            return redirect(url_for("heatmap_months"))
        except Exception as e:
            if "No idea how to parse it:" in str(e):
                session["upload_pending_path"] = filepath
                session["upload_pending_filename"] = file.filename
                return redirect(url_for("select_source"))
            raise
    return render_template(
        "file_input.html", form=form, latest_source_dates=latest_source_dates
    )


@app.route("/moneypit/transaction/upload/select-source", methods=["GET", "POST"])
def select_source():
    filepath = session.get("upload_pending_path")
    original_filename = session.get("upload_pending_filename", "file")

    if not filepath or not os.path.isfile(filepath):
        session.pop("upload_pending_path", None)
        session.pop("upload_pending_filename", None)
        return redirect(url_for("upload_file"))

    if request.method == "POST":
        source_name = request.form.get("source")
        if source_name not in [c[0] for c in SOURCE_CHOICES]:
            return render_template(
                "select_source.html",
                original_filename=original_filename,
                source_choices=SOURCE_CHOICES,
                error="Please select a valid source.",
            )

        date_str = TimeObserver.get_now_date_string()
        timestamp_str = datetime.now().strftime("%H%M%S")
        new_filename = f"{source_name}_{date_str}_{timestamp_str}.csv"
        new_filepath = "/tmp/" + new_filename

        try:
            shutil.copy2(filepath, new_filepath)
            input_file = InputFile(db_client)
            input_file.insert_file(new_filepath)
        finally:
            try:
                os.remove(filepath)
            except OSError:
                pass

        session.pop("upload_pending_path", None)
        session.pop("upload_pending_filename", None)
        flash("File uploaded successfully", "success")
        return redirect(url_for("heatmap_months"))

    return render_template(
        "select_source.html",
        original_filename=original_filename,
        source_choices=SOURCE_CHOICES,
    )


def render_uncategorized_transactions_group_page():
    open_transactions = db_client.get_uncategorized_transactions()

    categorizer = Categorizer(db_client)

    transaction_group = []
    tx_ids = []
    category_name = ""
    category_guess = None
    total_remaining = len(open_transactions)
    if len(open_transactions) > 0:
        first_memo_raw = open_transactions[0][2]

        # Find every other transaction that has the same memo (group by raw bank description)
        for tx_id, denomination, memo_raw, custom_memo, date, source in open_transactions:
            if memo_raw == first_memo_raw:
                display_memo = custom_memo if custom_memo else memo_raw
                transaction_group.append((tx_id, denomination, display_memo, date, source))
                tx_ids.append(tx_id)

        category_guess = categorizer.guess_best_category(first_memo_raw)
        _logger.info("Guessed category for group: " + str(category_guess))

    if category_guess:
        category_name = category_guess["category_name"]

    return render_template(
        "resolve_category_group.html",
        category_guess=category_name,
        transaction_group=transaction_group,
        categories=db_client.get_categories(),
        tx_ids=",".join(map(str, tx_ids)),
        total_remaining=total_remaining,
    )


def render_file_transactions_page():
    open_transactions = db_client.get_uncategorized_transactions()

    categorizer = Categorizer(db_client)

    open_transactions_categorized = []
    for tx_id, denomination, memo_raw, custom_memo, date, source in open_transactions:
        display_memo = custom_memo if custom_memo else memo_raw
        category_guess = categorizer.guess_best_category(memo_raw)

        category_name = ""
        if category_guess is not None:
            category_name = category_guess["category_name"]
        open_transactions_categorized.append(
            (tx_id, denomination, display_memo, date, source, category_name)
        )

    return render_template(
        "resolve_categories.html",
        open_txs=open_transactions_categorized,
        categories_list=db_client.get_categories(),
    )


@app.route("/moneypit/transactions/uncategorized/group")
def show_uncategorized_transactions_group():
    return render_uncategorized_transactions_group_page()


@app.route("/moneypit/transactions/uncategorized")
def show_uncategorized_transactions():
    return render_file_transactions_page()


def get_filtered_categories(
    additional_ignored_categories=None,
    only_core_expenses=False,
    core_expense_names=None,
):
    if not additional_ignored_categories:
        additional_ignored_categories = []
    if core_expense_names is None:
        core_expense_names = db_client.get_core_expense_category_names()

    categories = db_client.get_categories()

    if only_core_expenses:
        return [a for a in categories if a[1] in core_expense_names]

    return [
        a
        for a in categories
        if a[1] not in (IGNORED_CATEGORIES + additional_ignored_categories)
    ]


@app.route("/moneypit/transactions/category", methods=["POST"])
def save_categories():
    category_ids = request.form.getlist("category-id")
    tx_ids = request.form.getlist("tx-id")

    iter = 0
    while iter < len(category_ids):
        cat_id = category_ids[iter]
        db_client.update_category(int(tx_ids[iter]), int(cat_id) if cat_id else None)
        iter = iter + 1

    return heatmap_months()


@app.route("/moneypit/transaction-group/category", methods=["POST"])
def save_category_for_tx_group():
    category_id = request.form.get("category-id", "").strip()
    tx_ids = request.form["tx-ids"].split(",")

    cat_id = int(category_id) if category_id else None
    iter = 0
    while iter < len(tx_ids):
        db_client.update_category(int(tx_ids[iter]), cat_id)
        iter = iter + 1

    # Make note of it for the future so it will show up next time (only when categorizing, not uncategorizing)
    if cat_id:
        tx_data = db_client.get_transaction(tx_ids[0])
        memo_raw = tx_data.get("memo_raw") or tx_data["memo"]
        db_client.insert_memo_to_category(memo_raw, cat_id)

    return render_uncategorized_transactions_group_page()


@app.route("/moneypit/transactions", methods=["GET"])
def all_transactions():
    categories = db_client.get_categories()
    return render_template("transactions_search.html", categories=categories)


@app.route("/moneypit/api/transactions/search", methods=["GET"])
def api_search_transactions():
    from Levenshtein import distance as levenshtein_distance

    memo_query = request.args.get("q", "").strip()
    category_filter = request.args.get("category", "").strip()

    results = db_client.search_transactions(
        memo_query=memo_query,
        category_filter=category_filter,
        limit=300,
    )

    # If there's a query, sort by fuzzy similarity (best match first)
    if memo_query:
        query_lower = memo_query.lower()
        def score(tx):
            memo_lower = tx["Memo"].lower()
            # Exact substring → score 0 (best); otherwise Levenshtein on the memo
            if query_lower in memo_lower:
                return 0
            return levenshtein_distance(query_lower, memo_lower[:len(query_lower) + 10])
        results.sort(key=score)

    return jsonify(results[:200])


@app.route("/moneypit/files", methods=["GET"])
def list_files():
    all_files = db_client.get_all_input_files()
    source_filter = request.args.get("source", "").strip()

    for f in all_files:
        if f.get("date_min_ts") and f.get("date_max_ts"):
            f["date_range"] = format_timestamp(f["date_min_ts"], "%Y/%m/%d") + " – " + format_timestamp(f["date_max_ts"], "%Y/%m/%d")
        else:
            f["date_range"] = ""

    files = [f for f in all_files if f["source_bank"] == source_filter] if source_filter else all_files
    sources = sorted({f["source_bank"] for f in all_files}) if all_files else []
    return render_template("files.html", files=files, sources=sources, source_filter=source_filter)


@app.route("/moneypit/files/<int:file_id>", methods=["GET"])
def view_file(file_id):
    files = db_client.get_all_input_files()
    current_file = next((f for f in files if f["file_id"] == file_id), None)
    if not current_file:
        return redirect("/moneypit/files")
    transactions = db_client.get_transactions_for_file(file_id)
    categories = db_client.get_categories()
    return render_template(
        "file_transactions.html",
        current_file=current_file,
        transactions=transactions,
        categories=categories,
    )


@app.route("/moneypit/files/<int:file_id>/delete", methods=["POST"])
def delete_file_and_transactions(file_id):
    files = db_client.get_all_input_files()
    if not any(f["file_id"] == file_id for f in files):
        return redirect("/moneypit/files")
    db_client.delete_file_and_transactions(file_id)
    return redirect("/moneypit/files")


@app.route("/moneypit/api/transaction/<int:tx_id>/category", methods=["POST"])
def api_update_tx_category(tx_id):
    data = request.get_json()
    if data is None or "category_id" not in data:
        return jsonify({"ok": False, "error": "missing category_id"}), 400
    category_id = data.get("category_id")
    # Allow null/None to set uncategorized
    cat_id = int(category_id) if category_id is not None and category_id != "" else None
    db_client.update_category(tx_id, cat_id)
    return jsonify({"ok": True, "tx_id": tx_id, "category_id": cat_id})


@app.route("/moneypit/api/transaction/<int:tx_id>/custom-memo", methods=["POST"])
def api_update_tx_custom_memo(tx_id):
    data = request.get_json()
    custom_memo = data.get("custom_memo")
    if custom_memo is None:
        return jsonify({"ok": False, "error": "missing custom_memo"}), 400
    db_client.update_custom_memo(tx_id, custom_memo)
    return jsonify({"ok": True, "tx_id": tx_id, "custom_memo": custom_memo})


@app.route("/moneypit/categories/matches", methods=["GET"])
def manage_category_matches():
    match_strings = db_client.get_match_strings_with_tx_counts()
    categories = db_client.get_categories()

    # Pre-group by category so the template doesn't have to track state mid-loop
    groups = {}
    for entry in match_strings:
        cat = entry["category_name"]
        if cat not in groups:
            groups[cat] = []
        groups[cat].append(entry)

    return render_template(
        "category_matches.html",
        groups=groups,
        categories=categories,
    )


@app.route("/moneypit/categories/matches/add", methods=["POST"])
def add_category_match():
    match_string = request.form.get("match-string", "").strip()
    category_id = request.form.get("category-id", "").strip()
    if match_string and category_id:
        rows_updated = db_client.add_match_rule_and_apply(match_string, category_id)
        _logger.info(f"Added match rule '{match_string}' -> category_id={category_id}, {rows_updated} transactions backfilled")
    return redirect("/moneypit/categories/matches")


@app.route("/moneypit/categories/matches/reassign", methods=["POST"])
def reassign_category_match():
    match_id = request.form["match-id"]
    new_category_id = request.form["new-category-id"]
    rows_updated = db_client.reassign_match_string(match_id, new_category_id)
    _logger.info(f"Reassigned match_id={match_id} to category_id={new_category_id}, {rows_updated} transactions updated")
    return redirect("/moneypit/categories/matches")


@app.route("/moneypit/categories/core-expenses", methods=["GET"])
def manage_core_expenses():
    core_expenses = db_client.get_core_expense_categories()
    all_categories = db_client.get_categories()
    core_ids = {c[0] for c in core_expenses}
    available_categories = [c for c in all_categories if c[0] not in core_ids]
    return render_template(
        "core_expenses.html",
        core_expenses=core_expenses,
        available_categories=available_categories,
    )


@app.route("/moneypit/categories/core-expenses/add", methods=["POST"])
def add_core_expense():
    category_id = request.form.get("category-id", "").strip()
    if category_id:
        db_client.add_core_expense_category(category_id)
        _logger.info(f"Added category_id={category_id} as core expense")
    return redirect(url_for("manage_core_expenses"))


@app.route("/moneypit/categories/core-expenses/remove", methods=["POST"])
def remove_core_expense():
    category_id = request.form.get("category-id", "").strip()
    if category_id:
        db_client.remove_core_expense_category(category_id)
        _logger.info(f"Removed category_id={category_id} from core expenses")
    return redirect(url_for("manage_core_expenses"))


@app.route("/moneypit/categories", methods=["GET", "POST"])
def manage_categories():

    if request.method == "POST":
        category_input = request.form["category-input"]

        if category_input:
            db_client.insert_category(category_input)

    categories = db_client.get_categories()

    return render_template("categories.html", data=[a[1] for a in categories])


def _parse_month_key_param(arg):
    if arg and len(arg) == 7 and arg[4] == "-":
        return arg
    return get_datekey_for_timestamp(timestamp_now())


def _parse_budget_income_from_form():
    raw = (request.form.get("income") or "").strip().replace(",", "")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _parse_category_amounts_from_form(name_prefix="cat"):
    out = {}
    for key, val in request.form.items():
        p = name_prefix + "-"
        if not key.startswith(p):
            continue
        rest = key[len(p) :]
        if not rest.isdigit():
            continue
        raw = (val or "").strip().replace(",", "")
        try:
            out[int(rest)] = float(raw) if raw else 0.0
        except ValueError:
            out[int(rest)] = 0.0
    return out


def _budget_excluded_name(name):
    return name.lower() in ("transfer", "credit card payment")


def _budget_6mo_window_label(end_month_key):
    """Human-readable range for 6-mo average (months end_month-6 .. end_month-1)."""
    r_start = add_month(end_month_key, -6)
    r_end = add_month(end_month_key, -1)
    return (
        f"{format_timestamp(get_timestamp_for_datekey(r_start), '%b %Y')}"
        f" – {format_timestamp(get_timestamp_for_datekey(r_end), '%b %Y')}"
    )


def _rows_for_month_progress(month_key):
    """List of dicts with category_id, name, budget, tx_sum, remaining."""
    lines = db_client.get_monthly_budget_lines(month_key)
    spend = db_client.get_category_tx_sum_for_month(month_key)
    rows = []
    for cat_id, name in db_client.get_categories():
        if _budget_excluded_name(name):
            continue
        b = lines.get(cat_id, 0.0)
        tx = spend.get(cat_id, 0.0)
        rows.append(
            {
                "category_id": cat_id,
                "name": name,
                "budget": b,
                "tx_sum": tx,
                "remaining": b + tx,
            }
        )
    rows.sort(key=lambda r: r["name"].lower())
    return rows


@app.route("/moneypit/budget", methods=["GET"])
def budget_current_month():
    month_key = _parse_month_key_param(request.args.get("month"))
    prev_m = add_month(month_key, -1)
    next_m = add_month(month_key, 1)
    month_label = format_timestamp(get_timestamp_for_datekey(month_key), "%B %Y")

    income, is_locked = db_client.get_monthly_budget(month_key)
    if income is None:
        return render_template(
            "budget_status.html",
            month_key=month_key,
            month_label=month_label,
            prev_m=prev_m,
            next_m=next_m,
            has_budget=False,
            total_income=0.0,
            is_locked=False,
            rows=[],
        )

    rows = _rows_for_month_progress(month_key)
    return render_template(
        "budget_status.html",
        month_key=month_key,
        month_label=month_label,
        prev_m=prev_m,
        next_m=next_m,
        has_budget=True,
        total_income=income,
        is_locked=is_locked,
        rows=rows,
    )


@app.route("/moneypit/budget/template", methods=["GET", "POST"])
def budget_template():
    if request.method == "POST":
        inc = _parse_budget_income_from_form()
        amounts = _parse_category_amounts_from_form("cat")
        db_client.save_budget_template(inc, amounts)
        flash("Budget template saved.", "success")
        return redirect(url_for("budget_template"))

    income, rows = db_client.get_budget_template()
    anchor = get_datekey_for_timestamp(timestamp_now())
    avgs = db_client.get_category_6mo_avg_monthly_spend(anchor)
    rows = [(c, n, a, avgs.get(c, 0.0)) for c, n, a in rows]
    return render_template(
        "budget_template.html",
        income=income,
        rows=rows,
        avg_window_label=_budget_6mo_window_label(anchor),
    )


@app.route("/moneypit/budget/plan", methods=["GET", "POST"])
def budget_plan_month():
    month_key = _parse_month_key_param(request.args.get("month"))

    if request.method == "POST":
        if request.form.get("action") == "fill_template":
            _inc_m, is_l = db_client.get_monthly_budget(month_key)
            if is_l:
                flash("This month is locked; template was not applied.", "error")
                return redirect(url_for("budget_plan_month", month=month_key))
            db_client.copy_template_to_month(month_key)
            flash("Filled this month from your template.", "success")
            return redirect(url_for("budget_plan_month", month=month_key))

        inc = _parse_budget_income_from_form()
        amounts = _parse_category_amounts_from_form("cat")
        _inc_exist, is_locked = db_client.get_monthly_budget(month_key)
        if is_locked:
            flash("This month is locked. Budget cannot be edited here.", "error")
            return redirect(url_for("budget_plan_month", month=month_key))

        lock_after = request.form.get("action") == "lock"
        db_client.save_monthly_budget(month_key, inc, amounts, is_locked=lock_after)
        if lock_after:
            flash("Budget saved and locked for this month.", "success")
        else:
            flash("Budget saved.", "success")
        return redirect(url_for("budget_plan_month", month=month_key))

    prev_m = add_month(month_key, -1)
    next_m = add_month(month_key, 1)
    month_label = format_timestamp(get_timestamp_for_datekey(month_key), "%B %Y")
    t_income, t_rows = db_client.get_budget_template()
    m_income, is_locked = db_client.get_monthly_budget(month_key)
    if m_income is None:
        rows = t_rows
        form_income = t_income
        is_locked = False
    else:
        m_lines = db_client.get_monthly_budget_lines(month_key)
        rows = [(cid, n, m_lines.get(cid, 0.0)) for cid, n, _ in t_rows]
        form_income = m_income

    avgs = db_client.get_category_6mo_avg_monthly_spend(month_key)
    rows = [(c, n, a, avgs.get(c, 0.0)) for c, n, a in rows]
    return render_template(
        "budget_plan.html",
        month_key=month_key,
        month_label=month_label,
        prev_m=prev_m,
        next_m=next_m,
        income=form_income,
        rows=rows,
        is_locked=is_locked,
        template_income=t_income,
        avg_window_label=_budget_6mo_window_label(month_key),
    )
