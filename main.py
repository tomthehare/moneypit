# This is the main file.
from database.sqlite_client import SqliteClient
import os
import sys
import coloredlogs, logging

from parsers.parsers import CapitalOneParser, Parser, BarclaysParser, ChaseParser
from utility.time_observer import TimeObserver

coloredlogs.install(level='DEBUG')

db_client = SqliteClient('database/tx.db')

def get_input_files() -> list[str]:
    return [i.lower() for i in os.listdir('./input') if i.lower().endswith('.csv')]

def get_parser(file_path) -> Parser:
    filename = file_path.split('/')[-1].lower()

    if 'capital_one' in filename \
        or 'capitalone' in filename \
        or 'capital-one' in filename:
        return CapitalOneParser(db_client)
    elif 'barclays' in filename:
        return BarclaysParser(db_client)
    elif 'chase' in filename:
        return ChaseParser(db_client)

    raise Exception('No idea how to parse it: ' + filename)

def process_file(file_path):
    parser = get_parser(file_path)

    db_client.insert_input_file(
        parser.source_id,
        TimeObserver.get_timestamp_from_date_string(TimeObserver.get_now_date_string()),
        TimeObserver.get_now_date_string(),
        file_path
    )
    (file_path_id, processed_success_date) = db_client.get_input_file_id(parser.source_id, file_path)

    if file_path_id and not processed_success_date:
        parser.parse(file_path, file_path_id)

input_files = get_input_files()

try:
    for file in input_files:
        process_file('./input/' + file)

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.error(exc_type, fname, exc_tb.tb_lineno)