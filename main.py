# This is the main file.
from categories.categorizer import Categorizer
from data_containers.input_file import InputFile
from database.sqlite_client import SqliteClient
import os
import sys
import coloredlogs, logging

from parsers.parsers import CapitalOneParser, Parser, BarclaysParser, ChaseParser
from utility.time_observer import TimeObserver

coloredlogs.install(level='DEBUG')

db_client = SqliteClient('sqlite/tx.db')

def get_input_files() -> list[str]:
    return [i.lower() for i in os.listdir('./input') if i.lower().endswith('.csv')]

# def get_parser(file_path) -> Parser:
#     filename = file_path.split('/')[-1].lower()
#
#     if 'capital_one' in filename \
#         or 'capitalone' in filename \
#         or 'capital-one' in filename:
#         return CapitalOneParser(db_client)
#     elif 'barclays' in filename:
#         return BarclaysParser(db_client)
#     elif 'chase' in filename:
#         return ChaseParser(db_client)
#
#     raise Exception('No idea how to parse it: ' + filename)

# def process_file(file_path):
#     parser = get_parser(file_path)
#
#     db_client.insert_input_file(
#         parser.source_id,
#         TimeObserver.get_timestamp_from_date_string(TimeObserver.get_now_date_string()),
#         TimeObserver.get_now_date_string(),
#         file_path
#     )
#     (file_path_id, processed_success_date) = db_client.get_input_file_id(parser.source_id, file_path)
#
#     if processed_success_date:
#         logging.debug('Skipping file since already processed: ' + file_path)
#         return
#
#     if file_path_id:
#         parser.parse(file_path, file_path_id)
#
#     return file_path_id


# def determine_category_id(memo, categorizer: Categorizer, sqlite_client: SqliteClient):
#     best_category = categorizer.guess_best_category(memo)
#
#     if best_category:
#         category_name = best_category['category_name']
#         logging.debug('Using best guess category: ' + category_name)
#
#         if not best_category['category_id']:
#             best_category['category_id'] = sqlite_client.get_category_id(category_name)
#
#         return best_category['category_id']
#
#     category_names = categorizer.get_category_names()
#     best_new_category_name = input('No category on file.  Categories on file:\n%s\n\nWhat category should we use?: ' % ('\n'.join(category_names))).lower()
#
#     very_similar_category = categorizer.get_very_similar_category(best_new_category_name)
#     if very_similar_category:
#         very_similar_category_name = very_similar_category['category_name']
#
#         if very_similar_category_name != best_new_category_name:
#             answer = input('Did you mean: %s? (Y/N): ' % very_similar_category_name).lower()
#             if answer == 'y':
#                 return very_similar_category['category_id']
#         else:
#             return very_similar_category['category_id']
#
#     categorizer.insert_category(best_new_category_name)
#
#     logging.info('Inserted new category: ' + best_new_category_name)
#
#     return sqlite_client.get_category_id(best_new_category_name)


# MAIN FILE BEGINS HERE #
input_files = get_input_files()

try:
    for file in input_files:
        input_file = InputFile(db_client)
        input_file.insert_file('./input/' + file)

    uncategorized_txs = db_client.get_uncategorized_transactions()

    categorizer = Categorizer(db_client)

    for tx_id, denomination, memo in uncategorized_txs:
        denomination = denomination * -1
        print('$%.2f was spent with the following memo: [%s]' % (denomination, memo))

        # Try to look up the cleaned memo in order to find a match in the table already.
        memo = categorizer.clean_string(memo)
        logging.debug('cleaned memo: ' + memo)

        category_id = determine_category_id(memo, categorizer, db_client)

        db_client.set_category_id_for_tx(tx_id, category_id)

        categorizer.make_note_of_memo_and_category(memo, category_id)

except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.error(exc_type, fname, exc_tb.tb_lineno)