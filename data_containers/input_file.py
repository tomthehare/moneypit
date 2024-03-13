from database.sqlite_client import SqliteClient
from parsers.parsers import Parser, CapitalOneParser, BarclaysParser, ChaseParser
from utility.time_observer import TimeObserver

import coloredlogs, logging
coloredlogs.install(level='DEBUG')

class InputFile:

    def __init__(self, db_client: SqliteClient):
        self.db_client = db_client

    def get_parser(self, file_path) -> Parser:
        filename = file_path.split('/')[-1].lower()

        if 'capital_one' in filename \
                or 'capitalone' in filename \
                or 'capital-one' in filename \
                or '360performancesavings' in filename \
                or 'l-tsharedchecking' in filename \
                or '360checking' in filename:
            return CapitalOneParser(self.db_client)
        elif 'barclays' in filename or 'creditcard' in filename:
            return BarclaysParser(self.db_client)
        elif 'chase' in filename:
            return ChaseParser(self.db_client)

        raise Exception('No idea how to parse it: ' + filename)

    def insert_file(self, file_path):
        parser = self.get_parser(file_path)

        self.db_client.insert_input_file(
            parser.source_id,
            TimeObserver.get_timestamp_from_date_string(TimeObserver.get_now_date_string()),
            TimeObserver.get_now_date_string(),
            file_path
        )
        (file_path_id, processed_success_date) = self.db_client.get_input_file_id(parser.source_id, file_path)

        if processed_success_date:
            logging.debug('Skipping file since already processed: ' + file_path)
            return

        if file_path_id:
            parser.parse(file_path, file_path_id)

        return file_path_id
