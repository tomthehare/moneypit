import coloredlogs, logging
import datetime

from categories.categorizer import Categorizer
from database.sqlite_client import SqliteClient
from utility.time_observer import TimeObserver

CHASE = 1
CAPITAL_ONE = 2
BARCLAYS = 3

coloredlogs.install(level='DEBUG')

class Parser:

    def __init__(self, source_id, sqlite_client: SqliteClient):
        self.source_id = source_id
        self.sqlite_client = sqlite_client

    def parse(self, filepath, file_id):
        self.sqlite_client.set_processed_success_date(file_id)

    def parse_line(self, line):
        pass

    def load_file_contents(self, filepath):
        with (open(filepath, 'r') as f):
            return f.readlines()


class CapitalOneParser(Parser):

    def __init__(self, sqlite_client: SqliteClient):
        Parser.__init__(self, CAPITAL_ONE, sqlite_client)

    def parse(self, filepath, file_id):
        logging.info('CapitalOne processing: ' + filepath)
        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == '':
            return True

        line = line.lower()
        ignored_slugs = [
            'account number,transaction date,transaction amount'
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        # 5279,12/31/22,360.47,Credit,Monthly Interest Paid,136065.38
        (account_number, tx_date, amount, descriptor, description, balance) = line.split(',')

        format = "%m/%d/%y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime('%Y-%m-%d')

        return (tx_date, description, amount)


class BarclaysParser(Parser):

    def __init__(self, sql_client: SqliteClient):
        Parser.__init__(self, BARCLAYS, sql_client)

    def parse(self, filepath, file_id):
        logging.info('Barclays processing: ' + filepath)

        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == '':
            return True

        line = line.lower()
        ignored_slugs = [
            'barclays bank delaware',
            'account number:',
            'account balance as of',
            'transaction date,description,category,amount'
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        (tx_date, description, category, amount) = line.split(',')

        format = "%m/%d/%Y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime('%Y-%m-%d')

        return (tx_date, description, amount)


class ChaseParser(Parser):

    def __init__(self, sqlite_client: SqliteClient):
        Parser.__init__(self, CHASE, sqlite_client)

    def parse(self, filepath, file_id):
        logging.info('Chase processing: ' + filepath)

        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            logging.debug('Line: '  + line)
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == '':
            return True

        line = line.lower()
        ignored_slugs = [
            'transaction date,post date,description,category,type,amount,memo'
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        # 12/29/2022,12/30/2022,WHOLEFDS AVR 10371,Groceries,Sale,-99.28
        (tx_date, post_date, description, category, descriptor, amount, emptysomething) = line.split(',')

        format = "%m/%d/%Y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime('%Y-%m-%d')

        return (tx_date, description, amount)