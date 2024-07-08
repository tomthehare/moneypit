import coloredlogs, logging
import datetime
import re

from categories.categorizer import Categorizer
from database.sqlite_client import SqliteClient
from utility.time_observer import TimeObserver

CHASE = 1
CAPITAL_ONE = 2
BARCLAYS = 3
AMERICAN_EXPRESS = 4

coloredlogs.install(level="DEBUG")


class Parser:

    def __init__(self, source_id, sqlite_client: SqliteClient):
        self.source_id = source_id
        self.sqlite_client = sqlite_client

    def parse(self, filepath, file_id):
        self.sqlite_client.set_processed_success_date(file_id)

    def parse_line(self, line):
        pass

    def load_file_contents(self, filepath):
        with open(filepath, "r") as f:
            return f.readlines()

    def replace_commas_between_quotes(self, text):
        # I was having a really hard time doing this with regex, so doing it the old fashioned way
        new_string = ""
        opening_index = -1
        for index in range(0, len(text)):
            new_character = text[index]
            if text[index] == '"':
                if opening_index <= 0:
                    opening_index = index
                else:
                    opening_index = -1

            if text[index] == "," and opening_index >= 0:
                new_character = ""

            new_string = new_string + new_character

        return new_string


class CapitalOneParser(Parser):

    def __init__(self, sqlite_client: SqliteClient):
        Parser.__init__(self, CAPITAL_ONE, sqlite_client)

    def parse(self, filepath, file_id):
        logging.info("CapitalOne processing: " + filepath)
        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            logging.debug("Loaded line: " + line)
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id,
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == "":
            return True

        line = line.lower()
        ignored_slugs = [
            "account number,transaction date,transaction amount",
            "account number,transaction description,transaction date,transaction type,transaction amount,balance",
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        line = self.replace_commas_between_quotes(line)
        logging.debug("New line: %s" % line)

        # Account Number,Transaction Description,Transaction Date,Transaction Type,Transaction Amount,Balance
        # 5279,12/31/22,360.47,Credit,Monthly Interest Paid,136065.38
        # (account_number, tx_date, amount, descriptor, description, balance) = (
        (account_number, description, tx_date, tx_type, amount, balance) = line.split(
            ","
        )

        # CapitalOne uses tx types of debit and credit.  Let's use this to set the sign on the amount.
        if tx_type.lower() == "debit":
            amount = float(amount) * -1

        format = "%m/%d/%y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime("%Y-%m-%d")

        return (tx_date, description, amount)


class BarclaysParser(Parser):

    def __init__(self, sql_client: SqliteClient):
        Parser.__init__(self, BARCLAYS, sql_client)

    def parse(self, filepath, file_id):
        logging.info("Barclays processing: " + filepath)

        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            logging.debug("Loaded line: " + line)
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id,
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == "":
            return True

        line = line.lower()
        ignored_slugs = [
            "barclays bank delaware",
            "account number:",
            "account balance as of",
            "transaction date,description,category,amount",
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        line = self.replace_commas_between_quotes(line)

        (tx_date, description, category, amount) = line.split(",")

        format = "%m/%d/%Y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime("%Y-%m-%d")

        return (tx_date, description, amount)


class ChaseParser(Parser):

    def __init__(self, sqlite_client: SqliteClient):
        Parser.__init__(self, CHASE, sqlite_client)

    def parse(self, filepath, file_id):
        logging.info("Chase processing: " + filepath)

        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            logging.debug("Loaded line: " + line)
            if self.is_ignored_line(line):
                continue

            (tx_date, description, amount) = self.parse_line(line)

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id,
            )

        Parser.parse(self, filepath, file_id)

    def is_ignored_line(self, line):
        if line == "":
            return True

        line = line.lower()
        ignored_slugs = [
            "transaction date,post date,description,category,type,amount,memo"
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse_line(self, line):
        line = self.replace_commas_between_quotes(line)

        # 12/29/2022,12/30/2022,WHOLEFDS AVR 10371,Groceries,Sale,-99.28
        (
            tx_date,
            post_date,
            description,
            category,
            descriptor,
            amount,
            emptysomething,
        ) = line.split(",")

        format = "%m/%d/%Y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime("%Y-%m-%d")

        return (tx_date, description, amount)


class AmericanExpressParser(Parser):

    def __init__(self, sqlite_client: SqliteClient):
        Parser.__init__(self, AMERICAN_EXPRESS, sqlite_client)

    def parse_line(self, line):
        line = self.replace_commas_between_quotes(line)

        # Date,Description,Card Member,Account #,Amount
        split_parts = line.split(",")

        if len(split_parts) == 5:
            (tx_date, description, card_member, account_id, amount) = split_parts
        else:
            (tx_date, description, amount) = split_parts

        format = "%m/%d/%Y"
        tx_date = datetime.datetime.strptime(tx_date, format)
        tx_date = tx_date.strftime("%Y-%m-%d")

        return (tx_date, description, amount)

    def is_ignored_line(self, line):
        if line == "":
            return True

        line = line.lower()
        ignored_slugs = [
            "date,description,amount",
            "date,description,card member,account #,amount",
        ]

        for ignored in ignored_slugs:
            if ignored in line:
                return True

        return False

    def parse(self, filepath, file_id):
        logging.info("American express processing: " + filepath)

        contents = self.load_file_contents(filepath)

        for line in contents:
            line = line.strip()
            logging.debug("Loaded line: " + line)
            if self.is_ignored_line(line):
                continue

            # Date,Description,Card Member,Account #,Amount
            (tx_date, description, amount) = self.parse_line(line)

            # AMEX does balances differently from other providers - positive amounts are actually charges
            amount = float(amount) * -1

            self.sqlite_client.insert_transaction(
                amount,
                tx_date,
                TimeObserver.get_timestamp_from_date_string(tx_date),
                description,
                file_id,
            )

        Parser.parse(self, filepath, file_id)
