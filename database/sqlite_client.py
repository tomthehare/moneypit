import sqlite3

from utility.time_observer import TimeObserver


class ConnectionWrapper:
    _cursor = None
    _connection = None
    _last_rows = None

    def __init__(self, database_name):
        self._connection = sqlite3.connect(database_name)
        self._cursor = self._connection.cursor()

    def execute_sql(self, sql: str):
        if self._cursor is not None:
            self._cursor.execute(sql)

        if self._connection is not None:
            self._connection.commit()

    def get_results(self):
        return self._cursor.fetchall()

    def wrap_it_up(self):
        self._cursor.close()
        self._connection.close()


class SqliteClient:

    def __init__(self, database_name):
        self.database_name = database_name
        self.create_tables_if_not_exist()
        self.run_migrations()

    def create_tables_if_not_exist(self):
        sql = """
        SELECT name FROM sqlite_master WHERE type = \'table\'
        """

        connection = ConnectionWrapper(self.database_name)
        connection.execute_sql(sql)

        results = connection.get_results()

        tables = []
        for result in results:
            tables.append(result[0])

        if "tblSourceBank" not in tables:
            table_sql = """
                 CREATE TABLE tblSourceBank (
                    SourceBankID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT,
                    UNIQUE(Name)
            );
            """
            connection.execute_sql(table_sql)
            print("Created tblSourceBank")

        source_bank_seed = ["Chase", "CapitalOne", "Barclays", "AmericanExpress"]

        for bank in source_bank_seed:
            connection.execute_sql(
                "INSERT OR IGNORE INTO tblSourceBank (Name) VALUES ('%s');" % bank
            )
            print("Ensured that  " + bank + " was in tblSourceBank")

        if "tblCategory" not in tables:
            table_sql = """
             CREATE TABLE tblCategory (
                CategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                UNIQUE(Name)
            );
            """
            connection.execute_sql(table_sql)
            print("Created tblCategory")

        if "tblInputFile" not in tables:
            table_sql = """
             CREATE TABLE tblInputFile (
                InputFileID INTEGER PRIMARY KEY AUTOINCREMENT,
                SourceBankID INTEGER,
                DateCreatedTimestamp INTEGER,
                DateCreatedHuman TEXT,
                FileName TEXT,
                DateProcessedSuccessfullyTimestamp INTEGER NULL,
                FOREIGN KEY(SourceBankID) REFERENCES tblSourceBank(SourceBankID),
                UNIQUE(SourceBankID, FileName)
            );
            """
            connection.execute_sql(table_sql)
            print("Created tblInputFile")

        if "tblTransaction" not in tables:
            table_sql = """
             CREATE TABLE "tblTransaction" (
                "TxID"	INTEGER,
                "TxDenomination"	REAL,
                "TxDateHuman"	TEXT,
                "TxDateTimestamp"	INTEGER,
                "TxMemoRaw"	TEXT,
                "TxCategoryID"	INTEGER,
                "InputFileID"	INTEGER,
                FOREIGN KEY("InputFileID") REFERENCES "tblInputFile"("InputFileID"),
                FOREIGN KEY("TxCategoryID") REFERENCES "tblCategory"("CategoryID"),
                PRIMARY KEY("TxID" AUTOINCREMENT),
                UNIQUE(TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, InputFileID)
            );
            """
            connection.execute_sql(table_sql)
            print("Created tblTransaction")

        if "tblCategoryMatchString" not in tables:
            table_sql = """
             CREATE TABLE "tblCategoryMatchString" (
                "MatchID"	INTEGER,
                "CategoryID"	INT,
                "MatchString"  TEXT,
                FOREIGN KEY("CategoryID") REFERENCES "tblCategory"("CategoryID"),
                PRIMARY KEY("MatchID" AUTOINCREMENT),
                UNIQUE(CategoryID, MatchString)
            );
            """
            connection.execute_sql(table_sql)
            print("Created tblCategoryMatchString")

        connection.wrap_it_up()

    def run_migrations(self):

        if "DateDeleted" not in self.get_columns_for_table("tblTransaction"):
            sql = """
            ALTER TABLE tblTransaction
            ADD COLUMN DateDeleted TEXT;
            """
            connection = ConnectionWrapper(self.database_name)
            connection.execute_sql(sql)
            connection.wrap_it_up()
            print("Migrated tblTransaction.DateDeleted")

    def get_columns_for_table(self, table_name):
        sql = f"""
        PRAGMA table_info({table_name});
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()

            if not results:
                return None

            columns = []
            for row in results:
                columns.append(row[1])

            return columns
        finally:
            connection.wrap_it_up()

    def get_category_id(self, category_name):
        sql = f"""
        SELECT CategoryID
        FROM tblCategory 
        WHERE Name = '{category_name}'
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()

            if not results:
                return None

            return results[0][0]
        finally:
            connection.wrap_it_up()

    def insert_category(self, category_name):
        category_name = category_name.lower()

        sql = f"""
        INSERT OR IGNORE INTO tblCategory (Name) VALUES ('{category_name}');
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def insert_input_file(
        self, source_bank_id, date_created_timestamp, human_date_created, file_name
    ):
        sql = f"""
        INSERT OR IGNORE INTO tblInputFile (
            SourceBankID,
            DateCreatedTimestamp,
            DateCreatedHuman,
            FileName
        ) VALUES (
            {source_bank_id},
            {date_created_timestamp},
            '{human_date_created}',
            '{file_name}'
        )
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def get_input_file_id(self, source_bank_id, file_name):
        sql = f"""
        SELECT InputFileID, DateProcessedSuccessfullyTimestamp
        FROM tblInputFile 
        WHERE SourceBankID = {source_bank_id}
          AND FileName = '{file_name}'
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()

            if not results:
                return None

            return (results[0][0], results[0][1])
        finally:
            connection.wrap_it_up()

    def insert_transaction(
        self, denomination, date_human, date_timestamp, memo_raw, file_id
    ):
        memo_raw = memo_raw.replace("'", "")

        sql = f"""
        INSERT OR IGNORE INTO tblTransaction(
            TxDenomination,
            TxDateHuman,
            TxDateTimestamp,
            TxMemoRaw,
            InputFileID
        ) VALUES (
            {denomination},
            '{date_human}',
            {date_timestamp},
            '{memo_raw}',
            {file_id}
        );
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def set_processed_success_date(self, file_id):
        now_timestamp = TimeObserver.get_timestamp_from_date_string(
            TimeObserver.get_now_date_string()
        )

        sql = f"""
            UPDATE tblInputFile
            SET DateProcessedSuccessfullyTimestamp = {now_timestamp}
            WHERE InputFileID = {file_id}
            """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def get_uncategorized_transactions(self):
        sql = f"""
        SELECT TxID, TxDenomination, TxMemoRaw, TxDateHuman, sb.Name
        FROM tblTransaction tx
        INNER JOIN tblInputFile inputFile ON InputFile.InputFileID = tx.InputFileID
        INNER JOIN tblSourceBank sb ON inputFile.SourceBankID = sb.SourceBankID
        WHERE TxCategoryID IS NULL AND tx.DateDeleted IS NULL
        ORDER BY TxDateTimestamp ASC
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            return connection.get_results()
        finally:
            connection.wrap_it_up()

    def get_transaction(self, tx_id):
        sql = f"""
                SELECT TxID AS txID, TxDenomination as denomination, TxMemoRaw as memo, TxDateHuman as dateHuman, sb.Name as bankName
                FROM tblTransaction tx
                INNER JOIN tblInputFile inputFile ON InputFile.InputFileID = tx.InputFileID
                INNER JOIN tblSourceBank sb ON inputFile.SourceBankID = sb.SourceBankID
                WHERE TxID = {int(tx_id)}
                
                """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            if results:
                return {
                    "txID": results[0][0],
                    "denomination": results[0][1],
                    "memo": results[0][2],
                    "dateHuman": results[0][3],
                    "bankName": results[0][4],
                }
            else:
                return []
        finally:
            connection.wrap_it_up()

    def get_memos_to_categories(self):
        sql = f"""
        SELECT cms.CategoryID, cat.Name AS CategoryName, MatchString
        FROM tblCategoryMatchString cms
        INNER JOIN tblCategory cat ON cms.CategoryID = cat.CategoryID
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            return connection.get_results()
        finally:
            connection.wrap_it_up()

    def insert_memo_to_category(self, memo, category_id):
        sql = f"""
        INSERT OR IGNORE INTO tblCategoryMatchString (CategoryID, MatchString) VALUES ({int(category_id)}, '{memo}');
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def get_categories(self, filter=""):
        sql = f"""
        SELECT CategoryID, Name 
        FROM tblCategory
        """

        if filter:
            sql = sql + f"WHERE Name = '{filter}'"

        sql = sql + " ORDER BY Name ASC "

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            return connection.get_results()
        finally:
            connection.wrap_it_up()

    def get_category_by_id(self, id):
        sql = f"""
                SELECT Name 
                FROM tblCategory
                WHERE CategoryID = {id}
                """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            return connection.get_results()[0][0]
        finally:
            connection.wrap_it_up()

    def set_category_id_for_tx(self, tx_id, category_id):
        sql = f"""
        UPDATE tblTransaction
        SET TxCategoryID = {category_id}
        WHERE TxID = {tx_id}
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def get_data_for_time_slice(self, date_start, date_end, category_filter=""):
        sql = f"""
        SELECT cat.Name, TxDenomination, TxDateTimestamp, TxMemoRaw, TxID, sb.Name AS SourceBankName
        FROM tblTransaction tx
        INNER JOIN tblCategory cat ON tx.TxCategoryID = cat.CategoryID
        INNER JOIN tblInputFile inputFile ON inputFile.InputFileID = tx.InputFileID
        LEFT JOIN tblSourceBank sb ON sb.SourceBankID = inputFile.SourceBankID
        WHERE cat.Name NOT IN ('transfer', 'credit card payment')
          AND TxDateTimestamp >= {date_start}
          AND TxDateTimestamp < {date_end}
          AND DateDeleted IS NULL
        """

        if category_filter != "":
            sql = sql + f" AND cat.Name = '{category_filter}'"

        sql = sql + " ORDER BY TxDateTimestamp ASC"

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()

            return [
                {
                    "CategoryName": a[0],
                    "MoneySpent": a[1],
                    "Timestamp": a[2],
                    "Memo": a[3],
                    "ID": a[4],
                    "SourceBankName": a[5],
                }
                for a in results
            ]
        finally:
            connection.wrap_it_up()

    def update_category(self, tx_id, category_id):
        sql = f"""
        UPDATE tblTransaction 
        SET TxCategoryID = {category_id}
        WHERE TxID = {tx_id}
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()

    def get_latest_source_dates(self):
        sql = """
        SELECT sourceBank.Name, MAX(TxDateTimestamp) AS DateCreatedTimeStamp, TxDateHuman AS DateCreatedHuman
        FROM tblInputFile inputFile
        INNER JOIN tblSourceBank sourceBank ON inputFile.SourceBankID = sourceBank.SourceBankID
        INNER JOIN tblTransaction tx ON tx.InputFileID = inputFile.InputFileID
        WHERE tx.DateDeleted IS NULL
        GROUP BY sourceBank.Name
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
        finally:
            connection.wrap_it_up()

        return [{"source": a[0], "latest_date": a[2]} for a in results]

    def delete_transaction(self, tx_id):
        sql = f"""
                UPDATE tblTransaction 
                SET DateDeleted = '{TimeObserver.get_now_date_string()}'
                WHERE TxID = {tx_id};
                """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()
