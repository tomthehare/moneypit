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

    def search_transactions(self, memo_query="", category_filter="", limit=200):
        """Return transactions with optional memo substring filter and category filter.
        Results are returned ordered by date desc; fuzzy ranking is done in the caller."""
        conditions = ["tx.DateDeleted IS NULL"]
        if memo_query:
            safe = memo_query.replace("'", "''")
            conditions.append(f"LOWER(tx.TxMemoRaw) LIKE LOWER('%{safe}%')")
        if category_filter:
            safe_cat = category_filter.replace("'", "''")
            conditions.append(f"LOWER(cat.Name) = LOWER('{safe_cat}')")

        where = " AND ".join(conditions)

        sql = f"""
        SELECT
            tx.TxID,
            tx.TxDenomination,
            tx.TxDateHuman,
            tx.TxMemoRaw,
            COALESCE(cat.Name, '') AS CategoryName,
            COALESCE(sb.Name, '') AS SourceBankName
        FROM tblTransaction tx
        LEFT JOIN tblCategory cat ON cat.CategoryID = tx.TxCategoryID
        LEFT JOIN tblInputFile inputFile ON inputFile.InputFileID = tx.InputFileID
        LEFT JOIN tblSourceBank sb ON sb.SourceBankID = inputFile.SourceBankID
        WHERE {where}
        ORDER BY tx.TxDateTimestamp DESC
        LIMIT {int(limit)}
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            return [
                {
                    "ID": r[0],
                    "MoneySpent": r[1],
                    "Date": r[2],
                    "Memo": r[3],
                    "CategoryName": r[4],
                    "SourceBankName": r[5],
                }
                for r in results
            ]
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

    def get_all_input_files(self):
        sql = """
        SELECT
            f.InputFileID,
            f.FileName,
            f.DateCreatedHuman,
            sb.Name AS SourceBank,
            COUNT(tx.TxID) AS TxTotal,
            SUM(CASE WHEN tx.TxCategoryID IS NULL AND tx.DateDeleted IS NULL THEN 1 ELSE 0 END) AS TxUncategorized
        FROM tblInputFile f
        INNER JOIN tblSourceBank sb ON sb.SourceBankID = f.SourceBankID
        LEFT JOIN tblTransaction tx ON tx.InputFileID = f.InputFileID
        GROUP BY f.InputFileID, f.FileName, f.DateCreatedHuman, sb.Name
        ORDER BY f.DateCreatedTimestamp DESC
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            return [
                {
                    "file_id": r[0],
                    "file_name": r[1],
                    "date_created": r[2],
                    "source_bank": r[3],
                    "tx_total": r[4],
                    "tx_uncategorized": r[5],
                }
                for r in results
            ]
        finally:
            connection.wrap_it_up()

    def get_transactions_for_file(self, file_id):
        sql = f"""
        SELECT
            tx.TxID,
            tx.TxDenomination,
            tx.TxDateHuman,
            tx.TxMemoRaw,
            tx.TxCategoryID,
            cat.Name AS CategoryName,
            tx.DateDeleted
        FROM tblTransaction tx
        LEFT JOIN tblCategory cat ON cat.CategoryID = tx.TxCategoryID
        WHERE tx.InputFileID = {int(file_id)}
        ORDER BY tx.TxDateTimestamp ASC, tx.TxID ASC
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            return [
                {
                    "tx_id": r[0],
                    "denomination": r[1],
                    "date": r[2],
                    "memo": r[3],
                    "category_id": r[4],
                    "category_name": r[5] or "",
                    "deleted": r[6] is not None,
                }
                for r in results
            ]
        finally:
            connection.wrap_it_up()

    def add_match_rule_and_apply(self, match_string, category_id):
        """Insert a new match rule and backfill all transactions whose memo matches."""
        safe_memo = match_string.strip().replace("'", "''")

        insert_sql = f"""
        INSERT OR IGNORE INTO tblCategoryMatchString (CategoryID, MatchString)
        VALUES ({int(category_id)}, '{safe_memo}');
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(insert_sql)
        finally:
            connection.wrap_it_up()

        backfill_sql = f"""
        UPDATE tblTransaction
        SET TxCategoryID = {int(category_id)}
        WHERE LOWER(TxMemoRaw) = LOWER('{safe_memo}')
          AND DateDeleted IS NULL
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(backfill_sql)
            connection._cursor.execute("SELECT changes()")
            rows_affected = connection._cursor.fetchone()[0]
            return rows_affected
        finally:
            connection.wrap_it_up()

    def get_match_strings_with_tx_counts(self):
        """Return all category match strings with how many transactions currently carry that memo."""
        sql = """
        SELECT
            cms.MatchID,
            cms.MatchString,
            cms.CategoryID,
            cat.Name AS CategoryName,
            COUNT(tx.TxID) AS TxCount
        FROM tblCategoryMatchString cms
        INNER JOIN tblCategory cat ON cms.CategoryID = cat.CategoryID
        LEFT JOIN tblTransaction tx
            ON LOWER(tx.TxMemoRaw) = LOWER(cms.MatchString)
           AND tx.DateDeleted IS NULL
        GROUP BY cms.MatchID, cms.MatchString, cms.CategoryID, cat.Name
        ORDER BY cat.Name ASC, cms.MatchString ASC
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            return [
                {
                    "match_id": r[0],
                    "match_string": r[1],
                    "category_id": r[2],
                    "category_name": r[3],
                    "tx_count": r[4],
                }
                for r in results
            ]
        finally:
            connection.wrap_it_up()

    def reassign_match_string(self, match_id, new_category_id):
        """Move a match string to a new category and backfill all matching transactions."""
        get_sql = f"""
        SELECT MatchString FROM tblCategoryMatchString WHERE MatchID = {int(match_id)}
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(get_sql)
            results = connection.get_results()
            if not results:
                return 0
            match_string = results[0][0]
        finally:
            connection.wrap_it_up()

        update_cms_sql = f"""
        UPDATE tblCategoryMatchString
        SET CategoryID = {int(new_category_id)}
        WHERE MatchID = {int(match_id)}
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(update_cms_sql)
        finally:
            connection.wrap_it_up()

        safe_memo = match_string.replace("'", "''")
        update_tx_sql = f"""
        UPDATE tblTransaction
        SET TxCategoryID = {int(new_category_id)}
        WHERE LOWER(TxMemoRaw) = LOWER('{safe_memo}')
          AND DateDeleted IS NULL
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(update_tx_sql)
            connection._cursor.execute("SELECT changes()")
            rows_affected = connection._cursor.fetchone()[0]
            return rows_affected
        finally:
            connection.wrap_it_up()

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
