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
                "TxCustomMemo"	TEXT,
                "TxCategoryID"	INTEGER,
                "InputFileID"	INTEGER,
                "SourceBankID"	INTEGER,
                "DateDeleted"	TEXT,
                FOREIGN KEY("InputFileID") REFERENCES "tblInputFile"("InputFileID"),
                FOREIGN KEY("TxCategoryID") REFERENCES "tblCategory"("CategoryID"),
                FOREIGN KEY("SourceBankID") REFERENCES "tblSourceBank"("SourceBankID"),
                PRIMARY KEY("TxID" AUTOINCREMENT),
                UNIQUE(TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, SourceBankID)
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

        # Add SourceBankID and enforce unique (denom, date, memo) per source across files
        cols = self.get_columns_for_table("tblTransaction")
        if cols and "SourceBankID" not in cols:
            connection = ConnectionWrapper(self.database_name)
            try:
                connection.execute_sql(
                    "ALTER TABLE tblTransaction ADD COLUMN SourceBankID INTEGER;"
                )
                connection.execute_sql("""
                    UPDATE tblTransaction SET SourceBankID = (
                        SELECT SourceBankID FROM tblInputFile
                        WHERE tblInputFile.InputFileID = tblTransaction.InputFileID
                    );
                """)
            finally:
                connection.wrap_it_up()
            print("Migrated tblTransaction.SourceBankID added and backfilled")

        # Recreate tblTransaction with UNIQUE(..., SourceBankID) instead of UNIQUE(..., InputFileID)
        create_sql = self._get_table_creation_sql("tblTransaction")
        if create_sql and "TxMemoRaw, InputFileID)" in create_sql:
            self._migrate_tbltransaction_unique_per_source()

        # Add TxCustomMemo for user notes that override display
        cols = self.get_columns_for_table("tblTransaction")
        if cols:
            if "CustomMemo" in cols and "TxCustomMemo" not in cols:
                connection = ConnectionWrapper(self.database_name)
                try:
                    connection.execute_sql(
                        "ALTER TABLE tblTransaction RENAME COLUMN CustomMemo TO TxCustomMemo;"
                    )
                finally:
                    connection.wrap_it_up()
                print("Migrated tblTransaction.CustomMemo -> TxCustomMemo")
            elif "TxCustomMemo" not in cols:
                connection = ConnectionWrapper(self.database_name)
                try:
                    connection.execute_sql(
                        "ALTER TABLE tblTransaction ADD COLUMN TxCustomMemo TEXT;"
                    )
                finally:
                    connection.wrap_it_up()
                print("Migrated tblTransaction.TxCustomMemo")

    def _get_table_creation_sql(self, table_name):
        connection = ConnectionWrapper(self.database_name)
        try:
            connection._cursor.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            row = connection._cursor.fetchone()
            return row[0] if row else None
        finally:
            connection.wrap_it_up()

    def _migrate_tbltransaction_unique_per_source(self):
        """Recreate tblTransaction with UNIQUE(..., SourceBankID) so one row per (denom, date, memo, source)."""
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql("""
                CREATE TABLE tblTransaction_new (
                    TxID INTEGER PRIMARY KEY AUTOINCREMENT,
                    TxDenomination REAL,
                    TxDateHuman TEXT,
                    TxDateTimestamp INTEGER,
                    TxMemoRaw TEXT,
                    TxCustomMemo TEXT,
                    TxCategoryID INTEGER,
                    InputFileID INTEGER,
                    SourceBankID INTEGER,
                    DateDeleted TEXT,
                    FOREIGN KEY(InputFileID) REFERENCES tblInputFile(InputFileID),
                    FOREIGN KEY(TxCategoryID) REFERENCES tblCategory(CategoryID),
                    FOREIGN KEY(SourceBankID) REFERENCES tblSourceBank(SourceBankID),
                    UNIQUE(TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, SourceBankID)
                );
            """)
            # Copy one row per (denom, date, memo, source), keeping smallest TxID; and all rows with NULL source
            connection.execute_sql("""
                INSERT INTO tblTransaction_new
                  (TxID, TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, TxCustomMemo, TxCategoryID, InputFileID, SourceBankID, DateDeleted)
                SELECT t.TxID, t.TxDenomination, t.TxDateHuman, t.TxDateTimestamp, t.TxMemoRaw, NULL, t.TxCategoryID, t.InputFileID, t.SourceBankID, t.DateDeleted
                FROM tblTransaction t
                INNER JOIN (
                    SELECT TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, SourceBankID, MIN(TxID) AS KeptTxID
                    FROM tblTransaction
                    WHERE SourceBankID IS NOT NULL
                    GROUP BY TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, SourceBankID
                ) u ON t.TxID = u.KeptTxID
            """)
            connection.execute_sql("""
                INSERT INTO tblTransaction_new
                  (TxID, TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, TxCustomMemo, TxCategoryID, InputFileID, SourceBankID, DateDeleted)
                SELECT t.TxID, t.TxDenomination, t.TxDateHuman, t.TxDateTimestamp, t.TxMemoRaw, NULL, t.TxCategoryID, t.InputFileID, t.SourceBankID, t.DateDeleted
                FROM tblTransaction t
                INNER JOIN (
                    SELECT TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw, MIN(TxID) AS KeptTxID
                    FROM tblTransaction WHERE SourceBankID IS NULL
                    GROUP BY TxDenomination, TxDateHuman, TxDateTimestamp, TxMemoRaw
                ) u ON t.TxID = u.KeptTxID
            """)
            connection.execute_sql("DROP TABLE tblTransaction;")
            connection.execute_sql("ALTER TABLE tblTransaction_new RENAME TO tblTransaction;")
        finally:
            connection.wrap_it_up()
        print("Migrated tblTransaction: unique per (denom, date, memo, source) across files")

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

    def get_source_bank_id_for_file(self, file_id):
        """Return SourceBankID for the given InputFileID, or None."""
        sql = f"""
        SELECT SourceBankID FROM tblInputFile WHERE InputFileID = {int(file_id)}
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            return results[0][0] if results else None
        finally:
            connection.wrap_it_up()

    def transaction_exists_for_source(
        self, denomination, date_human, date_timestamp, memo_raw, source_bank_id
    ):
        """True if a non-deleted transaction with this (denom, date, memo) already exists for this source (any file)."""
        safe_memo = memo_raw.replace("'", "''")
        safe_date = date_human.replace("'", "''")
        sql = f"""
        SELECT 1 FROM tblTransaction tx
        INNER JOIN tblInputFile f ON tx.InputFileID = f.InputFileID
        WHERE f.SourceBankID = {int(source_bank_id)}
          AND tx.TxDenomination = {denomination}
          AND tx.TxDateHuman = '{safe_date}'
          AND tx.TxDateTimestamp = {int(date_timestamp)}
          AND tx.TxMemoRaw = '{safe_memo}'
          AND tx.DateDeleted IS NULL
        LIMIT 1
        """
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            return len(connection.get_results()) > 0
        finally:
            connection.wrap_it_up()

    def insert_transaction(
        self, denomination, date_human, date_timestamp, memo_raw, file_id
    ):
        memo_raw = memo_raw.replace("'", "")

        source_bank_id = self.get_source_bank_id_for_file(file_id)
        if source_bank_id is None:
            return
        if self.transaction_exists_for_source(
            denomination, date_human, date_timestamp, memo_raw, source_bank_id
        ):
            return

        date_safe = date_human.replace("'", "''")
        sql = f"""
        INSERT OR IGNORE INTO tblTransaction(
            TxDenomination,
            TxDateHuman,
            TxDateTimestamp,
            TxMemoRaw,
            TxCategoryID,
            InputFileID,
            SourceBankID
        ) VALUES (
            {denomination},
            '{date_safe}',
            {date_timestamp},
            '{memo_raw.replace("'", "''")}',
            NULL,
            {file_id},
            {source_bank_id}
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
        sql = """
        SELECT TxID, TxDenomination, TxMemoRaw, TxCustomMemo, TxDateHuman, sb.Name
        FROM tblTransaction tx
        INNER JOIN tblInputFile inputFile ON inputFile.InputFileID = tx.InputFileID
        INNER JOIN tblSourceBank sb ON sb.SourceBankID = inputFile.SourceBankID
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
        SELECT TxID, TxDenomination, TxMemoRaw, TxCustomMemo, TxDateHuman, sb.Name
        FROM tblTransaction tx
        INNER JOIN tblInputFile inputFile ON inputFile.InputFileID = tx.InputFileID
        INNER JOIN tblSourceBank sb ON sb.SourceBankID = inputFile.SourceBankID
        WHERE TxID = {int(tx_id)}
        """

        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(sql)
            results = connection.get_results()
            if results:
                r = results[0]
                memo_raw = r[2]
                custom_memo = r[3]
                display_memo = custom_memo if custom_memo else memo_raw
                return {
                    "txID": r[0],
                    "denomination": r[1],
                    "memo": display_memo,
                    "memo_raw": memo_raw,
                    "custom_memo": custom_memo,
                    "dateHuman": r[4],
                    "bankName": r[5],
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
            tx.TxCustomMemo,
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
            out = []
            for r in results:
                memo_raw = r[3]
                custom_memo = r[4]
                display_memo = custom_memo if custom_memo else memo_raw
                out.append({
                    "ID": r[0],
                    "MoneySpent": r[1],
                    "Date": r[2],
                    "Memo": display_memo,
                    "MemoRaw": memo_raw,
                    "CategoryName": r[5],
                    "SourceBankName": r[6],
                })
            return out
        finally:
            connection.wrap_it_up()

    def get_data_for_time_slice(self, date_start, date_end, category_filter=""):
        sql = f"""
        SELECT cat.Name, TxDenomination, TxDateTimestamp, TxMemoRaw, TxCustomMemo, TxID, sb.Name AS SourceBankName
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
            out = []
            for a in results:
                memo_raw = a[3]
                custom_memo = a[4]
                display_memo = custom_memo if custom_memo else memo_raw
                out.append({
                    "CategoryName": a[0],
                    "MoneySpent": a[1],
                    "Timestamp": a[2],
                    "Memo": display_memo,
                    "MemoRaw": memo_raw,
                    "ID": a[5],
                    "SourceBankName": a[6],
                })
            return out
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

    def update_custom_memo(self, tx_id, custom_memo):
        safe = (custom_memo or "").strip().replace("'", "''")
        sql = f"""
        UPDATE tblTransaction
        SET TxCustomMemo = {'NULL' if not safe else f"'{safe}'"}
        WHERE TxID = {int(tx_id)}
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
            COUNT(CASE WHEN tx.DateDeleted IS NULL THEN tx.TxID END) AS TxTotal,
            SUM(CASE WHEN tx.TxCategoryID IS NULL AND tx.DateDeleted IS NULL THEN 1 ELSE 0 END) AS TxUncategorized,
            MIN(CASE WHEN tx.DateDeleted IS NULL THEN tx.TxDateTimestamp END) AS DateMinTs,
            MAX(CASE WHEN tx.DateDeleted IS NULL THEN tx.TxDateTimestamp END) AS DateMaxTs
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
                    "date_min_ts": r[6],
                    "date_max_ts": r[7],
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
            tx.TxCustomMemo,
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
            out = []
            for r in results:
                memo_raw = r[3]
                custom_memo = r[4] if len(r) > 4 else None
                display_memo = custom_memo if custom_memo else memo_raw
                out.append({
                    "tx_id": r[0],
                    "denomination": r[1],
                    "date": r[2],
                    "memo": display_memo,
                    "memo_raw": memo_raw,
                    "custom_memo": custom_memo,
                    "category_id": r[5],
                    "category_name": (r[6] or ""),
                    "deleted": r[7] is not None,
                })
            return out
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

    def delete_file_and_transactions(self, file_id):
        """Permanently delete all transactions for this file and the input file record."""
        connection = ConnectionWrapper(self.database_name)
        try:
            connection.execute_sql(
                f"DELETE FROM tblTransaction WHERE InputFileID = {int(file_id)};"
            )
            connection.execute_sql(
                f"DELETE FROM tblInputFile WHERE InputFileID = {int(file_id)};"
            )
        finally:
            connection.wrap_it_up()
