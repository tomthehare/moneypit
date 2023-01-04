import sqlite3

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
            print('Created tblSourceBank')

	    source_bank_seed = [
              'Chase',
              'CapitalOne',
              'Barclays'
            ]

            for bank in source_bank_seed:
              connection.execute_sql('INSERT INTO tblSourceBank (Name) VALUES (\'\s\');' % bank)
              print('Added ' + bank + ' to database tblSourceBank')

	if "tblCategory" not in tables:
            table_sql = """
             CREATE TABLE tblCategory (
                CategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT,
                UNIQUE(Name)
	    );
            """
            connection.execute_sql(table_sql)
            print('Created tblCategory')

        if "tblInputFile" not in tables:
            table_sql = """
             CREATE TABLE tblInputFile (
                InputFileID INTEGER PRIMARY KEY AUTOINCREMENT,
                SourceBankID INTEGER,
		DateCreatedTimestamp INTEGER,
		DateCreatedHuman TEXT,
		DateProcessedSuccessfullyTimestamp INTEGER,
                FOREIGN KEY(SourceBankID) REFERNECES tblSourceBank(SourceBankID)
            );
            """
            connection.execute_sql(table_sql)
            print('Created tblInputFile')

	
        if "tblTransaction" not in tables:
            table_sql = """
             CREATE TABLE tblTransaction (
                TxID INTEGER PRIMARY KEY AUTOINCREMENT,
                TxDenomination REAL,
		TxDateHuman TEXT,
		TxDateTimestamp INTEGER,
		TxMemoRaw TEXT,
		TxCategoryID INTEGER NULL,
		InputFileID INTEGER,
		FOREIGN KEY(TxCategoryID) REFERENCES tblCateogry(CategoryID),
		FOREIGN KEY(InputFileID) REFERENCES tblInputFile(InputFileID)
            );
            """
            connection.execute_sql(table_sql)
            print('Created tblTransaction')


    def insert_monolith_measurement(
        self,
        git_group_id,
        date_unix_timestamp,
        human_date_stamp,
        file_count
    ):
        sql = f"""
        INSERT OR IGNORE INTO tblMonolithMeasurement (GroupID, DateUnixTimestamp, HumanDateStamp, FileCount)
        VALUES ({git_group_id}, {date_unix_timestamp}, '{human_date_stamp}', {file_count})
        """

        connection = ConnectionWrapper(self.database_name)

        try:
            connection.execute_sql(sql)
        finally:
            connection.wrap_it_up()


    def get_first_measurements(self, qualifier):
        sql = f"""
        SELECT GroupName, MIN(DateUnixTimestamp) AS DateUnixTimestamp, HumanDateStamp, FileCount
        FROM tblMonolithMeasurement 
        INNER JOIN tblGitGroup ON tblGitGroup.ID = tblMonolithMeasurement.GroupID
        WHERE {qualifier} = 1
        GROUP BY GroupID
        """
        connection = ConnectionWrapper(self.database_name)

        try:
            connection.execute_sql(sql)
            results = connection.get_results()
        finally:
            connection.wrap_it_up()

        return results

