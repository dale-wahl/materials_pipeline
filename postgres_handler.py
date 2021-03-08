import psycopg2
from psycopg2 import sql
from psycopg2.extensions import AsIs

"""
Be wary if editing queries. psycopg2 has guidelines to prevent SQL injection issues.
Do not use Python string concatenation (+) or string parameters interpolation (%) to pass variables to a SQL query string.
https://www.psycopg.org/docs/usage.html
"""

class SQL_Connector():
    def __init__(self, connect_statement):
        # connect to database and create cursor object
        self.conn = psycopg2.connect(connect_statement)
        self.cur = self.conn.cursor()

    def reconnect(self):
        """
        attempts to close connection and cursor and reestablish them
        """
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass
        self.conn = psycopg2.connect(connect_statement)
        self.cur = self.conn.cursor()

    def disconnect(self):
        """
        disconnects from database
        """
        try:
            self.cur.close()
            self.conn.close()
        except:
            pass

    def commit(self):
        """
        commits any changes to the database. nothing changes will be saved to the database unless this is called.
        """
        self.conn.commit()

    def check_table_exists(self, table_name):
        """
        check if table "table_name" exists in database from information_schema.tables.
        """
        self.cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s);", (table_name,))
        return self.cur.fetchone()[0]

    def check_record_exists(self, table_name, column_name, match_value):
        """
        check if a record with string "match_value" is in column "column_name" of table "table_name"
        """
        self.cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {table_name} WHERE {column_name} = %s);")
            .format(table_name=sql.Identifier(table_name),column_name=sql.Identifier(column_name)), (match_value,))
        return self.cur.fetchone()[0]

    def collect_table_column_names(self, table_name):
        self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s;", (table_name,))
        return [name[0] for name in self.cur.fetchall()]

    def collect_all_table_records(self, table_name):
        """
        returns list of column names and list of rows (each row as a tuple) for table table_name
        """
        # collect column names for table
        column_names = self.collect_table_column_names(table_name)
        # select all rows
        self.cur.execute(sql.SQL("SELECT * FROM {};")
               .format(sql.Identifier(table_name)))
        rows = self.cur.fetchall()
        return rows, column_names

    def create_table(self, table_name, columns_as_sql_str):
        """
        function checks if table_name exists in database, then if it does not creates the table using columns_as_sql_str.

        columns_as_sql_str should be formatted in sql (e.g.: "unique_id character varying(30) NOT NULL, mearsurement_name character varying(40), measurement real")
        """
        if self.check_table_exists(table_name):
            # table already exists
            print(f"Table {table_name} already exists")
            pass
        else:
            self.cur.execute(sql.SQL("CREATE TABLE {} (%s);").format(sql.Identifier(table_name)), (AsIs(columns_as_sql_str),))
            print(f"Table {table_name} created")

    def add_record(self, table_name, record_as_dict, record_column_decode_dict):
        """
        adds a record to table "table_name" based on dictionary record_as_dict of key values representing the record and a dictionary record_column_decode_dict that should translate the keys in the record_as_dict to the appropriate columns names used in the sql database.

        i did it this way so that it is not necessary to preemptively change the keys of each individual record
        """
        columns = [record_column_decode_dict[key] for key in record_as_dict.keys()]
        values = record_as_dict.values()

        self.cur.execute(sql.SQL("INSERT INTO {} (%s) values %s;").format(sql.Identifier(table_name)),
                            (AsIs(','.join(columns)), tuple(values)))
