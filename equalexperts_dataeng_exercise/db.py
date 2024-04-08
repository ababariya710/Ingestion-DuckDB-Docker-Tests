import logging

import duckdb


class DuckDBHandler:
    def __init__(self, database_path='warehouse.db'):
        self.database_path = database_path
        self.con = None

    def connect(self):
        self.con = duckdb.connect(database=self.database_path, read_only=False)
        logging.info("Connected to DuckDB database")

    def execute(self, command):
        return self.con.execute(command)

    def execute_statement(self, statement):
        self.con.execute(statement)
        logging.info("Statement executed successfully")

    def insert_batch_values(self, batch_values):
        self.con.executemany(
            "INSERT OR IGNORE INTO blog_analysis.votes VALUES (?, ?, ?, ?, ?)", batch_values)
        logging.info("Batch values inserted successfully")

    def insert_multiple(self, batch_values):
        self.con.executemany(
            "INSERT OR IGNORE INTO blog_analysis.votes VALUES (?, ?, ?, ?)", batch_values)
        logging.info("Inserted successfully")

    def begin_transaction(self):
        self.con.begin()

    def commit_transaction(self):
        self.con.commit()

    def rollback_transaction(self):
        self.con.rollback()

    def close_connection(self):
        if self.con:
            self.con.close()
            logging.info("Connection closed")
