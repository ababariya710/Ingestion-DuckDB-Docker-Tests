import json
import logging
import sys

from equalexperts_dataeng_exercise.db import DuckDBHandler


def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


def get_create_table_statement():
    return """
        CREATE SCHEMA IF NOT EXISTS blog_analysis;
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            UserId INTEGER,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        );
        """


class DuckDBDataIngester:
    def __init__(self, database_path='warehouse.db'):
        self.db_handler = DuckDBHandler(database_path)

    def insert_json_data(self, filename):
        try:
            setup_logging()
            self.db_handler.connect()
            self.db_handler.execute_statement(get_create_table_statement())
            self.db_handler.begin_transaction()

            batch_size = 1000
            batch_values = []
            with open(filename, 'r') as file:
                for line in file:
                    try:
                        data = json.loads(line)
                        values = (int(data.get('Id')), data.get('UserId'), int(
                            data.get('PostId')), int(data.get('VoteTypeId')), data.get('CreationDate'))
                        batch_values.append(values)
                        if len(batch_values) >= batch_size:
                            self.db_handler.insert_batch_values(batch_values)
                            batch_values = []
                    except Exception as e:
                        logging.error(f"Error processing line: {line}. Error: {e}")

            if batch_values:
                self.db_handler.insert_batch_values(batch_values)

            self.db_handler.commit_transaction()
            logging.info("Data insertion completed successfully")
        except Exception as e:
            self.db_handler.rollback_transaction()
            logging.error(f"Error: {e}")
        finally:
            self.db_handler.close_connection()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide the path to the JSON file.")
        sys.exit(1)

    filename = sys.argv[1]
    inserter = DuckDBDataIngester()
    inserter.insert_json_data(filename=filename)
