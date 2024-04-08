import logging

from equalexperts_dataeng_exercise.db import DuckDBHandler


class DuckDBViewCreator:
    def __init__(self, database_path='warehouse.db'):
        self.db_handler = DuckDBHandler(database_path)

    def create_view(self):
        try:
            sql_query = """
                CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
                SELECT
                    CAST(strftime('%Y', CreationDate) AS INTEGER) AS Year,
                    CAST(strftime('%W', CreationDate) AS INTEGER) AS WeekNumber,
                    COUNT(*) AS VoteCount
                FROM
                    blog_analysis.votes
                GROUP BY
                    Year, WeekNumber
                HAVING
                    ABS(1 - COUNT(*) / (SELECT AVG(cnt) FROM (SELECT COUNT(*) AS cnt FROM blog_analysis.votes GROUP BY CAST(strftime('%Y', CreationDate) AS INTEGER), CAST(strftime('%W', CreationDate) AS INTEGER))))
                    > 0.2
                ORDER BY
                    Year, WeekNumber;
            """
            self.db_handler.connect()
            self.db_handler.execute_statement(sql_query)
            logging.info("View created successfully")
        except Exception as e:
            logging.error(f"Error creating view: {e}")

    def close_connection(self):
        try:
            self.db_handler.close_connection()
        except Exception as e:
            logging.error(f"Error closing connection: {e}")


def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    setup_logging()
    creator = DuckDBViewCreator()
    creator.create_view()
    creator.close_connection()
