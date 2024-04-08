import json
import tempfile
import pytest
from equalexperts_dataeng_exercise.db import DuckDBHandler
from equalexperts_dataeng_exercise.outliers import DuckDBViewCreator

@pytest.fixture(scope="module")
def get_data_base_path():
    temp_db_file = tempfile.NamedTemporaryFile(suffix='.db')
    temp_db_file.close()
    return temp_db_file.name

def test_view_with_test_data(get_data_base_path):
    creator = DuckDBViewCreator(get_data_base_path)

    test_data = [
    {"Id": "1", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-02T00:00:00.000"},
    {"Id": "2", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
    {"Id": "4", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
    {"Id": "5", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-09T00:00:00.000"},
    {"Id": "6", "PostId": "5", "VoteTypeId": "3", "CreationDate": "2022-01-16T00:00:00.000"},
    {"Id": "7", "PostId": "3", "VoteTypeId": "2", "CreationDate": "2022-01-16T00:00:00.000"},
    {"Id": "8", "PostId": "4", "VoteTypeId": "2", "CreationDate": "2022-01-16T00:00:00.000"},
    {"Id": "9", "PostId": "2", "VoteTypeId": "2", "CreationDate": "2022-01-23T00:00:00.000"},
    {"Id": "10", "PostId": "2", "VoteTypeId": "2", "CreationDate": "2022-01-23T00:00:00.000"},
    {"Id": "11", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-30T00:00:00.000"},
    {"Id": "12", "PostId": "5", "VoteTypeId": "2", "CreationDate": "2022-01-30T00:00:00.000"},
    {"Id": "13", "PostId": "8", "VoteTypeId": "2", "CreationDate": "2022-02-06T00:00:00.000"},
    {"Id": "14", "PostId": "13", "VoteTypeId": "3", "CreationDate": "2022-02-13T00:00:00.000"},
    {"Id": "15", "PostId": "13", "VoteTypeId": "3", "CreationDate": "2022-02-20T00:00:00.000"},
    {"Id": "16", "PostId": "11", "VoteTypeId": "2", "CreationDate": "2022-02-20T00:00:00.000"},
    {"Id": "17", "PostId": "3", "VoteTypeId": "3", "CreationDate": "2022-02-27T00:00:00.000"}
]

    creator.db_handler.connect()
    creator.db_handler.execute_statement("""
        CREATE SCHEMA IF NOT EXISTS blog_analysis;
        CREATE TABLE IF NOT EXISTS blog_analysis.votes (
            Id INTEGER PRIMARY KEY,
            PostId INTEGER,
            VoteTypeId INTEGER,
            CreationDate TIMESTAMP
        );
        """)

    batch_values = []
    for record in test_data:
        values = (int(record.get('Id')), int(record.get('PostId')), int(record.get('VoteTypeId')), record.get('CreationDate'))
        batch_values.append(values)

    creator.db_handler.insert_multiple(batch_values)
    creator.create_view()
    result = creator.db_handler.execute("SELECT * FROM blog_analysis.outlier_weeks").fetchall()
    creator.close_connection()
    expected_output = [
        (2022, 0, 1),
        (2022, 1, 3),
        (2022, 2, 3),
        (2022, 5, 1),
        (2022, 6, 1),
        (2022, 8, 1)
    ]
    assert result == expected_output


def test_error_handling_view_creation(get_data_base_path):
    creator = DuckDBViewCreator(get_data_base_path)
    invalid_sql_query = """
        CREATE OR REPLACE VIEW blog_analysis.outlier_weeks AS
        SELECT 
            CAST(strftime('%Y', CreationDate) AS INTEGER) AS Year,
            CAST(strftime('%W', CreationDate) AS INTEGER) AS WeekNumber,
            COUNT(*) AS VoteCount
        FROM 
            non_existing_table
        GROUP BY 
            Year, WeekNumber
    """
    with pytest.raises(Exception):
        creator.create_view(invalid_sql_query)

def test_error_handling_connection_closure(get_data_base_path, caplog, monkeypatch):
    creator = DuckDBViewCreator(get_data_base_path)
    def mock_close_connection():
        raise Exception("Error closing connection")

    monkeypatch.setattr(creator.db_handler, "close_connection", mock_close_connection)

    creator.close_connection()
    assert "Error closing connection" in caplog.text
