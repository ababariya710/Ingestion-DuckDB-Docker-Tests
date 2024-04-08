import json
import tempfile
import os
import pytest
from equalexperts_dataeng_exercise.ingest import DuckDBDataIngester
from equalexperts_dataeng_exercise.db import DuckDBHandler

@pytest.fixture(scope="module")
def get_data_base_path():
    temp_db_file = tempfile.NamedTemporaryFile(suffix='.db')
    temp_db_file.close()
    return temp_db_file.name


def test_successful_data_insertion(get_data_base_path):
    inserter = DuckDBDataIngester(get_data_base_path)
    data = [
        {"Id": "1", "UserId": "1", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-01"},
        {"Id": "2", "UserId": "2", "PostId": "1", "VoteTypeId": "2", "CreationDate": "2022-01-01"}
    ]
    filename = 'test_data.json'
    with open(filename, 'w') as file:
        for entry in data:
            file.write(json.dumps(entry) + '\n')
    
    inserter.insert_json_data(filename)

    inserter.db_handler.connect()
    result = inserter.db_handler.execute("SELECT COUNT(*) FROM blog_analysis.votes").fetchall()
    assert result[0][0] == 2
    os.remove(filename)


def test_error_handling_invalid_json(get_data_base_path, caplog):
    inserter = DuckDBDataIngester(get_data_base_path)
    filename = 'invalid_data.json'
    with open(filename, 'w') as file:
        file.write("invalid json\n")

    inserter.insert_json_data(filename)
    
    assert "Error processing line" in caplog.text
    os.remove(filename)

def test_error_handling_connection_failure(caplog, get_data_base_path):
    inserter = DuckDBDataIngester(get_data_base_path)
    filename = 'test_data.json'
    os.unlink(get_data_base_path)  # Simulate database file deletion

    inserter.insert_json_data(filename)

    assert "Error" in caplog.text
