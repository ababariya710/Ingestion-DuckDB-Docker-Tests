"""
You shouldn't need to change this file.

This is a utility script which allows you to quickly run and test various parts of the exercise.

Use 'poetry run exercise --help' to see the various options

For example

    poetry run exercise ingest-data
    poetry run exercise detect-outliers
    poetry run exercise test

"""
import subprocess
from pathlib import Path

import duckdb
import typer

app = typer.Typer()


def run_cmd(cmd: str):
    proc = subprocess.Popen(cmd, shell=True)
    proc.communicate()


@app.command()
def tidy():
    run_cmd("isort equalexperts_dataeng_exercise")
    run_cmd("autopep8 --in-place --recursive equalexperts_dataeng_exercise")


@app.command()
def lint():
    run_cmd("mypy equalexperts_dataeng_exercise")
    run_cmd("flake8 equalexperts_dataeng_exercise")


@app.command()
def test():
    run_cmd(
        "pytest --cov=equalexperts_dataeng_exercise equalexperts_dataeng_exercise tests "
        f"--ignore={Path('tests') / 'exercise_tests'}"
    )


@app.command()
def fetch_data():
    run_cmd("python -m equalexperts_dataeng_exercise.scripts.fetch_data")


@app.command()
def ingest_data():
    path_to_data = Path("uncommitted") / "votes.jsonl"
    run_cmd(f"python -m equalexperts_dataeng_exercise.ingest {path_to_data}")


@app.command()
def run_query(query: str):
    conn = duckdb.connect("warehouse.db")
    result = conn.sql(query)
    result.show()


@app.command()
def detect_outliers():
    run_cmd("python -m equalexperts_dataeng_exercise.outliers")


@app.command()
def check_ingestion():
    run_cmd(f"pytest {Path('tests') / 'exercise_tests' / 'test_ingestion.py'}")


@app.command()
def check_outliers():
    run_cmd(f"pytest {Path('tests') / 'exercise_tests' / 'test_outliers.py'}")


def main():
    app()


if __name__ == "__main__":
    main()
