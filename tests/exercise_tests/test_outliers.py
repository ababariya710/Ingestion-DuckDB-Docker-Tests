"""
Don't change this file please. We'll use it to evaluate your submission
"""
import subprocess

import duckdb


def run_outliers_calculation():
    result = subprocess.run(
        args=["python", "-m", "equalexperts_dataeng_exercise.outliers"],
        capture_output=True,
    )
    result.check_returncode()


def test_check_view_exists():
    sql = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type='VIEW' AND table_name='outlier_weeks' AND table_schema='blog_analysis';
    """
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) == 1, "Expected view 'outlier_weeks' to exist"
    finally:
        con.close()


def test_check_view_has_data():
    sql = "SELECT COUNT(*) FROM blog_analysis.outlier_weeks"
    run_outliers_calculation()
    con = duckdb.connect("warehouse.db", read_only=True)
    try:
        result = con.execute(sql)
        assert len(result.fetchall()) > 0, "Expected view 'outlier_weeks' to have data"
    finally:
        con.close()
