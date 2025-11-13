import tempfile
import subprocess
import os
import json

def run_ai_unit_tests(dag_code: str, test_code: str) -> dict:
    """
    Write DAG and test code to temp files, run pytest, and return result report.

    Args:
        dag_code: The Python Airflow DAG code string.
        test_code: The pytest unit test code string.

    Returns:
        dict with keys:
            - stdout: pytest stdout log
            - stderr: pytest stderr log
            - test_report: parsed pytest JSON report (if generated)
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        dag_path = os.path.join(temp_dir, "dag_under_test.py")
        test_path = os.path.join(temp_dir, "test_dag.py")
        with open(dag_path, "w") as f:
            f.write(dag_code)
        with open(test_path, "w") as f:
            f.write(test_code)

        # Run pytest with JSON report output
        try:
            result = subprocess.run(
                [
                    "pytest",
                    test_path,
                    "--maxfail=5",
                    "--disable-warnings",
                    "--tb=short",
                    "--json-report",
                    "--json-report-file=report.json"
                ],
                cwd=temp_dir,
                capture_output=True,
                text=True,
                timeout=60,
            )
            report = {}
            report_path = os.path.join(temp_dir, "report.json")
            if os.path.exists(report_path):
                with open(report_path, "r") as f:
                    report = json.load(f)
            return {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "test_report": report
            }
        except subprocess.TimeoutExpired:
            return {
                "stdout": "",
                "stderr": "Test execution timed out.",
                "test_report": {}
            }
        except Exception as e:
            return {
                "stdout": "",
                "stderr": f"Test execution error: {str(e)}",
                "test_report": {}
            }
