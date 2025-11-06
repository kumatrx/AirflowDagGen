# Airflow DAG Validator - version 2.0
# Last updated: 2025-11-06

import ast


def validate_dag_code(dag_code):
    """
    Validates generated DAG Python code for syntax errors.

    Args:
        dag_code: Python DAG code string

    Returns:
        tuple: (is_valid: bool, message: str)
    """
    try:
        ast.parse(dag_code)
        return True, "✅ DAG code is syntactically valid and ready to use!"
    except SyntaxError as e:
        return False, f"❌ Syntax Error at line {e.lineno}: {e.msg}"
    except Exception as e:
        return False, f"❌ Validation Error: {str(e)}"
