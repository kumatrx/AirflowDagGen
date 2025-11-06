# Airflow DAG Code Builder - version 2.0

from dag_generator.task_handlers import TASK_TYPES
from datetime import datetime, date

def python_repr(val):
    """
    Converts Python values to their string representation for code generation.
    Handles datetime, date, bool, str, timedelta, and None types.
    """
    if isinstance(val, datetime):
        return f"datetime({val.year}, {val.month}, {val.day})"
    elif isinstance(val, date):  # Handle date objects from st.date_input
        return f"datetime({val.year}, {val.month}, {val.day})"
    elif isinstance(val, bool):
        return str(val)
    elif isinstance(val, str):
        if val.startswith("timedelta("):
            return val
        return f'"{val}"'
    elif val is None:
        return "None"
    else:
        return str(val)

def generate_dag_code(dag_name, schedule_interval, tasks, dependencies, custom_functions, extra_args=None, default_args_dict=None):
    extra_args = extra_args or []
    default_args_dict = default_args_dict or {}

    # Base imports always needed
    base_imports = [
        "from airflow import DAG",
        "from datetime import datetime, timedelta",
    ]
    task_type_imports = {
        "BashOperator": "from airflow.operators.bash import BashOperator",
        "PythonOperator": "from airflow.operators.python import PythonOperator",
        "DummyOperator": "from airflow.operators.dummy import DummyOperator",
        "EmailOperator": "from airflow.operators.email import EmailOperator",
        "BranchPythonOperator": "from airflow.operators.branch import BranchPythonOperator",
        "HttpSensor": "from airflow.sensors.http_sensor import HttpSensor",
        "FileSensor": "from airflow.sensors.filesystem import FileSensor",
        "SqlOperator": "from airflow.operators.sql import SqlOperator",
        "DockerOperator": "from airflow.providers.docker.operators.docker import DockerOperator",
        "TriggerDagRunOperator": "from airflow.operators.trigger_dagrun import TriggerDagRunOperator",
        "SnowflakeOperator": "from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator",
    }
    used_imports = set()
    for _, task_type, _ in tasks:
        imp = task_type_imports.get(task_type)
        if imp:
            used_imports.add(imp)
    import_lines = base_imports + sorted(used_imports)
    code_lines = []
    code_lines.extend(import_lines)
    code_lines.append("")

    # Custom functions
    for fname, fcode in custom_functions.items():
        code_lines.append(f"# Custom function: {fname}")
        code_lines.append(fcode)
        code_lines.append("")
    default_args_lines = ["default_args = {"]
    for k, v in default_args_dict.items():
        default_args_lines.append(f'    "{k}": {python_repr(v)},')
    default_args_lines.append("}")
    default_args_lines.append("")
    code_lines.extend(default_args_lines)

    # Add the DAG block header (tasks and dependencies come INSIDE here)
    dag_lines = [
        "with DAG(",
        f"    dag_id='{dag_name}',",
        "    default_args=default_args,",
        f"    schedule_interval='{schedule_interval}',"
    ]
    for extra in extra_args:
        dag_lines.append(f"    {extra},")
    if dag_lines[-1].endswith(","):
        dag_lines[-1] = dag_lines[-1][:-1]
    dag_lines.append(") as dag:")
    code_lines.extend(dag_lines)
    code_lines.append("")

    # Indented task blocks
    for task_id, task_type, params in tasks:
        handler = TASK_TYPES.get(task_type)
        if handler:
            task_code = handler.generate_code(task_id, params)
            task_code_indented = "\n".join("    " + line if line.strip() else "" for line in task_code.splitlines())
            code_lines.append(task_code_indented)
        else:
            code_lines.append(f"# Unknown task type for {task_id}: {task_type}")

    # Dependency processing: detect linear chains
    adjacency = {}
    for u,v in dependencies:
        adjacency.setdefault(u, []).append(v)
    visited = set()
    chains = []
    def get_chain(start):
        chain = [start]
        while start in adjacency and len(adjacency[start]) == 1 and adjacency[start][0] not in chain:
            next_task = adjacency[start][0]
            chain.append(next_task)
            start = next_task
        return chain
    for u, v in dependencies:
        if u not in visited:
            chain = get_chain(u)
            for c in chain:
                visited.add(c)
            if len(chain) > 1:
                chains.append(chain)
    rendered_tasks = set()
    for chain in chains:
        code_lines.append("    " + " >> ".join(chain))
        rendered_tasks.update(chain)
    # Leftover dependencies
    for u, v in dependencies:
        if u not in rendered_tasks or v not in rendered_tasks:
            code_lines.append(f"    {u} >> {v}")

    code_lines.append("")
    return "\n".join(code_lines)
