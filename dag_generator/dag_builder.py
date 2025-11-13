# Airflow DAG Code Builder - version 3

from dag_generator.task_handlers import TASK_TYPES
from datetime import datetime, date
import streamlit as st
import re
from datetime import timedelta
from dag_generator.utils.alert_notification_handlers import notify_failure, notify_success

def format_timedelta_code(value, unit):
    # Sanitize unit if needed (should be one of valid timedelta args)
    valid_units = ["days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"]
    if unit not in valid_units:
        unit = "seconds"  # fallback
    return f"timedelta({unit}={value})"


def fix_datetime_calls(func_code):
    code = re.sub(r'datetime\\.datetime\\.','datetime', func_code)
    return code

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

def clean_duplicate_imports(custom_functions: dict) -> dict:
    forbidden = ['import datetime', 'from datetime import datetime']
    cleaned = {}

    for func_name, func_code in custom_functions.items():
        lines = []
        for line in func_code.split('\n'):
            if line.strip() not in forbidden:
                lines.append(line)
        cleaned[func_name] = '\n'.join(lines)
    return cleaned

def generate_dag_code(dag_name, schedule_interval, tasks, dependencies, custom_functions, extra_args=None,
                      default_args_dict=None):
    extra_args = extra_args or []
    default_args_dict = default_args_dict or {}
    failure_notif_options = default_args_dict.get("failure_notif_options", [])
    failure_emails_str = default_args_dict.get("failure_emails", "")
    failure_emails = [email.strip() for email in failure_emails_str.split(",") if email.strip()]
    success_notif_options = default_args_dict.get("success_notif_options", [])
    success_emails_str = default_args_dict.get("success_emails", "")
    success_emails = [email.strip() for email in success_emails_str.split(",") if email.strip()]

    # Base imports always needed
    base_imports = [
        "from airflow import DAG",
        "from datetime import datetime, timedelta",
    ]

    task_type_imports = {
        "BashOperator": "from airflow.operators.bash import BashOperator",
        "DummyOperator": "from airflow.operators.dummy import DummyOperator",
        "EmailOperator": "from airflow.operators.email import EmailOperator",
        "BranchPythonOperator": "from airflow.operators.branch import BranchPythonOperator",
        "HttpSensor": "from airflow.sensors.http_sensor import HttpSensor",
        "FileSensor": "from airflow.sensors.filesystem import FileSensor",
        "PythonOperator": "from airflow.operators.python import PythonOperator",
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

    # Add pendulum import if timezone is specified
    needs_pendulum = any("timezone=" in arg for arg in extra_args)
    if needs_pendulum:
        base_imports.insert(0, "from pendulum import timezone")

    import_lines = base_imports + sorted(used_imports)

    code_lines = []
    code_lines.extend(import_lines)
    code_lines.append("")

    if failure_notif_options:
        code_lines.append(
            "def make_failure_callback(failure_notif_options, failure_emails):\n"
            "    def on_failure_callback(context):\n"
            "        from utils.alert_notification_handlers import notify_failure\n"
            "        notify_failure(context, notif_options=failure_notif_options, emails=failure_emails)\n"
            "    return on_failure_callback\n"
        )
    if success_notif_options:
        code_lines.append(
            "def make_success_callback(success_notif_options, success_emails):\n"
            "    def on_success_callback(context):\n"
            "        from utils.alert_notification_handlers import notify_success\n"
            "        notify_success(context, notif_options=success_notif_options, emails=success_emails)\n"
            "    return on_success_callback\n"
        )

    if custom_functions:
        custom_functions = clean_duplicate_imports(custom_functions)
        for k in custom_functions:
            custom_functions[k] = fix_datetime_calls(custom_functions[k])

    # Custom functions
    for fname, fcode in custom_functions.items():
        code_lines.append(f"# Custom function: {fname}")
        code_lines.append(fcode)
        code_lines.append("")

    default_args_lines = ["default_args = {"]
    excluded_keys = {
        "failure_notif_options",
        "failure_emails",
        "success_notif_options",
        "success_emails",
    }
    for k, v in default_args_dict.items():
        if k in excluded_keys:
            continue  # Skip notification configs here
        default_args_lines.append(f'    "{k}": {python_repr(v)},')
    for extra in extra_args:
        if extra.startswith('timezone='):
            default_args_lines.append(f'    "timezone": timezone("Europe/London")')
    if failure_notif_options:
        default_args_lines.append(f'    "on_failure_callback": make_failure_callback({failure_notif_options}, {failure_emails}),')
    if success_notif_options:
        default_args_lines.append(f'    "on_success_callback": make_success_callback({success_notif_options}, {success_emails}),')
    default_args_lines.append("}")
    default_args_lines.append("")

    code_lines.extend(default_args_lines)

    # Add the DAG block header (tasks and dependencies come INSIDE here)
    dag_lines = [
        "with DAG(",
        f"    dag_id='{dag_name}',",
        "    default_args=default_args,",
        f"    schedule_interval='{schedule_interval}',",
    ]

    for extra in extra_args:
        if not extra.startswith('timezone='):
            dag_lines.append(f"    {extra},")

    if dag_lines[-1].endswith(","):
        dag_lines[-1] = dag_lines[-1][:-1]

    dag_lines.append(") as dag:")

    code_lines.extend(dag_lines)
    code_lines.append("")

    # Indented task blocks
    for task_id, task_type, params in tasks:
        task_args = default_args_dict.copy()

        # Check if individual retry enabled; assuming you track that flag in params
        if params.get("individual_retry_enabled", False):
            if "retries" in params:
                task_args["retries"] = params["retries"]
            if "retry_delay_value" in params and "retry_delay_unit" in params:
                # Convert to timedelta
                retry_delay = format_timedelta_code(params["retry_delay_value"], params["retry_delay_unit"])
                task_args["retry_delay"] = retry_delay
            if "retry_exponential_backoff" in params:
                task_args["retry_exponential_backoff"] = params["retry_exponential_backoff"]
            if "max_retry_delay_value" in params and "max_retry_delay_unit" in params:
                max_retry_delay = format_timedelta_code(params["max_retry_delay_value"], params["max_retry_delay_unit"])
                task_args["max_retry_delay"] = max_retry_delay
        else:
            # Do not add retries or retry_delay keys so global defaults apply implicitly
            # optionally explicitly delete retry keys if present
            for key in ["retries", "retry_delay", "retry_exponential_backoff", "max_retry_delay"]:
                task_args.pop(key, None)

        handler = TASK_TYPES.get(task_type)
        if handler:
            task_code = handler.generate_code(task_id, params, task_args)
            task_code_indented = "\n".join("    " + line if line.strip() else "" for line in task_code.splitlines())
            code_lines.append(task_code_indented)
        else:
            code_lines.append(f"    # Unknown task type for {task_id}: {task_type}")

    # Dependency processing: detect linear chains
    adjacency = {}
    for u, v in dependencies:
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
