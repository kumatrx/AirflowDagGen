# Airflow DAG Task Handlers - version 3
import streamlit as st
from datetime import timedelta


def format_retry_params(task_args):
    if not task_args:
        return ""

    retry_params = []
    if "retries" in task_args:
        retry_params.append(f"retries={task_args['retries']}")

    if "retry_delay" in task_args:
        val = task_args["retry_delay"]
        if isinstance(val, timedelta):
            seconds = int(val.total_seconds())
            retry_params.append(f"retry_delay=timedelta(seconds={seconds})")
        else:
            retry_params.append(f"retry_delay={val}")

    if "retry_exponential_backoff" in task_args and task_args["retry_exponential_backoff"]:
        retry_params.append(f"retry_exponential_backoff={task_args['retry_exponential_backoff']}")

    if "max_retry_delay" in task_args:
        val = task_args["max_retry_delay"]
        if isinstance(val, timedelta):
            seconds = int(val.total_seconds())
            retry_params.append(f"max_retry_delay=timedelta(seconds={seconds})")
        else:
            retry_params.append(f"max_retry_delay={val}")

    if retry_params:
        # Format retry params in multiline with indentation
        return ",\n    " + ",\n    ".join(retry_params)
    else:
        return ""

class TaskTypeBase:
    def generate_code(self, task_id: str, params: dict, task_args=None) -> str:
        raise NotImplementedError


class BashOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)
        bash_command = params.get('bash_command', 'echo "Hello World"').replace('"', '\\"')

        return f'''{task_id} = BashOperator(
    task_id="{task_id}"{retry_params},
    bash_command="{bash_command}"
)'''


class PythonOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)
        python_callable = params.get('python_callable', 'my_function')
        provide_context = params.get('provide_context', False)
        op_kwargs = params.get('op_kwargs', {})

        op_kwargs_code = ""
        if op_kwargs:
            items = [f'"{k}": {repr(v)}' for k, v in op_kwargs.items()]
            op_kwargs_code = ",\n    op_kwargs={ " + ", ".join(items) + " }"
        provide_context_code = ",\n    provide_context=True" if provide_context else ""

        return f'''{task_id} = PythonOperator(
    task_id="{task_id}"{retry_params},
    python_callable={python_callable}{provide_context_code}{op_kwargs_code}
)'''


class DummyOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)
        return f'''{task_id} = DummyOperator(
    task_id="{task_id}"{retry_params}
)'''


class EmailOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)
        to = params.get('to', '')
        subject = params.get('subject', '')
        html_content = params.get('html_content', '""').replace('"', '\\"')

        return f'''{task_id} = EmailOperator(
    task_id="{task_id}"{retry_params},
    to="{to}",
    subject="{subject}",
    html_content="{html_content}"
)'''


class BranchPythonOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        python_callable = params.get('python_callable', 'branch_func')
        provide_context = params.get('provide_context', False)
        op_kwargs = params.get('op_kwargs', {})

        op_kwargs_code = ""
        if op_kwargs:
            items = [f'"{k}": {repr(v)}' for k, v in op_kwargs.items()]
            op_kwargs_code = ",\n    op_kwargs={ " + ", ".join(items) + " }"
        provide_context_code = ",\n    provide_context=True" if provide_context else ""

        return f'''{task_id} = BranchPythonOperator(
    task_id="{task_id}"{retry_params},
    python_callable={python_callable}{provide_context_code}{op_kwargs_code}
)'''


class HttpSensorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        endpoint = params.get('endpoint', '')
        method = params.get('method', 'GET')
        poke_interval = params.get('poke_interval', '')
        timeout = params.get('timeout', '')
        mode = params.get('mode', '')
        soft_fail = params.get('soft_fail', '')

        code = f'''{task_id} = HttpSensor(
    task_id="{task_id}"{retry_params},
    http_conn_id="http_default",
    endpoint="{endpoint}",
    method="{method}"'''

        if poke_interval:
            code += f",\n    poke_interval={poke_interval}"
        if timeout:
            code += f",\n    timeout={timeout}"
        if mode:
            code += f",\n    mode=\"{mode}\""
        if soft_fail:
            soft_fail_val = 'True' if soft_fail else 'False'
            code += f",\n    soft_fail={soft_fail_val}"

        code += "\n)"

        return code


class FileSensorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        filepath = params.get('filepath', '/tmp/dummy.txt')
        poke_interval = params.get('poke_interval', '')
        timeout = params.get('timeout', '')
        mode = params.get('mode', '')
        soft_fail = params.get('soft_fail', '')

        code = f'''{task_id} = FileSensor(
    task_id="{task_id}"{retry_params},
    filepath="{filepath}"'''

        if poke_interval:
            code += f",\n    poke_interval={poke_interval}"
        if timeout:
            code += f",\n    timeout={timeout}"
        if mode:
            code += f",\n    mode=\"{mode}\""
        if soft_fail:
            soft_fail_val = 'True' if soft_fail else 'False'
            code += f",\n    soft_fail={soft_fail_val}"

        code += "\n)"

        return code


class SqlOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        sql = params.get('sql', 'SELECT 1')
        conn_id = params.get('conn_id', 'my_db')

        return f'''{task_id} = SqlOperator(
    task_id="{task_id}"{retry_params},
    sql="{sql}",
    conn_id="{conn_id}"
)'''


class DockerOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        image = params.get('image', 'ubuntu:latest')
        command = params.get('command', 'echo hello').replace('"', '\\"')

        return f'''{task_id} = DockerOperator(
    task_id="{task_id}"{retry_params},
    image="{image}",
    command="{command}"
)'''


class TriggerDagRunOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)
        trigger_dag_id = params.get('trigger_dag_id', '')

        return f'''{task_id} = TriggerDagRunOperator(
    task_id="{task_id}"{retry_params},
    trigger_dag_id="{trigger_dag_id}"
)'''


class SnowflakeOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params, task_args=None):
        retry_params = format_retry_params(task_args)

        sql = params.get('sql', 'SELECT CURRENT_DATE;').replace('"', '\\"')
        snowflake_conn_id = params.get('snowflake_conn_id', 'snowflake_default')
        warehouse = params.get('warehouse', '')
        database = params.get('database', '')
        schema = params.get('schema', '')
        role = params.get('role', '')

        options = []
        if warehouse:
            options.append(f'warehouse="{warehouse}"')
        if database:
            options.append(f'database="{database}"')
        if schema:
            options.append(f'schema="{schema}"')
        if role:
            options.append(f'role="{role}"')

        options_str = ""
        for opt in options:
            options_str += f",\n    {opt}"

        return f'''{task_id} = SnowflakeOperator(
    task_id="{task_id}"{retry_params},
    sql="{sql}",
    snowflake_conn_id="{snowflake_conn_id}"{options_str}
)'''


TASK_TYPES = {
    "BashOperator": BashOperatorTask(),
    "BranchPythonOperator": BranchPythonOperatorTask(),
    "DockerOperator": DockerOperatorTask(),
    "DummyOperator": DummyOperatorTask(),
    "EmailOperator": EmailOperatorTask(),
    "FileSensor": FileSensorTask(),
    "HttpSensor": HttpSensorTask(),
    "PythonOperator": PythonOperatorTask(),
    "SqlOperator": SqlOperatorTask(),
    "SnowflakeOperator": SnowflakeOperatorTask(),
    "TriggerDagRunOperator": TriggerDagRunOperatorTask(),
}

TASK_PARAMS = {
    "BashOperator": ["bash_command"],
    "PythonOperator": ["python_callable", "provide_context", "op_kwargs"],
    "DummyOperator": [],
    "EmailOperator": ["to", "subject", "html_content"],
    "BranchPythonOperator": ["python_callable", "provide_context", "op_kwargs"],
    "HttpSensor": ["endpoint", "method", "poke_interval", "timeout", "mode", "soft_fail"],
    "FileSensor": ["filepath", "poke_interval", "timeout", "mode", "soft_fail"],
    "SqlOperator": ["sql", "conn_id"],
    "DockerOperator": ["image", "command"],
    "TriggerDagRunOperator": ["trigger_dag_id"],
    "SnowflakeOperator": ["sql", "snowflake_conn_id", "warehouse", "database", "schema", "role"],
}
