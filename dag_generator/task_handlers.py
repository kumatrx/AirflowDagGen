# Airflow DAG Task Handlers - version 2.0

class TaskTypeBase:
    def generate_code(self, task_id: str, params: dict) -> str:
        raise NotImplementedError

class BashOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        bash_command = params.get('bash_command', 'echo "Hello World"').replace('"', '\\"')
        return f'''{task_id} = BashOperator(
    task_id="{task_id}",
    bash_command="{bash_command}"
)
'''

class PythonOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        python_callable = params.get('python_callable', 'my_function')
        provide_context = params.get('provide_context', False)
        op_kwargs = params.get('op_kwargs', {})

        op_kwargs_code = ""
        if op_kwargs:
            items = [f'"{k}": {repr(v)}' for k, v in op_kwargs.items()]
            op_kwargs_code = ",\n    op_kwargs={{ " + ", ".join(items) + " }}"

        provide_context_code = ",\n    provide_context=True" if provide_context else ""

        return f'''{task_id} = PythonOperator(
    task_id="{task_id}",
    python_callable={python_callable}{provide_context_code}{op_kwargs_code}
)
'''

class DummyOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        return f'''{task_id} = DummyOperator(
    task_id="{task_id}"
)
'''

class EmailOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        to = params.get('to', '')
        subject = params.get('subject', '')
        html_content = params.get('html_content', '""').replace('"', '\\"')
        return f'''{task_id} = EmailOperator(
    task_id="{task_id}",
    to="{to}",
    subject="{subject}",
    html_content="{html_content}"
)
'''

class BranchPythonOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        python_callable = params.get('python_callable', 'branch_func')
        provide_context = params.get('provide_context', False)
        op_kwargs = params.get('op_kwargs', {})

        op_kwargs_code = ""
        if op_kwargs:
            items = [f'"{k}": {repr(v)}' for k, v in op_kwargs.items()]
            op_kwargs_code = ",\n    op_kwargs={{ " + ", ".join(items) + " }}"

        provide_context_code = ",\n    provide_context=True" if provide_context else ""

        return f'''{task_id} = BranchPythonOperator(
    task_id="{task_id}",
    python_callable={python_callable}{provide_context_code}{op_kwargs_code}
)
'''

class HttpSensorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        endpoint = params.get('endpoint', '')
        method = params.get('method', 'GET')
        return f'''{task_id} = HttpSensor(
    task_id="{task_id}",
    http_conn_id="http_default",
    endpoint="{endpoint}",
    method="{method}"
)
'''

class FileSensorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        filepath = params.get('filepath', '/tmp/dummy.txt')
        return f'''{task_id} = FileSensor(
    task_id="{task_id}",
    filepath="{filepath}"
)
'''

class SqlOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        sql = params.get('sql', 'SELECT 1')
        conn_id = params.get('conn_id', 'my_db')
        return f'''{task_id} = SqlOperator(
    task_id="{task_id}",
    sql="{sql}",
    conn_id="{conn_id}"
)
'''

class DockerOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        image = params.get('image', 'ubuntu:latest')
        command = params.get('command', 'echo hello').replace('"', '\\"')
        return f'''{task_id} = DockerOperator(
    task_id="{task_id}",
    image="{image}",
    command="{command}"
)
'''

class TriggerDagRunOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
        trigger_dag_id = params.get('trigger_dag_id', '')
        return f'''{task_id} = TriggerDagRunOperator(
    task_id="{task_id}",
    trigger_dag_id="{trigger_dag_id}"
)
'''

class SnowflakeOperatorTask(TaskTypeBase):
    def generate_code(self, task_id, params):
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
    task_id="{task_id}",
    sql="{sql}",
    snowflake_conn_id="{snowflake_conn_id}"{options_str}
)
'''

TASK_TYPES = {
    "BashOperator": BashOperatorTask(),
    "PythonOperator": PythonOperatorTask(),
    "DummyOperator": DummyOperatorTask(),
    "EmailOperator": EmailOperatorTask(),
    "BranchPythonOperator": BranchPythonOperatorTask(),
    "HttpSensor": HttpSensorTask(),
    "FileSensor": FileSensorTask(),
    "SqlOperator": SqlOperatorTask(),
    "DockerOperator": DockerOperatorTask(),
    "TriggerDagRunOperator": TriggerDagRunOperatorTask(),
    "SnowflakeOperator": SnowflakeOperatorTask(),
}

TASK_PARAMS = {
    "BashOperator": ["bash_command"],
    "PythonOperator": ["python_callable", "provide_context", "op_kwargs"],
    "DummyOperator": [],
    "EmailOperator": ["to", "subject", "html_content"],
    "BranchPythonOperator": ["python_callable", "provide_context", "op_kwargs"],
    "HttpSensor": ["endpoint", "method"],
    "FileSensor": ["filepath"],
    "SqlOperator": ["sql", "conn_id"],
    "DockerOperator": ["image", "command"],
    "TriggerDagRunOperator": ["trigger_dag_id"],
    "SnowflakeOperator": ["sql", "snowflake_conn_id", "warehouse", "database", "schema", "role"],
}
