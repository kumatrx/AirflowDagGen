# Airflow Automation Portal - version 2.0
# Last updated: 2025-11-06

import streamlit as st
from dag_generator.dag_builder import generate_dag_code
from dag_generator.task_handlers import TASK_TYPES, TASK_PARAMS
from datetime import datetime
import json

st.set_page_config(page_title="Airflow Automation Portal", page_icon=":rocket:")

st.markdown("""
    <style>
    .block-container { padding-top: 18px !important; }
    .centered-card {
        max-width: 670px;
        margin: 0 auto;
        background: #f8f9fc;
        padding: 22px 35px 24px 35px;
        border-radius: 16px;
        box-shadow: 0 2px 18px rgba(34,40,66,0.08);
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<div class="centered-card">', unsafe_allow_html=True)

st.title("Airflow Automation Portal")

choice = st.selectbox(
    "What do you want to automate?",
    (
        "Select an option",
        "Airflow Connection Management (coming soon)",
        "Orchestrate a Data Pipeline"
    )
)

if choice == "Airflow Connection Management (coming soon)":
    st.info("Connection management functionality will be added soon. Stay tuned!")

elif choice == "Orchestrate a Data Pipeline":
    st.subheader("Pipeline DAG Orchestration")

    col1, col2 = st.columns(2)
    with col1:
        dag_name = st.text_input("DAG Name (ID)", "data_pipeline_dag", help="Unique pipeline identifier")
        schedule_interval = st.text_input("Schedule Interval", "@daily", help="Cron or preset schedule")
        max_active_runs = st.number_input("Max Active Runs", 1, 200, 1, help="Concurrent runs limit")
        enable_timeout = st.checkbox("Enable DAG Run Timeout", value=False)
        dag_timeout_value = None
        dag_timeout_unit = None
        if enable_timeout:
            tcol1, tcol2 = st.columns([1, 1])
            with tcol1:
                dag_timeout_value = st.number_input("Timeout Value", min_value=1, value=60)
            with tcol2:
                dag_timeout_unit = st.selectbox("Timeout Unit", ["Minutes", "Seconds", "Hours"], index=0)

        catchup = st.checkbox("Catchup", False)
    with col2:
        tags = st.text_input("Tags for Monitoring (comma separated)", "")
        purpose = st.text_area("Purpose / Business Description", "", height=110)

    with st.expander("Default Args / Advanced Settings", expanded=False):
        colA, colB, colC = st.columns(3)
        with colA:
            owner = st.text_input("Owner", "airflow_user")
        with colB:
            start_date = st.date_input("Start Date", value=datetime.now())
        with colC:
            end_date = st.date_input("End Date", value=datetime(2099, 12, 31))
        depends_on_past = st.checkbox("Depends on previous run")
        failure_callback = st.text_input("On Failure Callback function", "")

        st.markdown("**Retry Settings**")
        colR1, colR2, colR3 = st.columns([1, 1, 1.5])
        with colR1:
            retries = st.number_input("Retries", 0, 20, 0)
        with colR2:
            retry_delay_value = st.number_input("Retry Delay Value", 1, 10000, 5)
        with colR3:
            retry_delay_unit = st.selectbox("Retry Delay Unit", ["Minutes", "Seconds", "Hours"], index=0)
        retry_exponential_backoff = st.checkbox("Exponential Backoff")
        max_retry_delay_value = st.number_input("Max Retry Delay Value", 1, 10000, 60)
        max_retry_delay_unit = st.selectbox("Max Retry Delay Unit", ["Minutes", "Seconds", "Hours"], index=0)

    st.markdown("---")
    num_tasks = st.number_input("Number of tasks", 1, 20, 1)

    tasks = []
    task_ids = set()
    for i in range(num_tasks):
        with st.expander(f"Task {i + 1} Configuration", expanded=True):
            colT1, colT2 = st.columns(2)
            with colT1:
                task_id = st.text_input(f"Task ID for Task {i + 1}", f"task_{i + 1}", key=f"task_id_{i}")
                if task_id in task_ids:
                    st.error("Duplicate Task ID")
                task_ids.add(task_id)
            with colT2:
                task_type = st.selectbox(f"Task Type for Task {i + 1}", list(TASK_TYPES.keys()), key=f"task_type_{i}")

            params = {}
            for param in TASK_PARAMS.get(task_type, []):
                if task_type in ["PythonOperator", "BranchPythonOperator"] and param in ("provide_context",
                                                                                         "op_kwargs"):
                    continue
                params[param] = st.text_input(f"{param} for Task {i + 1}", key=f"{param}_{i}")

            if task_type in ["PythonOperator", "BranchPythonOperator"]:
                safe_task_id = task_id.replace("-", "_").replace(" ", "_")
                provide_context = st.checkbox(f"Provide Context for Task {i + 1}",
                                              key=f"provide_context_{safe_task_id}")
                op_kwargs_str = st.text_area(f"op_kwargs (JSON) for Task {i + 1}", "{}",
                                             key=f"op_kwargs_{safe_task_id}", height=80)
                try:
                    op_kwargs = json.loads(op_kwargs_str)
                except Exception as e:
                    st.error(f"Invalid JSON in op_kwargs for Task {i + 1}: {e}")
                    op_kwargs = {}
                params['provide_context'] = provide_context
                params['op_kwargs'] = op_kwargs
            else:
                params['provide_context'] = False
                params['op_kwargs'] = {}

            tasks.append((task_id, task_type, params))

    st.markdown("### Define dependencies (upstream >> downstream)")
    dependencies = []
    if len(tasks) > 1:
        for i, (tid_i, _, _) in enumerate(tasks):
            for j, (tid_j, _, _) in enumerate(tasks):
                if i != j:
                    dep = st.checkbox(f"{tid_i} >> {tid_j}", key=f"dep_{i}_{j}")
                    if dep:
                        dependencies.append((tid_i, tid_j))

    st.markdown("### Define Custom Python Functions (Advanced)")
    custom_functions = {}
    num_funcs = st.number_input("Number of custom functions", 0, 5, 0)
    for i in range(num_funcs):
        func_name = st.text_input(f"Function {i + 1} Name", key=f"func_name_{i}")
        func_code = st.text_area(f"Function {i + 1} Code", height=100, key=f"func_code_{i}")
        if func_name and func_code:
            custom_functions[func_name] = func_code

    if st.button("Generate DAG Code"):
        unit_map = {"Minutes": "minutes", "Seconds": "seconds", "Hours": "hours"}

        default_args = {
            'owner': owner,
            'start_date': start_date,
            'end_date': end_date,
            'depends_on_past': depends_on_past,
        }

        if failure_callback:
            default_args['on_failure_callback'] = failure_callback

        if retries > 0:
            default_args['retries'] = retries
            if retry_exponential_backoff:
                default_args['retry_exponential_backoff'] = retry_exponential_backoff
            if retry_delay_value > 0:
                unit = unit_map.get(retry_delay_unit, "minutes")
                default_args["retry_delay"] = f"timedelta({unit}={retry_delay_value})"
            if max_retry_delay_value > 0:
                max_unit = unit_map.get(max_retry_delay_unit, "minutes")
                default_args["max_retry_delay"] = f"timedelta({max_unit}={max_retry_delay_value})"

        extra_args = []
        if tags.strip():
            tags_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
            extra_args.append(f"tags={tags_list}")

        extra_args.append(f"max_active_runs={max_active_runs}")
        extra_args.append(f"catchup={catchup}")

        if enable_timeout and dag_timeout_value:
            unit_param = unit_map.get(dag_timeout_unit, "minutes")
            extra_args.append(f"dagrun_timeout=timedelta({unit_param}={dag_timeout_value})")

        dag_code = generate_dag_code(
            dag_name,
            schedule_interval,
            tasks,
            dependencies,
            custom_functions,
            extra_args,
            default_args,
        )

        st.code(dag_code, language="python")

        # Validate DAG code
        from dag_generator.dag_validator import validate_dag_code

        is_valid, validation_msg = validate_dag_code(dag_code)

        # Store DAG code in session state to persist across reruns
        st.session_state['dag_code'] = dag_code
        st.session_state['dag_name'] = dag_name
        st.session_state['is_valid'] = is_valid
        st.session_state['validation_msg'] = validation_msg

    # Display validation and export options (outside the Generate button to persist)
    if 'dag_code' in st.session_state and st.session_state.get('is_valid'):
        st.success(st.session_state['validation_msg'])

        # Show export options
        st.markdown("---")
        st.subheader("📤 Export Options")

        # Let user choose export method - only Download or GitLab
        export_method = st.selectbox(
            "How would you like to export the DAG?",
            ["Select an option", "Download to Local Machine", "Push to GitLab"],
            help="Choose your preferred export method",
            key="export_method_select"
        )

        # Only show options if user has selected a valid export method
        if export_method == "Download to Local Machine":
            st.markdown("#### 📥 Download DAG File")
            st.download_button(
                label="Download DAG File",
                data=st.session_state['dag_code'],
                file_name=f"{st.session_state['dag_name']}.py",
                mime="text/x-python",
                help="Download the DAG file to your local machine for testing",
                use_container_width=True,
                key="download_dag_btn"
            )

        elif export_method == "Push to GitLab":
            st.markdown("#### 🚀 Push to GitLab")
            st.caption("Push the DAG directly to your GitLab repository")

            gitlab_url = st.text_input(
                "GitLab Repository URL",
                placeholder="https://gitlab.com/username/repository",
                help="Full URL to your GitLab repository",
                key="gitlab_url"
            )

            col_branch, col_folder = st.columns(2)
            with col_branch:
                gitlab_branch = st.text_input("Branch", "main", help="Target branch name", key="gitlab_branch")
            with col_folder:
                gitlab_folder = st.text_input("Folder Path", "dags/", help="Folder path within repository",
                                              key="gitlab_folder")

            gitlab_token = st.text_input(
                "GitLab Personal Access Token",
                type="password",
                help="Token with 'api' or 'write_repository' scope",
                key="gitlab_token"
            )

            if st.button("Push to GitLab", type="primary", use_container_width=True, key="push_gitlab_btn"):
                if gitlab_url and gitlab_token:
                    from dag_generator.gitlab_pusher import push_to_gitlab

                    success, msg = push_to_gitlab(
                        st.session_state['dag_code'],
                        st.session_state['dag_name'],
                        gitlab_url,
                        gitlab_branch,
                        gitlab_folder,
                        gitlab_token
                    )
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                else:
                    st.warning("⚠️ Please provide both GitLab URL and Personal Access Token")

    # Show validation error if DAG was invalid
    elif 'dag_code' in st.session_state and not st.session_state.get('is_valid'):
        st.error(st.session_state['validation_msg'])
        st.warning("⚠️ Please fix the errors above before exporting the DAG")

st.markdown("</div>", unsafe_allow_html=True)
