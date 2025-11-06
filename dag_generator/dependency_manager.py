def generate_dependencies_code(dependencies):
    """
    dependencies: list of (upstream_task_id, downstream_task_id) tuples
    Returns indented code strings for DAG task dependencies, e.g. "task1 >> task2"
    """
    lines = []
    for upstream, downstream in dependencies:
        lines.append(f"    {upstream} >> {downstream}")
    return "\n".join(lines)
