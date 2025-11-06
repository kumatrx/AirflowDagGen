# Airflow DAG GitLab Integration - version 2.0
# Last updated: 2025-11-06

import requests
from urllib.parse import quote


def push_to_gitlab(dag_code, dag_name, repo_url, branch, folder, token):
    """
    Pushes DAG code to GitLab repository via API.

    Args:
        dag_code: Python DAG code string
        dag_name: Name of the DAG file (without .py extension)
        repo_url: Full GitLab repo URL (e.g., https://gitlab.com/username/repo)
        branch: Target branch name
        folder: Folder path within repo (e.g., "dags/")
        token: GitLab Personal Access Token with api/write_repository scope

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # Extract project path from URL
        # Example: https://gitlab.com/username/repo -> username/repo
        project_path = repo_url.replace("https://gitlab.com/", "").replace("http://gitlab.com/", "")
        project_path = project_path.strip("/")

        # URL encode project path for API
        encoded_project = quote(project_path, safe='')

        # Construct file path
        file_path = f"{folder.rstrip('/')}/{dag_name}.py"
        encoded_file_path = quote(file_path, safe='')

        # GitLab API endpoint
        api_url = f"https://gitlab.com/api/v4/projects/{encoded_project}/repository/files/{encoded_file_path}"

        headers = {
            "PRIVATE-TOKEN": token,
            "Content-Type": "application/json"
        }

        payload = {
            "branch": branch,
            "content": dag_code,
            "commit_message": f"Add/Update Airflow DAG: {dag_name}"
        }

        # Try to create file (POST)
        response = requests.post(api_url, headers=headers, json=payload)

        if response.status_code == 201:
            return True, f"✅ DAG successfully created in GitLab at {file_path} on branch '{branch}'"

        # If file exists (400 error), try to update it (PUT)
        if response.status_code == 400 and "already exists" in response.text.lower():
            response = requests.put(api_url, headers=headers, json=payload)
            if response.status_code == 200:
                return True, f"✅ DAG successfully updated in GitLab at {file_path} on branch '{branch}'"

        # Handle other errors
        error_detail = response.text[:200] if response.text else "No error details"
        return False, f"❌ GitLab API Error ({response.status_code}): {error_detail}"

    except requests.exceptions.RequestException as e:
        return False, f"❌ Network Error: {str(e)}"
    except Exception as e:
        return False, f"❌ Error pushing to GitLab: {str(e)}"
