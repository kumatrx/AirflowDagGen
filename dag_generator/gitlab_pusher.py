"""
GitLab Push Module - Version 2.1
Pushes Airflow DAG files to GitLab repository via API
Last updated: 2025-11-06
"""

import requests
from urllib.parse import quote
import re


def push_to_gitlab(dag_code, dag_name, repo_url, branch, folder, token):
    """
    Push DAG code to GitLab repository using GitLab API.

    Args:
        dag_code (str): Complete DAG Python code
        dag_name (str): Name of the DAG file (without .py extension)
        repo_url (str): Full GitLab repository URL
        branch (str): Target branch name (e.g., 'main', 'develop')
        folder (str): Folder path within repository (e.g., 'dags/')
        token (str): GitLab Personal Access Token with API write access

    Returns:
        tuple: (success: bool, message: str)
    """
    try:
        # Clean URL - remove any angle brackets or extra whitespace
        repo_url = str(repo_url).strip()
        repo_url = re.sub(r'[<>]', '', repo_url)

        # Extract base URL and project path
        if "gitlab.com" in repo_url:
            base_url = "https://gitlab.com"
            project_path = repo_url.split("gitlab.com/", 1)[1] if "gitlab.com/" in repo_url else ""
        else:
            # Corporate/self-hosted GitLab
            temp = repo_url.replace("https://", "").replace("http://", "")
            parts = temp.split("/", 1)
            base_url = "https://" + parts[0]
            project_path = parts[1] if len(parts) > 1 else ""

        # Clean and encode project path
        project_path = project_path.strip("/")
        encoded_project = quote(project_path, safe='')

        # Build file path
        folder = folder.strip("/")
        file_path = folder + "/" + dag_name + ".py"
        encoded_file = quote(file_path, safe='')

        # Construct API URL
        api_url = f"{base_url}/api/v4/projects/{encoded_project}/repository/files/{encoded_file}"

        # Request headers
        headers = {
            "PRIVATE-TOKEN": token,
            "Content-Type": "application/json"
        }

        # Request payload
        payload = {
            "branch": branch,
            "content": dag_code,
            "commit_message": f"Add/Update Airflow DAG: {dag_name}"
        }

        # Try POST (create new file)
        # Note: verify=True uses the certificate from os.environ['REQUESTS_CA_BUNDLE']
        response = requests.post(api_url, headers=headers, json=payload, verify=True, timeout=30)

        if response.status_code == 201:
            return True, f"✅ DAG successfully created at {file_path} on branch '{branch}'"

        # If file exists (400 error), try PUT (update existing file)
        if response.status_code == 400:
            response = requests.put(api_url, headers=headers, json=payload, verify=True, timeout=30)
            if response.status_code == 200:
                return True, f"✅ DAG successfully updated at {file_path} on branch '{branch}'"

        # Handle other errors
        try:
            error_detail = response.json().get('message', response.text[:200])
        except:
            error_detail = response.text[:200] if response.text else "No error details available"

        return False, f"❌ GitLab API Error ({response.status_code}): {error_detail}"

    except requests.exceptions.SSLError as e:
        return False, f"❌ SSL Certificate Error: {str(e)}\n\nMake sure REQUESTS_CA_BUNDLE is set correctly in app.py"

    except requests.exceptions.Timeout:
        return False, f"❌ Request timeout. Please check your network connection and try again."

    except requests.exceptions.RequestException as e:
        return False, f"❌ Network Error: {str(e)}"

    except Exception as e:
        return False, f"❌ Unexpected Error: {str(e)}"
