# ai_function_generator.py
# AI-Powered Function Generator for Airflow Portal
# Uses enterprise JWT authentication with LLM API

import requests
import base64
import time
import json
import re
import os
from typing import Dict, Optional


class AIFunctionGenerator:
    """Generate Python functions from natural language descriptions using enterprise LLM"""

    def __init__(self):
        """Initialize with configuration from environment variables"""
        self.token_url = os.getenv('LLM_TOKEN_URL')
        self.username = os.getenv('LLM_USERNAME')
        self.password = os.getenv('LLM_PASSWORD')
        self.model_endpoint = os.getenv('LLM_MODEL_ENDPOINT')

        if not all([self.token_url, self.username, self.password, self.model_endpoint]):
            raise ValueError("LLM configuration incomplete. Check environment variables.")

    def generate_function(self, description: str, context: str = 'airflow') -> Dict:
        """
        Generate Python function from natural language description

        Args:
            description: Natural language description of what the function should do
            context: Context type ('airflow' for Airflow functions, 'general' for generic)

        Returns:
            {
                'success': True/False,
                'function_code': 'def my_func(): ...',
                'function_name': 'my_func',
                'error': 'error message if any'
            }
        """
        try:
            # Build optimized prompt
            prompt = self._build_prompt(description, context)

            # Call LLM with retry logic
            response = self._call_llm(prompt)

            # Extract and validate code
            code = self._extract_code(response)
            name = self._extract_function_name(code)

            # Validate generated code
            is_valid, validation_msg = self._validate_code(code)
            if not is_valid:
                return {
                    'success': False,
                    'function_code': code,
                    'function_name': name,
                    'error': f"Generated code validation failed: {validation_msg}"
                }

            return {
                'success': True,
                'function_code': code,
                'function_name': name,
                'error': None
            }

        except Exception as e:
            return {
                'success': False,
                'function_code': '',
                'function_name': '',
                'error': f"Function generation failed: {str(e)}"
            }

    def generate_code_review(self, dag_code: str) -> dict:
        review_prompt = (
            "You are an expert Airflow developer. Review the following generated Airflow DAG Python code. "
            "Provide detailed feedback on code quality, optimization, best practices, security vulnerabilities, "
            "inefficient dependency patterns, bottlenecks, and error handling. Suggest improvements and highlight potential issues.\n\n"
            f"### DAG Code ###\n{dag_code}\n\n### Review ###"
        )
        try:
            review_text = self._call_llm(review_prompt)
            return {
                'success': True,
                'review': review_text,
                'error': None
            }
        except Exception as e:
            return {
                'success': False,
                'review': '',
                'error': f"Code review generation failed: {str(e)}"
            }

    def generate_unit_tests(self, dag_code: str) -> dict:
        """
        Generate pytest unit test scripts for the given Airflow DAG code using AI.

        Args:
            dag_code: The Python Airflow DAG code string.

        Returns:
            dict: {
                'success': True/False,
                'unit_tests': generated pytest code string or '',
                'error': error message if any
            }
        """
        prompt = (
            "As a Python expert, write comprehensive pytest-based unit tests for the following Airflow DAG code. "
            "Cover task dependencies, parameter edge cases, notification handlers, success and failure scenarios. "
            "Use mocks for Airflow operators as needed. Provide only the Python test code without explanation.\n\n"
            f"### DAG Code ###\n{dag_code}\n\n### Unit Tests ###"
        )
        try:
            test_code = self._call_llm(prompt)
            return {'success': True, 'unit_tests': test_code, 'error': None}
        except Exception as e:
            return {'success': False, 'unit_tests': '', 'error': f"Test generation failed: {str(e)}"}

    def _build_prompt(self, description: str, context: str) -> str:
        """Build optimized prompt for LLM based on context"""

        if context == 'airflow':
            prompt = f"""Generate a production-ready Python function for Apache Airflow based on this description:

{description}

Requirements:
1. Use **context parameter to access Airflow context
2. Include comprehensive docstring explaining what the function does
3. Add try-except error handling for robustness
4. Use context['task_instance'] for XCom operations (push/pull data)
5. Include logging statements for debugging
6. Return meaningful value or dictionary
7. Follow PEP 8 style guidelines
8. Use descriptive variable names

Example structure:
def my_function(**context):
    \"\"\"Brief description\"\"\"
    import logging

    ti = context['task_instance']
    logger = logging.getLogger(__name__)

    try:
        # Your logic here
        result = ...

        logger.info(f"Processed: {{result}}")
        ti.xcom_push(key='result', value=result)
        return result

    except Exception as e:
        logger.error(f"Error: {{str(e)}}")
        raise

Generate ONLY the Python function code. Do not include explanations, markdown formatting, or additional text."""

        else:
            prompt = f"""Generate a Python function based on this description:

{description}

Requirements:
1. Include docstring
2. Add error handling
3. Use descriptive names
4. Follow PEP 8 style

Generate ONLY the Python function code. No explanations or markdown."""

        return prompt

    def _get_jwt(self):
        """
        Generate JWT token using basic authentication

        Returns:
            JWT token string

        Raises:
            Exception: If token generation fails
        """
        credentials = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()

        headers = {'Authorization': f'Basic {credentials}'}

        try:
            response = requests.post(self.token_url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text.strip()
        except requests.exceptions.RequestException as e:
            raise Exception(f"JWT token generation failed: {str(e)}")

    def _call_llm(self, prompt: str, max_retries: int = 5, base_delay: int = 2) -> str:
        """
        Call LLM API with exponential backoff retry logic for rate limiting

        Args:
            prompt: The prompt to send to the LLM
            max_retries: Maximum number of retry attempts (default: 5)
            base_delay: Base delay in seconds for exponential backoff (default: 2)

        Returns:
            LLM response text

        Raises:
            Exception: If all retries fail or non-retryable error occurs
        """
        # Get fresh JWT token
        token = self._get_jwt()

        # Configure LLM request
        config = {
            "maxTokens": 4096,
            "temperature": 0.3  # Lower temperature for more deterministic code generation
        }

        payload = {
            "messages": [{"role": "user", "content": {"text": prompt}}],
            "inferenceConfig": config
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        # Retry loop with exponential backoff
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    self.model_endpoint,
                    json=payload,
                    headers=headers,
                    timeout=120
                )
                resp.raise_for_status()
                result = resp.json()

                # Extract response based on LLM API structure
                try:
                    return result['output']['message']['content'][0]['text']
                except (KeyError, IndexError, TypeError) as e:
                    raise ValueError(f"Unexpected LLM response format: {result}")

            except requests.exceptions.HTTPError as e:
                # Handle rate limiting (429) with retry
                if resp.status_code == 429 and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    print(f"Rate limited (429). Retrying in {delay}s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    # Non-retryable error or max retries exceeded
                    raise Exception(f"LLM API call failed: {resp.status_code} - {resp.text}")

            except requests.exceptions.RequestException as e:
                # Network or timeout errors
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    print(f"Request failed. Retrying in {delay}s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    raise Exception(f"LLM API request failed after {max_retries} attempts: {str(e)}")

        raise Exception("LLM call failed after all retry attempts")

    def _extract_code(self, response: str) -> str:
        """
        Extract Python code from LLM response

        Handles various response formats:
        - Plain code
        - Markdown code blocks (```python ... ```)
        - Mixed text and code
        """
        # Remove markdown code blocks if present
        if '```python' in response:
            code = response.split('```python')[1].split('```')[0].strip()
        elif '```' in response:
            code = response.split('```')[1].split('```')[0].strip()
        else:
            code = response.strip()

        return code

    def _extract_function_name(self, code: str) -> str:
        """
        Extract function name from Python code

        Args:
            code: Python function code

        Returns:
            Function name or 'generated_function' if not found
        """
        match = re.search(r'def\s+(\w+)\s*\(', code)
        return match.group(1) if match else 'generated_function'

    def _validate_code(self, code: str) -> tuple[bool, str]:
        """
        Validate generated code for syntax and security

        Args:
            code: Python function code to validate

        Returns:
            (is_valid, error_message)
        """
        # Check if code starts with 'def'
        if 'def' not in code.strip():
            return False, "Code must contain a function definition"

        # Check syntax
        try:
            compile(code, '<string>', 'exec')
        except SyntaxError as e:
            return False, f"Syntax error: {str(e)}"

        # Check for potentially dangerous operations
        dangerous_patterns = [
            'eval(', 'exec(', '__import__', 'os.system(',
            'subprocess.', 'open(', 'file(', 'compile(',
            'globals()', 'locals()', 'vars()'
        ]

        code_lower = code.lower()
        for pattern in dangerous_patterns:
            if pattern.lower() in code_lower:
                return False, f"Potentially unsafe operation detected: {pattern}"

        # Check for function definition
        if not re.search(r'def\s+\w+\s*\(', code):
            return False, "No valid function definition found"

        return True, "Code is valid"


# Utility functions for testing and debugging

def test_generator():
    """Test the AI function generator with sample descriptions"""

    test_cases = [
        {
            "description": "Read CSV file, filter active records, calculate average",
            "context": "airflow"
        },
        {
            "description": "Call REST API, parse JSON response, return user count",
            "context": "airflow"
        },
        {
            "description": "Get data from XCom, multiply by 2, push result",
            "context": "airflow"
        }
    ]

    generator = AIFunctionGenerator()

    for i, test in enumerate(test_cases):
        print(f"\n{'=' * 70}")
        print(f"Test Case {i + 1}: {test['description']}")
        print('=' * 70)

        result = generator.generate_function(
            test['description'],
            test['context']
        )

        if result['success']:
            print(f"✓ Success! Function name: {result['function_name']}")
            print("\nGenerated Code:")
            print(result['function_code'])
        else:
            print(f"✗ Failed: {result['error']}")


if __name__ == "__main__":
    # Run tests if executed directly
    test_generator()
