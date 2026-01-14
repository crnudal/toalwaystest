#!/usr/bin/env python3
"""
Script to create Zephyr tests in a corporate Jira project.
Supports Zephyr Scale (formerly Zephyr Squad) API.
"""

import os
import sys
import json
import requests
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class ZephyrTestConfig:
    """Configuration for Zephyr test creation."""
    jira_url: str
    jira_project_key: str
    username: str
    api_token: str
    zephyr_api_url: str = None  # Optional, defaults to Zephyr Scale Cloud API

    def __post_init__(self):
        """Set default Zephyr API URL if not provided."""
        if not self.zephyr_api_url:
            # Default to Zephyr Scale Cloud API
            self.zephyr_api_url = "https://api.zephyrscale.smartbear.com/v2"


class ZephyrTestCreator:
    """
    Creates Zephyr tests in a corporate Jira project.
    Supports both Zephyr Scale Cloud and Server/Data Center versions.
    """

    def __init__(self, config: ZephyrTestConfig):
        """Initialize the Zephyr test creator with configuration."""
        self.config = config
        self.session = requests.Session()
        self.session.auth = (config.username, config.api_token)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def create_test_case(
        self,
        name: str,
        objective: str = "",
        precondition: str = "",
        test_steps: List[Dict[str, str]] = None,
        priority: str = "Medium",
        status: str = "Draft",
        labels: List[str] = None,
        folder: str = None,
        custom_fields: Dict = None
    ) -> Dict:
        """
        Create a Zephyr test case in the specified Jira project.

        Args:
            name: Test case name/summary
            objective: Test objective/description
            precondition: Test precondition
            test_steps: List of test steps with description and expected result
            priority: Test priority (Critical, High, Medium, Low)
            status: Test status (Draft, Approved, Deprecated, etc.)
            labels: List of labels to add to the test
            folder: Folder path to organize the test
            custom_fields: Dictionary of custom field values

        Returns:
            Dictionary containing the created test case details
        """
        if test_steps is None:
            test_steps = []
        if labels is None:
            labels = []
        if custom_fields is None:
            custom_fields = {}

        # Prepare test case payload for Zephyr Scale
        payload = {
            "projectKey": self.config.jira_project_key,
            "name": name,
            "objective": objective,
            "precondition": precondition,
            "priority": priority,
            "status": status,
            "labels": labels
        }

        # Add folder if specified
        if folder:
            payload["folder"] = folder

        # Add test steps if provided
        if test_steps:
            payload["testScript"] = {
                "type": "STEP_BY_STEP",
                "steps": [
                    {
                        "description": step.get("description", ""),
                        "testData": step.get("testData", ""),
                        "expectedResult": step.get("expectedResult", "")
                    }
                    for step in test_steps
                ]
            }

        # Add custom fields
        if custom_fields:
            payload["customFields"] = custom_fields

        # Create the test case via Zephyr Scale API
        url = f"{self.config.zephyr_api_url}/testcases"

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()

            test_case = response.json()
            print(f"✓ Successfully created test case: {name}")
            print(f"  Test Case Key: {test_case.get('key', 'N/A')}")
            print(f"  Test Case ID: {test_case.get('id', 'N/A')}")

            return test_case

        except requests.exceptions.HTTPError as e:
            print(f"✗ Failed to create test case: {name}")
            print(f"  Error: {e}")
            if e.response.text:
                print(f"  Response: {e.response.text}")
            raise

    def create_test_cases_bulk(
        self,
        test_cases: List[Dict]
    ) -> List[Dict]:
        """
        Create multiple test cases in bulk.

        Args:
            test_cases: List of test case configurations

        Returns:
            List of created test case details
        """
        created_tests = []

        for i, test_config in enumerate(test_cases, 1):
            print(f"\nCreating test case {i}/{len(test_cases)}...")
            try:
                test_case = self.create_test_case(**test_config)
                created_tests.append(test_case)
            except Exception as e:
                print(f"Failed to create test case {i}: {e}")
                continue

        return created_tests

    def get_project_folders(self) -> List[Dict]:
        """
        Get all folders in the project for organizing tests.

        Returns:
            List of folder structures
        """
        url = f"{self.config.zephyr_api_url}/folders"
        params = {"projectKey": self.config.jira_project_key}

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json().get('values', [])
        except requests.exceptions.HTTPError as e:
            print(f"Failed to get folders: {e}")
            return []

    def create_folder(self, name: str, parent_id: Optional[int] = None) -> Dict:
        """
        Create a folder to organize test cases.

        Args:
            name: Folder name
            parent_id: Parent folder ID (optional, for nested folders)

        Returns:
            Created folder details
        """
        payload = {
            "projectKey": self.config.jira_project_key,
            "name": name,
            "type": "TEST_CASE"
        }

        if parent_id:
            payload["parentId"] = parent_id

        url = f"{self.config.zephyr_api_url}/folders"

        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()

            folder = response.json()
            print(f"✓ Successfully created folder: {name}")
            print(f"  Folder ID: {folder.get('id', 'N/A')}")

            return folder

        except requests.exceptions.HTTPError as e:
            print(f"✗ Failed to create folder: {name}")
            print(f"  Error: {e}")
            raise


def load_config_from_env() -> ZephyrTestConfig:
    """
    Load configuration from environment variables.

    Expected environment variables:
    - JIRA_URL: Corporate Jira instance URL
    - JIRA_PROJECT_KEY: Project key (e.g., 'CORP')
    - JIRA_USERNAME: Jira username/email
    - JIRA_API_TOKEN: Jira API token
    - ZEPHYR_API_URL: (Optional) Zephyr API URL
    """
    required_vars = ['JIRA_URL', 'JIRA_PROJECT_KEY', 'JIRA_USERNAME', 'JIRA_API_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    return ZephyrTestConfig(
        jira_url=os.getenv('JIRA_URL'),
        jira_project_key=os.getenv('JIRA_PROJECT_KEY'),
        username=os.getenv('JIRA_USERNAME'),
        api_token=os.getenv('JIRA_API_TOKEN'),
        zephyr_api_url=os.getenv('ZEPHYR_API_URL')
    )


def example_usage():
    """Example usage of the Zephyr test creator."""

    # Load configuration from environment
    try:
        config = load_config_from_env()
    except ValueError as e:
        print(f"Configuration error: {e}")
        print("\nPlease set the following environment variables:")
        print("  export JIRA_URL='https://your-company.atlassian.net'")
        print("  export JIRA_PROJECT_KEY='CORP'")
        print("  export JIRA_USERNAME='your.email@company.com'")
        print("  export JIRA_API_TOKEN='your_api_token'")
        print("  export ZEPHYR_API_URL='https://api.zephyrscale.smartbear.com/v2'  # Optional")
        return

    # Initialize the creator
    creator = ZephyrTestCreator(config)

    # Example 1: Create a simple test case
    print("\n=== Creating a simple test case ===")
    test_case_1 = creator.create_test_case(
        name="Login functionality - Valid credentials",
        objective="Verify that users can successfully login with valid credentials",
        precondition="User account exists in the system",
        test_steps=[
            {
                "description": "Navigate to login page",
                "testData": "URL: https://app.company.com/login",
                "expectedResult": "Login page is displayed"
            },
            {
                "description": "Enter valid username and password",
                "testData": "Username: testuser@company.com, Password: ValidPass123!",
                "expectedResult": "Credentials are entered successfully"
            },
            {
                "description": "Click Login button",
                "testData": "",
                "expectedResult": "User is redirected to dashboard and logged in successfully"
            }
        ],
        priority="High",
        status="Approved",
        labels=["authentication", "smoke-test"]
    )

    # Example 2: Create multiple test cases in bulk
    print("\n=== Creating test cases in bulk ===")
    test_cases = [
        {
            "name": "Login functionality - Invalid password",
            "objective": "Verify error handling for invalid password",
            "precondition": "User account exists",
            "test_steps": [
                {
                    "description": "Navigate to login page and enter valid username with invalid password",
                    "testData": "Username: testuser@company.com, Password: WrongPass",
                    "expectedResult": "Error message displayed: 'Invalid credentials'"
                }
            ],
            "priority": "High",
            "labels": ["authentication", "negative-test"]
        },
        {
            "name": "User profile update - Email change",
            "objective": "Verify that users can update their email address",
            "precondition": "User is logged in",
            "test_steps": [
                {
                    "description": "Navigate to profile settings",
                    "expectedResult": "Profile page is displayed"
                },
                {
                    "description": "Update email address and save",
                    "testData": "New email: newemail@company.com",
                    "expectedResult": "Email updated successfully with confirmation message"
                }
            ],
            "priority": "Medium",
            "labels": ["user-profile"]
        }
    ]

    created_tests = creator.create_test_cases_bulk(test_cases)

    print(f"\n=== Summary ===")
    print(f"Total test cases created: {len(created_tests)}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Zephyr Test Case Creator for Corporate Jira")
    print("=" * 60)

    example_usage()


if __name__ == "__main__":
    main()
