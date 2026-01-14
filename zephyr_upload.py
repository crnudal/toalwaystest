#!/usr/bin/env python3
"""
Zephyr Scale Test Upload Script

This script uploads test cases and test execution results to Zephyr Scale (Jira).
Supports both Zephyr Scale Cloud and Server/Data Center versions.
"""

import os
import sys
import json
import requests
import argparse
from typing import Dict, List, Optional
from datetime import datetime


class ZephyrUploader:
    """Upload test cases and results to Zephyr Scale."""

    def __init__(self,
                 base_url: str,
                 api_token: str,
                 project_key: str,
                 is_cloud: bool = True):
        """
        Initialize Zephyr uploader.

        Args:
            base_url: Jira base URL (e.g., https://your-domain.atlassian.net)
            api_token: Zephyr Scale API token
            project_key: Jira project key (e.g., 'PROJ')
            is_cloud: True for Cloud version, False for Server/DC
        """
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.project_key = project_key
        self.is_cloud = is_cloud

        # Set API endpoints based on version
        if is_cloud:
            self.api_base = "https://api.zephyrscale.smartbear.com/v2"
        else:
            self.api_base = f"{self.base_url}/rest/atm/1.0"

        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_token}' if is_cloud else f'Bearer {api_token}',
            'Content-Type': 'application/json'
        })

    def create_test_case(self,
                         name: str,
                         objective: str = "",
                         precondition: str = "",
                         steps: List[Dict] = None,
                         priority: str = "Normal",
                         status: str = "Draft",
                         labels: List[str] = None,
                         folder: str = None) -> Optional[Dict]:
        """
        Create a test case in Zephyr Scale.

        Args:
            name: Test case name
            objective: Test objective/description
            precondition: Test preconditions
            steps: List of test steps [{"description": "step", "expectedResult": "result"}]
            priority: Priority level (Critical, High, Normal, Low)
            status: Test case status (Draft, Approved, Deprecated)
            labels: List of labels/tags
            folder: Folder path for organization

        Returns:
            Created test case data or None if failed
        """
        endpoint = f"{self.api_base}/testcases"

        payload = {
            "projectKey": self.project_key,
            "name": name,
            "objective": objective,
            "precondition": precondition,
            "priority": priority,
            "status": status
        }

        if steps:
            payload["testScript"] = {
                "type": "STEP_BY_STEP",
                "steps": steps
            }

        if labels:
            payload["labels"] = labels

        if folder:
            payload["folder"] = folder

        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            print(f"✓ Created test case: {name}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to create test case '{name}': {e}")
            if hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text}")
            return None

    def create_test_cycle(self,
                         name: str,
                         description: str = "",
                         folder: str = None,
                         jira_version: str = None) -> Optional[Dict]:
        """
        Create a test cycle for organizing test executions.

        Args:
            name: Cycle name
            description: Cycle description
            folder: Folder path
            jira_version: Jira version/release

        Returns:
            Created test cycle data or None if failed
        """
        endpoint = f"{self.api_base}/testcycles"

        payload = {
            "projectKey": self.project_key,
            "name": name,
            "description": description
        }

        if folder:
            payload["folder"] = folder

        if jira_version:
            payload["jiraProjectVersion"] = jira_version

        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            print(f"✓ Created test cycle: {name}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to create test cycle '{name}': {e}")
            if hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text}")
            return None

    def create_test_execution(self,
                            test_case_key: str,
                            test_cycle_key: str,
                            status: str,
                            comment: str = "",
                            execution_time: int = None,
                            executed_by: str = None,
                            environment: str = None,
                            actual_result: str = "") -> Optional[Dict]:
        """
        Create a test execution result.

        Args:
            test_case_key: Test case key (e.g., 'PROJ-T123')
            test_cycle_key: Test cycle key
            status: Execution status (Pass, Fail, Blocked, Not Executed)
            comment: Execution comment
            execution_time: Execution time in milliseconds
            executed_by: User who executed the test
            environment: Test environment
            actual_result: Actual result description

        Returns:
            Created test execution data or None if failed
        """
        endpoint = f"{self.api_base}/testexecutions"

        payload = {
            "projectKey": self.project_key,
            "testCaseKey": test_case_key,
            "testCycleKey": test_cycle_key,
            "statusName": status,
            "comment": comment
        }

        if execution_time:
            payload["executionTime"] = execution_time

        if executed_by:
            payload["executedById"] = executed_by

        if environment:
            payload["environmentName"] = environment

        if actual_result:
            payload["actualResult"] = actual_result

        try:
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            print(f"✓ Created test execution for {test_case_key}: {status}")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to create test execution for '{test_case_key}': {e}")
            if hasattr(e.response, 'text'):
                print(f"  Response: {e.response.text}")
            return None

    def upload_pytest_results(self, junit_xml_path: str, test_cycle_name: str = None) -> bool:
        """
        Parse pytest JUnit XML results and upload to Zephyr.

        Args:
            junit_xml_path: Path to JUnit XML results file
            test_cycle_name: Name for the test cycle (defaults to timestamp)

        Returns:
            True if successful, False otherwise
        """
        try:
            import xml.etree.ElementTree as ET
        except ImportError:
            print("✗ xml.etree.ElementTree not available")
            return False

        try:
            tree = ET.parse(junit_xml_path)
            root = tree.getroot()
        except Exception as e:
            print(f"✗ Failed to parse JUnit XML: {e}")
            return False

        # Create test cycle
        if not test_cycle_name:
            test_cycle_name = f"Automated Test Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        cycle = self.create_test_cycle(test_cycle_name)
        if not cycle:
            return False

        cycle_key = cycle.get('key')

        # Process test cases
        success_count = 0
        fail_count = 0

        for testcase in root.iter('testcase'):
            test_name = testcase.get('name')
            classname = testcase.get('classname', '')
            time_taken = float(testcase.get('time', 0)) * 1000  # Convert to ms

            # Determine status
            if testcase.find('failure') is not None:
                status = "Fail"
                failure_elem = testcase.find('failure')
                comment = failure_elem.get('message', '') if failure_elem is not None else ''
                fail_count += 1
            elif testcase.find('skipped') is not None:
                status = "Not Executed"
                comment = "Test skipped"
                fail_count += 1
            else:
                status = "Pass"
                comment = "Test passed successfully"
                success_count += 1

            # Create or find test case
            full_name = f"{classname}.{test_name}" if classname else test_name
            test_case = self.create_test_case(
                name=full_name,
                objective=f"Automated test: {test_name}",
                labels=["automated", "pytest"]
            )

            if test_case:
                test_case_key = test_case.get('key')
                # Create execution
                self.create_test_execution(
                    test_case_key=test_case_key,
                    test_cycle_key=cycle_key,
                    status=status,
                    comment=comment,
                    execution_time=int(time_taken)
                )

        print(f"\n{'='*60}")
        print(f"Upload Summary:")
        print(f"  Test Cycle: {test_cycle_name}")
        print(f"  Passed: {success_count}")
        print(f"  Failed: {fail_count}")
        print(f"  Total: {success_count + fail_count}")
        print(f"{'='*60}")

        return True

    def upload_json_results(self, json_path: str) -> bool:
        """
        Upload test results from a JSON file.

        Expected JSON format:
        {
            "cycle_name": "Test Cycle Name",
            "test_cases": [
                {
                    "name": "Test Case Name",
                    "status": "Pass/Fail/Blocked",
                    "comment": "Test comment",
                    "execution_time": 1000,
                    "steps": [
                        {"description": "Step 1", "expectedResult": "Result 1"}
                    ]
                }
            ]
        }

        Args:
            json_path: Path to JSON results file

        Returns:
            True if successful, False otherwise
        """
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
        except Exception as e:
            print(f"✗ Failed to read JSON file: {e}")
            return False

        cycle_name = data.get('cycle_name', f"Test Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        test_cases = data.get('test_cases', [])

        if not test_cases:
            print("✗ No test cases found in JSON file")
            return False

        # Create test cycle
        cycle = self.create_test_cycle(cycle_name)
        if not cycle:
            return False

        cycle_key = cycle.get('key')

        # Upload test cases and executions
        success_count = 0
        for tc in test_cases:
            test_case = self.create_test_case(
                name=tc.get('name'),
                objective=tc.get('objective', ''),
                steps=tc.get('steps'),
                labels=tc.get('labels', ['manual'])
            )

            if test_case:
                test_case_key = test_case.get('key')
                execution = self.create_test_execution(
                    test_case_key=test_case_key,
                    test_cycle_key=cycle_key,
                    status=tc.get('status', 'Not Executed'),
                    comment=tc.get('comment', ''),
                    execution_time=tc.get('execution_time'),
                    environment=tc.get('environment')
                )
                if execution:
                    success_count += 1

        print(f"\n{'='*60}")
        print(f"Upload Summary:")
        print(f"  Test Cycle: {cycle_name}")
        print(f"  Successful: {success_count}/{len(test_cases)}")
        print(f"{'='*60}")

        return True


def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(
        description='Upload test cases and results to Zephyr Scale',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload pytest JUnit XML results
  python zephyr_upload.py --junit results.xml

  # Upload custom JSON results
  python zephyr_upload.py --json results.json

  # Create a single test case
  python zephyr_upload.py --create-test "My Test" --objective "Test description"

Environment Variables:
  ZEPHYR_BASE_URL     - Jira base URL
  ZEPHYR_API_TOKEN    - Zephyr Scale API token
  ZEPHYR_PROJECT_KEY  - Jira project key
  ZEPHYR_IS_CLOUD     - Set to 'true' for Cloud, 'false' for Server/DC
        """
    )

    parser.add_argument('--base-url',
                       default=os.getenv('ZEPHYR_BASE_URL'),
                       help='Jira base URL (or set ZEPHYR_BASE_URL env var)')
    parser.add_argument('--api-token',
                       default=os.getenv('ZEPHYR_API_TOKEN'),
                       help='Zephyr API token (or set ZEPHYR_API_TOKEN env var)')
    parser.add_argument('--project-key',
                       default=os.getenv('ZEPHYR_PROJECT_KEY'),
                       help='Jira project key (or set ZEPHYR_PROJECT_KEY env var)')
    parser.add_argument('--cloud',
                       action='store_true',
                       default=os.getenv('ZEPHYR_IS_CLOUD', 'true').lower() == 'true',
                       help='Use Zephyr Scale Cloud API')

    parser.add_argument('--junit',
                       help='Path to pytest JUnit XML results file')
    parser.add_argument('--json',
                       help='Path to JSON results file')
    parser.add_argument('--cycle-name',
                       help='Test cycle name (optional)')

    parser.add_argument('--create-test',
                       help='Create a single test case with this name')
    parser.add_argument('--objective',
                       help='Test case objective (used with --create-test)')

    args = parser.parse_args()

    # Validate required parameters
    if not args.base_url:
        print("✗ Error: Base URL required (use --base-url or ZEPHYR_BASE_URL env var)")
        sys.exit(1)

    if not args.api_token:
        print("✗ Error: API token required (use --api-token or ZEPHYR_API_TOKEN env var)")
        sys.exit(1)

    if not args.project_key:
        print("✗ Error: Project key required (use --project-key or ZEPHYR_PROJECT_KEY env var)")
        sys.exit(1)

    # Initialize uploader
    uploader = ZephyrUploader(
        base_url=args.base_url,
        api_token=args.api_token,
        project_key=args.project_key,
        is_cloud=args.cloud
    )

    # Execute requested operation
    if args.junit:
        success = uploader.upload_pytest_results(args.junit, args.cycle_name)
        sys.exit(0 if success else 1)

    elif args.json:
        success = uploader.upload_json_results(args.json)
        sys.exit(0 if success else 1)

    elif args.create_test:
        test_case = uploader.create_test_case(
            name=args.create_test,
            objective=args.objective or ""
        )
        sys.exit(0 if test_case else 1)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
