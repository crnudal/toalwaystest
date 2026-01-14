# Zephyr Scale Test Upload Script

This script allows you to upload test cases and test execution results to Zephyr Scale (Jira Test Management).

## Features

- ✅ Upload pytest JUnit XML results automatically
- ✅ Upload custom test results from JSON files
- ✅ Create individual test cases programmatically
- ✅ Create test cycles for organizing executions
- ✅ Support for both Zephyr Scale Cloud and Server/Data Center
- ✅ Detailed test steps and execution results
- ✅ Environment and label tagging

## Prerequisites

1. **Zephyr Scale** subscription (Cloud or Server/Data Center)
2. **Jira** project with Zephyr Scale enabled
3. **API Token** for authentication

## Installation

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the example environment file and fill in your details:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
ZEPHYR_BASE_URL=https://your-domain.atlassian.net
ZEPHYR_API_TOKEN=your_api_token_here
ZEPHYR_PROJECT_KEY=PROJ
ZEPHYR_IS_CLOUD=true
```

### 3. Get Your Zephyr API Token

#### For Zephyr Scale Cloud:
1. Go to https://smartbear.com/
2. Log in with your Atlassian account
3. Navigate to API Access Tokens
4. Generate a new token for your Jira site

#### For Zephyr Scale Server/Data Center:
1. Log into your Jira instance
2. Go to your Profile → Personal Access Tokens
3. Generate a new token with appropriate permissions

## Usage

### Method 1: Upload pytest Results (Automated)

Run your pytest tests with JUnit XML output:

```bash
pytest --junitxml=results.xml
```

Then upload to Zephyr:

```bash
python zephyr_upload.py --junit results.xml --cycle-name "Sprint 23 Regression"
```

### Method 2: Upload Custom JSON Results

Create a JSON file with your test results (see `example_test_results.json`):

```bash
python zephyr_upload.py --json example_test_results.json
```

### Method 3: Create Individual Test Cases

```bash
python zephyr_upload.py \
  --create-test "Test User Registration" \
  --objective "Verify new users can register successfully"
```

### Method 4: Use as a Python Module

```python
from zephyr_upload import ZephyrUploader

# Initialize
uploader = ZephyrUploader(
    base_url="https://your-domain.atlassian.net",
    api_token="your_token",
    project_key="PROJ",
    is_cloud=True
)

# Create a test case
test_case = uploader.create_test_case(
    name="Test Login Functionality",
    objective="Verify user can login successfully",
    steps=[
        {
            "description": "Open login page",
            "expectedResult": "Login form is displayed"
        },
        {
            "description": "Enter credentials and submit",
            "expectedResult": "User is logged in"
        }
    ],
    priority="High",
    labels=["authentication", "critical"]
)

# Create a test cycle
cycle = uploader.create_test_cycle(
    name="Sprint 23 Tests",
    description="Regression tests for Sprint 23"
)

# Create test execution
if test_case and cycle:
    uploader.create_test_execution(
        test_case_key=test_case['key'],
        test_cycle_key=cycle['key'],
        status="Pass",
        comment="All steps passed successfully",
        execution_time=2000,
        environment="QA"
    )
```

## JSON Format for Custom Results

```json
{
  "cycle_name": "Test Cycle Name",
  "test_cases": [
    {
      "name": "Test Case Name",
      "objective": "What this test verifies",
      "status": "Pass",
      "comment": "Execution notes",
      "execution_time": 1500,
      "environment": "QA",
      "labels": ["tag1", "tag2"],
      "steps": [
        {
          "description": "Step description",
          "expectedResult": "Expected outcome"
        }
      ]
    }
  ]
}
```

## Status Values

- `Pass` - Test passed
- `Fail` - Test failed
- `Blocked` - Test blocked
- `Not Executed` - Test not executed

## Priority Values

- `Critical`
- `High`
- `Normal`
- `Low`

## Command Line Options

```
Options:
  --base-url TEXT       Jira base URL
  --api-token TEXT      Zephyr API token
  --project-key TEXT    Jira project key
  --cloud              Use Zephyr Scale Cloud API
  --junit TEXT         Path to pytest JUnit XML results
  --json TEXT          Path to JSON results file
  --cycle-name TEXT    Test cycle name
  --create-test TEXT   Create a single test case
  --objective TEXT     Test case objective
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Run Tests and Upload to Zephyr

on: [push]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt

      - name: Run tests
        run: pytest --junitxml=results.xml

      - name: Upload to Zephyr
        env:
          ZEPHYR_BASE_URL: ${{ secrets.ZEPHYR_BASE_URL }}
          ZEPHYR_API_TOKEN: ${{ secrets.ZEPHYR_API_TOKEN }}
          ZEPHYR_PROJECT_KEY: ${{ secrets.ZEPHYR_PROJECT_KEY }}
        run: |
          python zephyr_upload.py --junit results.xml --cycle-name "CI Build ${{ github.run_number }}"
```

### GitLab CI Example

```yaml
test_and_upload:
  stage: test
  script:
    - pip install -r requirements.txt
    - pytest --junitxml=results.xml
    - python zephyr_upload.py --junit results.xml --cycle-name "Pipeline $CI_PIPELINE_ID"
  variables:
    ZEPHYR_BASE_URL: $ZEPHYR_BASE_URL
    ZEPHYR_API_TOKEN: $ZEPHYR_API_TOKEN
    ZEPHYR_PROJECT_KEY: $ZEPHYR_PROJECT_KEY
```

## Troubleshooting

### Authentication Errors

- Verify your API token is correct and not expired
- Check that your token has permissions for the project
- Ensure your base URL is correct (no trailing slash)

### Test Case Creation Fails

- Verify the project key exists in Jira
- Check that Zephyr Scale is enabled for your project
- Ensure you have "Create Test Cases" permission

### Rate Limiting

- Zephyr Scale Cloud has rate limits (varies by plan)
- Add delays between bulk operations if needed
- Consider batching operations

## Advanced Features

### Custom Folder Structure

```python
# Create test in specific folder
uploader.create_test_case(
    name="My Test",
    folder="/Automation/API Tests"
)
```

### Link to Jira Version

```python
# Associate cycle with Jira version
uploader.create_test_cycle(
    name="Release 2.0 Tests",
    jira_version="2.0"
)
```

## Support

- Zephyr Scale Documentation: https://support.smartbear.com/zephyr-scale/
- Zephyr Scale API Docs: https://support.smartbear.com/zephyr-scale/api-docs/

## License

This script is provided as-is for use with Zephyr Scale.
