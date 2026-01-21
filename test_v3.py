# test_timestamp_validation.py
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from conftest import query_impala
from my_config_module import ImpalaConfig
import pytest
import os
from pathlib import Path


# Get test name from filename (e.g., test_timestamp_validation.py -> timestamp_validation)
TEST_FILE = Path(__file__).stem  # Gets filename without extension
TEST_NAME = TEST_FILE.replace('test_', '')  # Remove 'test_' prefix
REPORTS_FOLDER = TEST_NAME  # Folder name will be 'timestamp_validation'


# Query definition
TIMESTAMP_QUERY = """
    SELECT 
        timestamp,
        timestamp_1,
        timesequence
    FROM my_table 
    WHERE id < 1000
"""


# Define timestamp checks once for reuse
timestamp_checks = [
    Check(
        lambda s: s.str.match(r'^\d{8}-\d{2}:\d{2}:\d{2}\.\d{9}$').all(),
        error="Timestamp must be in format YYYYMMDD-HH:MM:SS.NNNNNNNNN"
    ),
    Check(
        lambda s: s.str[:8].apply(
            lambda x: pd.to_datetime(x, format='%Y%m%d', errors='coerce')
        ).notna().all(),
        error="Date part must be valid"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[0].astype(int).between(0, 23).all(),
        error="Hours must be between 0-23"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[1].astype(int).between(0, 59).all(),
        error="Minutes must be between 0-59"
    ),
    Check(
        lambda s: s.str.split('-').str[1].str.split(':').str[2].str.split('.').str[0].astype(int).between(0, 59).all(),
        error="Seconds must be between 0-59"
    ),
    Check(
        lambda s: s.str.split('.').str[1].str.len().eq(9).all(),
        error="Nanoseconds must have exactly 9 digits"
    ),
]


# Pandera Schema for timestamp validation
timestamp_schema = DataFrameSchema(
    {
        "timestamp": Column(str, checks=timestamp_checks, nullable=False),
        "timestamp_1": Column(str, checks=timestamp_checks, nullable=False),
        "timesequence": Column(
            pa.Int64,
            checks=[
                Check(lambda s: s.notna().all(), error="Timesequence cannot be null"),
                Check(lambda s: (s > 0).all(), error="Timesequence must be positive"),
                Check(
                    lambda s: s.astype(str).str.len().eq(19).all(),
                    error="Timesequence must have exactly 19 digits"
                ),
                Check(
                    lambda s: ~s.astype(str).str[-3:].eq('000').any(),
                    error="Timesequence last 3 digits cannot be 000"
                ),
            ],
            nullable=False
        )
    },
    strict=False,  # Allow other columns
    coerce=True
)


@pytest.fixture
def timestamp_dataframe():
    """Fixture to query and return timestamp data from Impala."""
    df = query_impala(ImpalaConfig, TIMESTAMP_QUERY)
    return df


def save_reports(df: pd.DataFrame, schema_errors: pd.DataFrame, test_name: str) -> tuple:
    """
    Save query data and Pandera validation report to CSV files with timestamp.
    
    Args:
        df: Original DataFrame from query
        schema_errors: Pandera failure_cases DataFrame
        test_name: Name of the test (from filename without 'test_' prefix)
        
    Returns:
        Tuple of (data_filename, report_filename)
    """
    # Create reports folder if it doesn't exist
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Save the original query data
    data_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_query_data_{timestamp}.csv")
    df.to_csv(data_filename, index=False)
    print(f"\nQuery data saved to: {data_filename}")
    
    # 2. Save Pandera validation report
    report_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_pandera_report_{timestamp}.csv")
    schema_errors.to_csv(report_filename, index=False)
    print(f"Pandera validation report saved to: {report_filename}")
    
    return data_filename, report_filename


def test_timestamp_validation(timestamp_dataframe):
    """
    Test timestamp format validation using Pandera schema.
    
    Validates:
    1. Timestamp format matches YYYYMMDD-HH:MM:SS.NNNNNNNNN for timestamp and timestamp_1
    2. All time components are valid (hours, minutes, seconds, nanoseconds)
    3. Timesequence is not null and positive
    4. Timesequence has exactly 19 digits
    5. Timesequence last 3 digits are not 000 (ensures nanosecond precision)
    
    Generates 2 CSV files in folder named after test file:
    - Query data export for traceability
    - Pandera validation report with failure cases
    """
    # Create reports folder if it doesn't exist
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Validate using Pandera schema
    try:
        validated_df = timestamp_schema.validate(timestamp_dataframe, lazy=True)
        validation_passed = True
        
        # If validation passes, save data and create empty report
        data_filename = os.path.join(REPORTS_FOLDER, f"{TEST_NAME}_query_data_{timestamp}.csv")
        timestamp_dataframe.to_csv(data_filename, index=False)
        
        report_filename = os.path.join(REPORTS_FOLDER, f"{TEST_NAME}_pandera_report_{timestamp}.csv")
        empty_report = pd.DataFrame({
            'schema_context': ['No validation errors'],
            'column': ['N/A'],
            'check': ['All checks passed'],
            'check_number': [0],
            'failure_case': ['N/A'],
            'index': [0]
        })
        empty_report.to_csv(report_filename, index=False)
        
        print(f"\n✓ All validations passed!")
        print(f"  - Total rows validated: {len(timestamp_dataframe)}")
        print(f"  - All schema checks: PASSED")
        print(f"\nReports generated in folder '{REPORTS_FOLDER}':")
        print(f"  - Query Data: {data_filename}")
        print(f"  - Pandera Report: {report_filename}")
        
    except pa.errors.SchemaErrors as e:
        validation_passed = False
        
        # Save query data and Pandera failure report
        data_filename, report_filename = save_reports(
            timestamp_dataframe,
            e.failure_cases,
            TEST_NAME
        )
        
        # Print summary of failures
        print(f"\n✗ Validation failed!")
        print(f"  - Total rows: {len(timestamp_dataframe)}")
        print(f"  - Failed checks: {len(e.failure_cases)}")
        print(f"\nReports generated in folder '{REPORTS_FOLDER}':")
        print(f"  - Query Data: {data_filename}")
        print(f"  - Pandera Report: {report_filename}")
        
        # Fail the test with clear message
        assert False, f"Schema validation failed. See Pandera report: {report_filename}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
