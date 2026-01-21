# test_timestamp_validation.py
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from conftest import query_impala
from my_config_module import ImpalaConfig
import pytest


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
        lambda s: s.str.match(r'^\d{8}-\d{2}:\d{2}:\d{2}:\d{9}$').all(),
        error="Timestamp must be in format YYYYMMDD-HH:MM:SS:NNNNNNNNN"
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
        lambda s: s.str.split('-').str[1].str.split(':').str[2].astype(int).between(0, 59).all(),
        error="Seconds must be between 0-59"
    ),
    Check(
        lambda s: s.str.split(':').str[3].str.len().eq(9).all(),
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


def convert_timestamp_to_nanoseconds(timestamp_str: str) -> int:
    """
    Convert timestamp string to nanoseconds since epoch.
    Format: YYYYMMDD-HH:MM:SS:NNNNNNNNN
    """
    date_part, time_part = timestamp_str.split('-')
    time_components = time_part.split(':')
    
    year = int(date_part[:4])
    month = int(date_part[4:6])
    day = int(date_part[6:8])
    hour = int(time_components[0])
    minute = int(time_components[1])
    second = int(time_components[2])
    nanosecond = int(time_components[3])
    
    dt = datetime(year, month, day, hour, minute, second)
    epoch = datetime(1970, 1, 1)
    
    # Calculate total nanoseconds since epoch
    seconds_since_epoch = (dt - epoch).total_seconds()
    nanoseconds_since_epoch = int(seconds_since_epoch * 1_000_000_000) + nanosecond
    
    return nanoseconds_since_epoch


def validate_timestamp_with_schema(df: pd.DataFrame) -> dict:
    """
    Validate DataFrame using Pandera schema and check timesequence granularity.
    
    Args:
        df: DataFrame with timestamp and timesequence columns
        
    Returns:
        dict with validation results
    """
    results = {
        'schema_validation_passed': False,
        'schema_errors': None,
        'granularity_validation_passed': False,
        'total_rows': len(df),
        'valid_rows': 0,
        'invalid_rows': 0,
        'validation_details': [],
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # 1. Validate using Pandera schema
    try:
        validated_df = timestamp_schema.validate(df, lazy=True)
        results['schema_validation_passed'] = True
    except pa.errors.SchemaErrors as e:
        results['schema_errors'] = e.failure_cases
        validated_df = df
    
    # 2. Validate timesequence nanosecond granularity for timestamp column
    for idx, row in df.iterrows():
        timestamp = row['timestamp']
        timesequence = row['timesequence']
        
        try:
            # Convert timestamp to nanoseconds
            expected_ns = convert_timestamp_to_nanoseconds(timestamp)
            
            # Calculate difference
            difference = abs(timesequence - expected_ns)
            
            # Check if difference is within nanosecond precision
            is_valid = difference < 1000  # Allow up to 1 microsecond difference
            
            results['validation_details'].append({
                'row_index': idx,
                'timestamp': timestamp,
                'timesequence': timesequence,
                'expected_nanoseconds': expected_ns,
                'difference_ns': difference,
                'is_valid': is_valid
            })
            
            if is_valid:
                results['valid_rows'] += 1
            else:
                results['invalid_rows'] += 1
                
        except Exception as e:
            results['validation_details'].append({
                'row_index': idx,
                'timestamp': timestamp,
                'timesequence': timesequence,
                'error': str(e),
                'is_valid': False
            })
            results['invalid_rows'] += 1
    
    # Check if all rows passed granularity validation
    results['granularity_validation_passed'] = (results['invalid_rows'] == 0)
    
    return results


def save_validation_report(results: dict, df: pd.DataFrame, test_name: str) -> tuple:
    """
    Save validation results and query data to CSV files with timestamp.
    
    Args:
        results: Validation results dictionary
        df: Original DataFrame from query
        test_name: Name of the test
        
    Returns:
        Tuple of (summary_filename, details_filename, data_filename)
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Save the original query data
    data_filename = f"{test_name}_query_data_{timestamp}.csv"
    df.to_csv(data_filename, index=False)
    print(f"\nQuery data saved to: {data_filename}")
    
    # 2. Create summary report
    summary_df = pd.DataFrame({
        'test_name': [test_name],
        'total_rows': [results['total_rows']],
        'schema_validation_passed': [results['schema_validation_passed']],
        'granularity_validation_passed': [results['granularity_validation_passed']],
        'valid_rows': [results['valid_rows']],
        'invalid_rows': [results['invalid_rows']],
        'validation_rate': [f"{(results['valid_rows']/results['total_rows'])*100:.2f}%"],
        'schema_errors': [str(results['schema_errors']) if results['schema_errors'] is not None else 'None'],
        'query_data_file': [data_filename],  # Link to query data file
        'timestamp': [results['timestamp']]
    })
    
    summary_filename = f"{test_name}_summary_{timestamp}.csv"
    summary_df.to_csv(summary_filename, index=False)
    print(f"Summary report saved to: {summary_filename}")
    
    # 3. Create detailed validation report
    details_filename = None
    if results['validation_details']:
        details_df = pd.DataFrame(results['validation_details'])
        details_filename = f"{test_name}_details_{timestamp}.csv"
        details_df.to_csv(details_filename, index=False)
        print(f"Detailed report saved to: {details_filename}")
    
    return summary_filename, details_filename, data_filename


def test_timestamp_validation(timestamp_dataframe):
    """
    Test timestamp format and timesequence nanosecond granularity validation.
    
    Validates:
    1. Timestamp format matches YYYYMMDD-HH:MM:SS:NNNNNNNNN for timestamp and timestamp_1
    2. All time components are valid (hours, minutes, seconds, nanoseconds)
    3. Timesequence has nanosecond granularity
    
    Generates 3 CSV reports:
    - Summary report with validation results
    - Detailed report with row-by-row validation
    - Query data export for traceability
    """
    # Run validation
    validation_results = validate_timestamp_with_schema(timestamp_dataframe)
    
    # Save validation reports and query data
    summary_file, details_file, data_file = save_validation_report(
        validation_results, 
        timestamp_dataframe, 
        'test_timestamp_validation'
    )
    
    # Assert validations passed
    assert validation_results['schema_validation_passed'], \
        f"Schema validation failed. See reports: {summary_file}, {data_file}"
    
    assert validation_results['granularity_validation_passed'], \
        f"Timesequence granularity validation failed. {validation_results['invalid_rows']} rows invalid. See reports: {summary_file}, {details_file}, {data_file}"
    
    print(f"\n✓ All validations passed!")
    print(f"  - Total rows validated: {validation_results['total_rows']}")
    print(f"  - Schema validation: PASSED")
    print(f"  - Granularity validation: PASSED")
    print(f"\nReports generated:")
    print(f"  - Summary: {summary_file}")
    print(f"  - Details: {details_file}")
    print(f"  - Query Data: {data_file}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
