# test_routing_sequence_validation.py
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from conftest import query_impala
from my_config_module import ImpalaConfig
import pytest
import os
from pathlib import Path


# Get test name from filename
TEST_FILE = Path(__file__).stem
TEST_NAME = TEST_FILE.replace('test_', '')
REPORTS_FOLDER = TEST_NAME

# Get from environment variables with defaults
TABLE_NAME = os.getenv('TABLE_NAME', 'my_table')
WHERE_CLAUSE = os.getenv('WHERE_CLAUSE', 'id < 1000')

# Timestamp granularity: 'nanosecond' or 'microsecond'
TIMESTAMP_GRANULARITY = os.getenv('TIMESTAMP_GRANULARITY', 'nanosecond')

# Query definition - orders by routing_sequence DESC
ROUTING_QUERY = f"""
    SELECT 
        id,
        routing_sequence,
        timestamp,
        timesequence
    FROM {TABLE_NAME}
    WHERE {WHERE_CLAUSE}
    ORDER BY routing_sequence DESC
"""


def convert_timestamp_to_nanoseconds(timestamp_str: str, granularity: str = 'nanosecond') -> int:
    """
    Convert timestamp string to nanoseconds since epoch.
    
    Args:
        timestamp_str: Timestamp in format YYYYMMDD-HH:MM:SS.NNNNNNNNN (nanosecond)
                       or YYYYMMDD-HH:MM:SS.NNNNNN (microsecond)
        granularity: 'nanosecond' (9 digits) or 'microsecond' (6 digits)
    
    Returns:
        Nanoseconds since epoch as integer
    """
    try:
        date_part, time_part = timestamp_str.split('-')
        time_components = time_part.split(':')
        
        year = int(date_part[:4])
        month = int(date_part[4:6])
        day = int(date_part[6:8])
        hour = int(time_components[0])
        minute = int(time_components[1])
        second_and_subsec = time_components[2].split('.')
        second = int(second_and_subsec[0])
        
        # Keep subsecond as string to preserve leading zeros
        subsecond_str = second_and_subsec[1]
        
        if granularity == 'nanosecond':
            # Ensure it's exactly 9 digits
            subsecond_str = subsecond_str.ljust(9, '0')[:9]
            nanosecond = int(subsecond_str)
        elif granularity == 'microsecond':
            # Ensure it's exactly 6 digits, then convert to nanoseconds
            subsecond_str = subsecond_str.ljust(6, '0')[:6]
            microsecond = int(subsecond_str)
            nanosecond = microsecond * 1000  # Convert microseconds to nanoseconds
        else:
            raise ValueError(f"Unknown granularity: {granularity}")
        
        dt = datetime(year, month, day, hour, minute, second)
        epoch = datetime(1970, 1, 1)
        
        # Calculate total nanoseconds since epoch
        seconds_since_epoch = (dt - epoch).total_seconds()
        nanoseconds_since_epoch = int(seconds_since_epoch * 1_000_000_000) + nanosecond
        
        return nanoseconds_since_epoch
    except Exception as e:
        return -1  # Return invalid value if conversion fails


def validate_timestamp_ordering(df: pd.DataFrame, granularity: str = 'nanosecond') -> pd.DataFrame:
    """
    Validate that timestamps are descending (or equal) when ordered by routing_sequence DESC.
    
    Args:
        df: DataFrame ordered by routing_sequence DESC
        granularity: 'nanosecond' or 'microsecond'
    
    Returns:
        DataFrame with validation results for each row, including the id column
    """
    validation_results = []
    
    # Convert all timestamps to nanoseconds for comparison
    df['timestamp_ns'] = df['timestamp'].apply(
        lambda x: convert_timestamp_to_nanoseconds(x, granularity)
    )
    
    for i in range(len(df)):
        current_row = df.iloc[i]
        
        result = {
            'row_index': i,
            'id': current_row['id'],  # Include the id column
            'routing_sequence': current_row['routing_sequence'],
            'timestamp': current_row['timestamp'],
            'timestamp_ns': current_row['timestamp_ns'],
            'previous_id': None,
            'previous_routing_sequence': None,
            'previous_timestamp': None,
            'previous_timestamp_ns': None,
            'timestamp_difference_ns': None,
            'is_valid': True,
            'validation_message': 'First row or valid ordering'
        }
        
        # Check against previous row (if exists)
        if i > 0:
            previous_row = df.iloc[i - 1]
            result['previous_id'] = previous_row['id']
            result['previous_routing_sequence'] = previous_row['routing_sequence']
            result['previous_timestamp'] = previous_row['timestamp']
            result['previous_timestamp_ns'] = previous_row['timestamp_ns']
            result['timestamp_difference_ns'] = current_row['timestamp_ns'] - previous_row['timestamp_ns']
            
            # Since routing_sequence is DESC, timestamp should also be DESC or equal
            # This means: current_timestamp_ns <= previous_timestamp_ns
            if current_row['timestamp_ns'] > previous_row['timestamp_ns']:
                result['is_valid'] = False
                result['validation_message'] = (
                    f"DISORDER DETECTED: Timestamp increased when routing_sequence decreased. "
                    f"Previous ID={previous_row['id']} (seq={previous_row['routing_sequence']}), "
                    f"Current ID={current_row['id']} (seq={current_row['routing_sequence']}), "
                    f"Timestamp diff={result['timestamp_difference_ns']} ns"
                )
            elif current_row['timestamp_ns'] == previous_row['timestamp_ns']:
                result['validation_message'] = 'Timestamp equal to previous (valid)'
            else:
                result['validation_message'] = f'Timestamp correctly descending (diff={result["timestamp_difference_ns"]} ns)'
        
        validation_results.append(result)
    
    return pd.DataFrame(validation_results)


# Pandera schema for basic column validation
if TIMESTAMP_GRANULARITY == 'nanosecond':
    timestamp_pattern = r'^\d{8}-\d{2}:\d{2}:\d{2}\.\d{9}$'
    subsecond_length = 9
    error_msg = "Timestamp must be in format YYYYMMDD-HH:MM:SS.NNNNNNNNN (nanosecond)"
elif TIMESTAMP_GRANULARITY == 'microsecond':
    timestamp_pattern = r'^\d{8}-\d{2}:\d{2}:\d{2}\.\d{6}$'
    subsecond_length = 6
    error_msg = "Timestamp must be in format YYYYMMDD-HH:MM:SS.NNNNNN (microsecond)"
else:
    raise ValueError(f"Unknown granularity: {TIMESTAMP_GRANULARITY}")


routing_schema = DataFrameSchema(
    {
        "id": Column(
            pa.Int64,
            checks=[
                Check(lambda s: s.notna().all(), error="ID cannot be null"),
            ],
            nullable=False
        ),
        "routing_sequence": Column(
            pa.Int64,
            checks=[
                Check(lambda s: s.notna().all(), error="Routing sequence cannot be null"),
            ],
            nullable=False
        ),
        "timestamp": Column(
            str,
            checks=[
                Check(
                    lambda s: s.str.match(timestamp_pattern).all(),
                    error=error_msg
                ),
                Check(
                    lambda s: s.str.split('.').str[1].str.len().eq(subsecond_length).all(),
                    error=f"Subsecond part must have exactly {subsecond_length} digits"
                ),
            ],
            nullable=False
        ),
    },
    strict=False,
    coerce=True
)


@pytest.fixture
def routing_dataframe():
    """Fixture to query and return data ordered by routing_sequence DESC."""
    print(f"\nExecuting query:")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Where: {WHERE_CLAUSE}")
    print(f"  Timestamp Granularity: {TIMESTAMP_GRANULARITY}")
    print(f"  Ordering: routing_sequence DESC")
    
    df = query_impala(ImpalaConfig, ROUTING_QUERY)
    return df


def save_reports(
    df: pd.DataFrame, 
    validation_df: pd.DataFrame,
    schema_errors: pd.DataFrame,
    test_name: str
) -> tuple:
    """
    Save query data, validation results, and Pandera report to CSV files.
    
    Args:
        df: Original DataFrame from query
        validation_df: DataFrame with ordering validation results
        schema_errors: Pandera failure_cases DataFrame (None if no errors)
        test_name: Name of the test
        
    Returns:
        Tuple of (data_filename, validation_filename, schema_filename, disorders_filename)
    """
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. Save the original query data
    data_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_query_data_{timestamp}.csv")
    df.to_csv(data_filename, index=True)
    print(f"\nQuery data saved to: {data_filename}")
    
    # 2. Save ordering validation results
    validation_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_ordering_validation_{timestamp}.csv")
    validation_df.to_csv(validation_filename, index=False)
    print(f"Ordering validation saved to: {validation_filename}")
    
    # 3. Save ONLY the rows with disorders (is_valid=False) for easy identification
    disorders_df = validation_df[~validation_df['is_valid']]
    disorders_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_DISORDERS_ONLY_{timestamp}.csv")
    
    if len(disorders_df) > 0:
        # Reorder columns to show id first
        disorder_cols = ['id', 'previous_id', 'routing_sequence', 'previous_routing_sequence', 
                        'timestamp', 'previous_timestamp', 'timestamp_difference_ns', 
                        'validation_message', 'row_index']
        disorders_df = disorders_df[disorder_cols]
        disorders_df.to_csv(disorders_filename, index=False)
        print(f"⚠️  DISORDERS ONLY report saved to: {disorders_filename}")
        print(f"⚠️  Found {len(disorders_df)} disorder(s) at ID(s): {disorders_df['id'].tolist()}")
    else:
        # No disorders - create empty file with headers
        empty_disorders = pd.DataFrame(columns=['id', 'previous_id', 'routing_sequence', 
                                                'previous_routing_sequence', 'timestamp', 
                                                'previous_timestamp', 'timestamp_difference_ns',
                                                'validation_message', 'row_index'])
        empty_disorders.to_csv(disorders_filename, index=False)
        print(f"✓ No disorders found - empty report saved to: {disorders_filename}")
    
    # 4. Save Pandera schema validation report
    schema_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_schema_report_{timestamp}.csv")
    if schema_errors is not None:
        schema_errors.to_csv(schema_filename, index=True)
        print(f"Schema validation report saved to: {schema_filename}")
    else:
        # No schema errors
        empty_report = pd.DataFrame({
            'schema_context': ['No validation errors'],
            'column': ['N/A'],
            'check': ['All checks passed'],
            'check_number': [0],
            'failure_case': ['N/A'],
            'index': [0]
        })
        empty_report.to_csv(schema_filename, index=False)
        print(f"Schema validation report (no errors) saved to: {schema_filename}")
    
    return data_filename, validation_filename, schema_filename, disorders_filename


def test_routing_sequence_timestamp_ordering(routing_dataframe):
    """
    Test that timestamps are descending (or equal) when ordered by routing_sequence DESC.
    
    Environment Variables:
    - TABLE_NAME: Name of the table to query (default: my_table)
    - WHERE_CLAUSE: WHERE clause for filtering (default: id < 1000)
    - TIMESTAMP_GRANULARITY: 'nanosecond' or 'microsecond' (default: nanosecond)
    
    Validates:
    1. Data is ordered by routing_sequence DESC
    2. Timestamp format is correct (nanosecond or microsecond)
    3. Timestamps are descending or equal as routing_sequence decreases
    4. For each row, current_timestamp <= previous_timestamp
    
    Generates 4 CSV files:
    - Query data (ordered by routing_sequence DESC)
    - Ordering validation results (row-by-row with IDs)
    - DISORDERS ONLY report (only rows where disorder occurred with IDs)
    - Schema validation report
    """
    os.makedirs(REPORTS_FOLDER, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    print(f"\n{'='*60}")
    print(f"Routing Sequence Validation Configuration:")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Where: {WHERE_CLAUSE}")
    print(f"  Timestamp Granularity: {TIMESTAMP_GRANULARITY}")
    print(f"  Total Rows: {len(routing_dataframe)}")
    print(f"{'='*60}")
    
    # 1. Validate basic schema
    schema_errors = None
    schema_passed = True
    try:
        routing_schema.validate(routing_dataframe, lazy=True)
        print("\n✓ Schema validation passed")
    except pa.errors.SchemaErrors as e:
        schema_passed = False
        schema_errors = e.failure_cases
        print(f"\n✗ Schema validation failed: {len(e.failure_cases)} errors")
    
    # 2. Validate timestamp ordering based on routing_sequence
    print("\nValidating timestamp ordering...")
    validation_df = validate_timestamp_ordering(routing_dataframe, TIMESTAMP_GRANULARITY)
    
    invalid_count = (~validation_df['is_valid']).sum()
    valid_count = validation_df['is_valid'].sum()
    
    ordering_passed = (invalid_count == 0)
    
    if ordering_passed:
        print(f"✓ Ordering validation passed: All {valid_count} rows have correct timestamp ordering")
    else:
        print(f"✗ Ordering validation failed: {invalid_count} out of {len(validation_df)} rows have incorrect ordering")
        disorder_ids = validation_df[~validation_df['is_valid']]['id'].tolist()
        print(f"⚠️  Disorders found at ID(s): {disorder_ids}")
    
    # 3. Save all reports
    data_file, validation_file, schema_file, disorders_file = save_reports(
        routing_dataframe,
        validation_df,
        schema_errors,
        TEST_NAME
    )
    
    # 4. Print summary
    print(f"\n{'='*60}")
    print(f"Validation Summary:")
    print(f"  Schema Validation: {'PASSED' if schema_passed else 'FAILED'}")
    print(f"  Ordering Validation: {'PASSED' if ordering_passed else 'FAILED'}")
    print(f"  Valid Rows: {valid_count}/{len(validation_df)}")
    print(f"  Invalid Rows (Disorders): {invalid_count}/{len(validation_df)}")
    
    if invalid_count > 0:
        disorder_ids = validation_df[~validation_df['is_valid']]['id'].tolist()
        print(f"  IDs with Disorders: {disorder_ids}")
    
    print(f"\nReports generated in folder '{REPORTS_FOLDER}':")
    print(f"  - Query Data: {data_file}")
    print(f"  - Full Validation: {validation_file}")
    print(f"  - ⚠️  DISORDERS ONLY: {disorders_file}")
    print(f"  - Schema Report: {schema_file}")
    print(f"{'='*60}")
    
    # 5. Assert both validations passed
    assert schema_passed, f"Schema validation failed. See report: {schema_file}"
    assert ordering_passed, (
        f"Ordering validation failed. {invalid_count} disorders found at ID(s): "
        f"{validation_df[~validation_df['is_valid']]['id'].tolist()}. "
        f"See detailed report: {disorders_file}"
    )
    
    print("\n✓✓✓ All validations passed! ✓✓✓")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
