"""
Script to generate pytest test files from a template.
Configure the test parameters and run this script to generate test files.
"""

from pathlib import Path
import os


# Test configurations - Add as many as you need
TEST_CONFIGS = [
    {
        "test_name": "timestamp_validation",  # Will create test_timestamp_validation.py
        "table_name": "my_table",
        "timestamp_column": "timestamp",
        "timestamp_column_1": "timestamp_1",
        "timesequence_column": "timesequence",
        "where_clause": "id < 1000"
    },
    {
        "test_name": "another_validation",
        "table_name": "another_table",
        "timestamp_column": "event_timestamp",
        "timestamp_column_1": "event_timestamp_1",
        "timesequence_column": "event_sequence",
        "where_clause": "status = 'active'"
    },
    # Add more configurations here
]


TEST_TEMPLATE = """ code here """


def generate_test_files():
    """Generate test files from configurations."""
    
    for config in TEST_CONFIGS:
        # Generate filename
        test_filename = f"test_{config['test_name']}.py"
        
        # Format the template with config values
        test_content = TEST_TEMPLATE.format(**config)
        
        # Write the test file
        with open(test_filename, 'w') as f:
            f.write(test_content)
        
        print(f"✓ Generated: {test_filename}")
        print(f"  - Table: {config['table_name']}")
        print(f"  - Columns: {config['timestamp_column']}, {config['timestamp_column_1']}, {config['timesequence_column']}")
        print(f"  - Where: {config['where_clause']}")
        print()


if __name__ == "__main__":
    print("=" * 60)
    print("Test File Generator")
    print("=" * 60)
    print()
    
    generate_test_files()
    
    print("=" * 60)
    print(f"Generated {len(TEST_CONFIGS)} test file(s)")
    print("=" * 60)
