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
    
    # 1. Save the original query data with its index
    data_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_query_data_{timestamp}.csv")
    df.to_csv(data_filename, index=True)
    print(f"\nQuery data saved to: {data_filename}")
    
    # 2. Debug: Print schema_errors structure
    print(f"\nSchema errors columns: {schema_errors.columns.tolist()}")
    print(f"Schema errors index: {schema_errors.index.tolist()}")
    
    # 3. Save Pandera validation report with all available information
    report_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_pandera_report_{timestamp}.csv")
    # Save with index=True to preserve the pandas index (which might contain the failure row numbers)
    schema_errors.to_csv(report_filename, index=True)
    print(f"Pandera validation report saved to: {report_filename}")
    
    return data_filename, report_filename
