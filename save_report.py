def save_reports(df: pd.DataFrame, schema_errors: pd.DataFrame, test_name: str) -> tuple:
    """
    Save query data and Pandera validation report to CSV files with timestamp.
    
    Args:
        df: Original DataFrame from query
        schema_errors: Pandera failure_cases DataFrame (already contains 'index' column)
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
    
    # 2. Save Pandera validation report
    # The 'index' column in schema_errors already contains the row indices from the original dataframe
    report_filename = os.path.join(REPORTS_FOLDER, f"{test_name}_pandera_report_{timestamp}.csv")
    schema_errors.to_csv(report_filename, index=False)  # Don't include pandas index, use the 'index' column
    print(f"Pandera validation report saved to: {report_filename}")
    
    return data_filename, report_filename
