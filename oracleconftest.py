import pytest
import pandas as pd
import cx_Oracle
import os


@pytest.fixture
def oracle_query():
    """
    Pytest fixture to query Oracle database using environment variables.
    
    Required environment variables:
    - ORACLE_HOST
    - ORACLE_PORT (default: 1521)
    - ORACLE_SERVICE_NAME
    - ORACLE_USER
    - ORACLE_PASSWORD
    """
    def _query_oracle(query: str, schema: str = None) -> pd.DataFrame:
        """
        Execute query on Oracle and return DataFrame.
        
        Args:
            query: SQL query string
            schema: Optional schema to set for the session
            
        Returns:
            pandas DataFrame with query results
        """
        connection = None
        
        try:
            # Get connection details from environment
            host = os.getenv('ORACLE_HOST')
            port = int(os.getenv('ORACLE_PORT', 1521))
            service_name = os.getenv('ORACLE_SERVICE_NAME')
            user = os.getenv('ORACLE_USER')
            password = os.getenv('ORACLE_PASSWORD')
            
            # Create DSN
            dsn = cx_Oracle.makedsn(host=host, port=port, service_name=service_name)
            
            # Connect to Oracle
            connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
            
            # Set schema if provided
            if schema:
                cursor = connection.cursor()
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")
                cursor.close()
            
            # Use pandas read_sql
            df = pd.read_sql(query, connection)
            
            return df
            
        finally:
            if connection:
                connection.close()
    
    return _query_oracle
