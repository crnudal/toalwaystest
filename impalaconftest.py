import pytest
import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas


@pytest.fixture
def impala_query():
    """
    Pytest fixture to query Impala and return a pandas DataFrame.
    Takes a config class and query string, returns DataFrame.
    """
    def _query_impala(config_class, query: str) -> pd.DataFrame:
        """
        Execute query on Impala and return DataFrame.
        
        Args:
            config_class: Class with Impala connection parameters
            query: SQL query string with SELECT, columns, and WHERE clauses
            
        Returns:
            pandas DataFrame with query results
        """
        # Initialize the config class
        config = config_class()
        
        conn = None
        cursor = None
        
        try:
            # Connect to Impala
            conn = connect(
                host=config.host,
                port=config.port,
                database=config.database,
                auth_mechanism=getattr(config, 'auth_mechanism', 'PLAIN'),
                user=getattr(config, 'user', None),
                password=getattr(config, 'password', None),
                use_ssl=getattr(config, 'use_ssl', False),
                timeout=getattr(config, 'timeout', 3600)
            )
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            # Convert to pandas DataFrame
            df = as_pandas(cursor)
            
            return df
            
        finally:
            # Clean up connections
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    return _query_impala
