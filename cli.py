def pytest_addoption(parser):
    """Add custom command-line options."""
    parser.addoption(
        "--table-name",
        action="store",
        default="my_table",
        help="Name of the table to query"
    )
    parser.addoption(
        "--where-clause",
        action="store",
        default="id < 1000",
        help="WHERE clause for the query"
    )


def pytest_configure(config):
    """Store CLI options as global variables."""
    pytest.table_name = config.getoption("--table-name")
    pytest.where_clause = config.getoption("--where-clause")


TABLE_NAME = pytest.table_name if hasattr(pytest, 'table_name') else "my_table"
WHERE_CLAUSE = pytest.where_clause if hasattr(pytest, 'where_clause') else "id < 1000"
