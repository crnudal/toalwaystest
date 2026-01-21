from conftest import query_impala

def test_something():
    df = query_impala(ImpalaConfig, "SELECT * FROM table")
