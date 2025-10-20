from . import *
import pytest

@pytest.fixture(scope="session")
def mock_data():
    spark = get_spark_session()
    # 1 prod - 1 cat, 1 prod - n cat, n prod - 1 cat, n prod - 0 cat, n cat - 0 prod
    p_df = spark.createDataFrame([
        ("A", 1),
        ("B", 2),
        ("C", 3),
        ("D", 4),
        ("E", 5),
    ], schema=["prod_name", "prod_id"])
    c_df = spark.createDataFrame([
        ("1", 1),
        ("2", 2),
        ("3", 3),
        ("4", 4),
    ], schema=["cat_name", "cat_id"])
    l_df = spark.createDataFrame([
        (1, 1),
        (2, 2),
        (3, 2),
    ], schema=["prod_id", "cat_id"])
    res_pairs = [['A', '1'], ['C', '2'], ['B', '2']]
    res_e = [['D'], ['E']]
    data = {"p": p_df, "c": c_df, "l": l_df, "r_p": res_pairs, "r_e": res_e, "spark": spark}
    yield data

def test_method(mock_data):
    res_pairs, res_e = get_pairs(mock_data["spark"], mock_data["p"], mock_data["c"], mock_data["l"])
    res_pairs = df_to_list(res_pairs)
    res_e = df_to_list(res_e)
    for i in range(len(res_pairs)):
        assert res_pairs[i] == mock_data["r_p"][i]
    for i in range(len(res_e)):
        assert res_e[i] == mock_data["r_e"][i]

def df_to_list(df):
    return [list(row) for row in df.collect()]

