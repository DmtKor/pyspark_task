"""Used for function definitions"""
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame


def get_spark_session() -> SparkSession:
    """Returns prepared SparkSession for tests"""
    return SparkSession.builder.appName("PySpark 101").getOrCreate()


def get_test_dataframes(sparksess: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Returns prepared DataFrames for tests"""
    products_df = sparksess.createDataFrame([
        ("Product A", 1),
        ("Product B", 2),
        ("Product C", 3),
        ("Product D", 4),
        ("Product E", 5),
    ], schema=["prod_name", "prod_id"])

    categories_df = sparksess.createDataFrame([
        ("Category 1", 1),
        ("Category 2", 2),
        ("Category 3", 3),
    ], schema=["cat_name", "cat_id"])

    links_df = sparksess.createDataFrame([
        (1, 1),
        (2, 2),
        (3, 2),
    ], schema=["prod_id", "cat_id"])

    return products_df, categories_df, links_df


# The desired method
def get_pairs(products_df: DataFrame, categories_df: DataFrame, links_df: DataFrame) \
                -> Tuple[DataFrame, DataFrame]:
    """
    Parameters:
        spark - active spark session
        products_df - DataFrame with columns "prod_name", "prod_id"
        categories_df - DataFrame with columns "cat_name", "cat_id"
        links_df - DataFrame with columns "cat_id", "prod_id"
    
    Returns:
        df1 - DataFrame with each prod_name-cat_name pair 
        df2 - names (prod_name) of all products without a single category
    """
    df1 = links_df.join(products_df, "prod_id").join(categories_df, \
        "cat_id").select("prod_name", "cat_name")
    df2 = products_df.join(links_df, "prod_id", "left")
    df2 = df2.filter(df2["cat_id"].isNull()).select("prod_name")
    return df1, df2


if __name__ == "__main__":
    spark = get_spark_session()
    pr_df, cat_df, l_df = get_test_dataframes(spark)
    prod_cat_df, prod_without_cat_df = get_pairs(pr_df, cat_df, l_df)
    print("Products with categories (each pair)")
    prod_cat_df.show()
    print("Products without categories")
    prod_without_cat_df.show()
    print([list(row) for row in prod_without_cat_df.collect()])
    spark.stop()
