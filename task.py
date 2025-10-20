from pyspark.sql import SparkSession, DataFrame
from typing import Tuple

spark = SparkSession.builder.appName("PySpark 101").getOrCreate()

products_df = spark.createDataFrame([
    ("Product A", 1),
    ("Product B", 2),
    ("Product C", 3),
    ("Product D", 4),
    ("Product E", 5),
], schema=["prod_name", "prod_id"])

categories_df = spark.createDataFrame([
    ("Category 1", 1),
    ("Category 2", 2),
    ("Category 3", 3),
], schema=["cat_name", "cat_id"])

links_df = spark.createDataFrame([
    (1, 1),
    (2, 2),
    (3, 2),
], schema=["prod_id", "cat_id"])

def get_pairs(spark: SparkSession, \
              products_df: DataFrame, \
                categories_df: DataFrame, \
                    links_df: DataFrame)\
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
    df1 = links_df.join(products_df, "prod_id").join(categories_df, "cat_id").select("prod_name", "cat_name")
    df2 = products_df.join(links_df, "prod_id", "left")
    df2 = df2.filter(df2["cat_id"].isNull()).select("prod_name")
    return df1, df2

prod_cat_df, prod_without_cat_df = get_pairs(spark, products_df, categories_df, links_df)
print("Products with categories (each pair)")
prod_cat_df.show()
print("Products without categories")
prod_without_cat_df.show()
spark.stop()