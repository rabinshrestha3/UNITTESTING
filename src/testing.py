from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.types import StringType

def replace_nulls_with_one(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumn(
            column,
            when(col(column).isNull(), '1').otherwise(col(column).cast(StringType()))
        )
    return df
