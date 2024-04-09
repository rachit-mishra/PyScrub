# profiling.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

def profile_data(df: DataFrame) -> DataFrame:
    """Generate a profile for the data, including statistics."""
    profile = df.describe().toPandas()
    return profile

def generate_statistics(df: DataFrame, columns: list) -> DataFrame:
    """Generate statistics (count, mean, stddev, min, max) for specified columns."""
    return df.describe(columns)

def data_quality_report(df: DataFrame, columns: list) -> DataFrame:
    """Generate a data quality report (missing values, unique values) for specified columns."""
    spark = SparkSession.builder.getOrCreate()
    
    # Define schema for the quality report DataFrame
    schema = StructType([
        StructField("Column", StringType(), False),
        StructField("MissingValues", IntegerType(), False),
        StructField("UniqueValues", IntegerType(), False)
    ])
    
    # Initialize an empty DataFrame for the report
    report_df = spark.createDataFrame([], schema)
    
    # Calculate missing and unique values for each column and append to report DataFrame
    for column in columns:
        missing_values_count = df.filter(col(column).isNull()).count()
        unique_values_count = df.select(column).distinct().count()
        
        # Creating a new row DataFrame
        new_row = spark.createDataFrame(
            [(column, missing_values_count, unique_values_count)],
            schema=schema
        )
        
        # Append new row to the report DataFrame
        report_df = report_df.union(new_row)
    
    return report_df