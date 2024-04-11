# profiling.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import col, count, skewness, kurtosis
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

def profile_data(df: DataFrame) -> DataFrame:
    """Generate a profile for the data, including statistics."""
    profile = df.describe().toPandas()
    return profile

def generate_statistics(df: DataFrame, columns: list) -> DataFrame:
    """Generate extended statistics including skewness and kurtosis."""
    summary_stats = df.describe(columns)
    skewness_stats = df.select([skewness(col(c)).alias(f"{c}_skewness") for c in columns])
    kurtosis_stats = df.select([kurtosis(col(c)).alias(f"{c}_kurtosis") for c in columns])
    return summary_stats.union(skewness_stats).union(kurtosis_stats)

def correlation_matrix(df: DataFrame, numeric_columns: list) -> DataFrame:
    """Calculate the correlation matrix for numeric columns."""
    vector_col = "corr_features"
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol=vector_col)
    df_vector = assembler.transform(df).select(vector_col)
    matrix = Correlation.corr(df_vector, vector_col)
    return matrix

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