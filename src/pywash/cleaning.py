from pyspark.sql import DataFrame
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, stddev, mean

def remove_missing_values(df: DataFrame, columns: list) -> DataFrame:
    """Remove rows with missing values in specified columns."""
    return df.dropna(subset=columns)

def deduplicate_rows(df: DataFrame) -> DataFrame:
    """Remove duplicate rows."""
    return df.dropDuplicates()

def remove_outliers(df: DataFrame, column: str) -> DataFrame:
    """Remove outliers from a specific column."""
    bounds = df.select(
        mean(col(column)).alias('mean'),
        stddev(col(column)).alias('stddev')
    ).collect()[0]
    
    lower_bound = bounds['mean'] - (3 * bounds['stddev'])
    upper_bound = bounds['mean'] + (3 * bounds['stddev'])
    
    return df.filter(col(column).between(lower_bound, upper_bound))

def normalize_column(df: DataFrame, column: str) -> DataFrame:
    """Normalize a numerical column to have mean 0 and stddev 1."""
    stats = df.select(
        mean(col(column)).alias('mean'),
        stddev(col(column)).alias('stddev')
    ).collect()[0]
    
    return df.withColumn(column, (col(column) - stats['mean']) / stats['stddev'])

