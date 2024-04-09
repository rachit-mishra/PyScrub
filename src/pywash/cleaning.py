from pyspark.sql import DataFrame

def remove_missing_values(df: DataFrame, columns: list) -> DataFrame:
    """Remove rows with missing values in specified columns."""
    return df.dropna(subset=columns)

def deduplicate_rows(df: DataFrame) -> DataFrame:
    """Remove duplicate rows."""
    return df.dropDuplicates()
