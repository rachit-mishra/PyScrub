def profile_data(df: DataFrame) -> DataFrame:
    """Generate a profile for the data, including statistics."""
    profile = df.describe().toPandas()
    return profile
