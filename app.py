# app.py

import json
from pyspark.sql import SparkSession
from src.pyscrub.cleaning import remove_outliers, normalize_column
from src.pyscrub.profiling import generate_statistics, data_quality_report
import os

import os
import pandas as pd  # Make sure pandas is installed

def write_dataframe_to_csv(df, path):
    """
    Collects a Spark DataFrame to the driver node and writes it as a single CSV file.
    Note: Use for small datasets to avoid memory issues. Scalability work to be taken up as future enhancements.
    """
    # Convert Spark DataFrame to a pandas DataFrame
    pandas_df = df.toPandas()

    # Create the directory for the file if it doesn't exist, not the file itself
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Write the pandas DataFrame to CSV
    pandas_df.to_csv(path, index=False)


def main(config_path):
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)

    spark = SparkSession.builder.appName("PyWash").getOrCreate()
    df = spark.read.csv(config["data_path"], header=True, inferSchema=True)

    # Assume config includes tasks as dictionaries with necessary parameters
    for task in config["cleaning_tasks"]:
        if task["task"] == "remove_outliers":
            df = remove_outliers(df, **task["params"])
        elif task["task"] == "normalize_column":
            df = normalize_column(df, **task["params"])
    
    # Writing the cleaned data to the output directory
    cleaned_data_path = "/app/data/output/cleaned_data.csv"
    write_dataframe_to_csv(df, cleaned_data_path)
    
    # Similarly, call profiling functions as needed
    if "profiling_tasks" in config:
        for task in config["profiling_tasks"]:
            if task["task"] == "generate_statistics":
                stats_df = generate_statistics(df, **task["params"])
                stats_df.show()
                # Writing profiling results to the output directory
                profiled_report_path = "/app/data/output/profiled_report.csv"
                write_dataframe_to_csv(stats_df, profiled_report_path)
            elif task["task"] == "data_quality_report":
                quality_df = data_quality_report(df, **task["params"])
                data_quality_report_path = "/app/data/output/data_quality_report.csv"
                write_dataframe_to_csv(quality_df, data_quality_report_path)

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
