# app.py

import json
from pyspark.sql import SparkSession
from src.pywash.cleaning import remove_outliers, normalize_column
from src.pywash.profiling import generate_statistics, data_quality_report
import os

# Create the output directory if it doesn't exist
output_dir = "/app/data/output"
os.makedirs(output_dir, exist_ok=True)

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
    df.coalesce(1).write.option("header", "true").csv(cleaned_data_path, mode="overwrite")    
    
    # Similarly, call profiling functions as needed
    if "profiling_tasks" in config:
        for task in config["profiling_tasks"]:
            if task["task"] == "generate_statistics":
                stats_df = generate_statistics(df, **task["params"])
                stats_df.show()
                # Writing profiling results to the output directory
                profiled_report_path = "/app/data/output/profiled_report.csv"
                stats_df.coalesce(1).write.option("header", "true").csv(profiled_report_path, mode="overwrite")
            elif task["task"] == "data_quality_report":
                df = data_quality_report(df, **task["params"])
                data_quality_report_path = "/app/data/output/data_quality_report.csv"
                df.coalesce(1).write.option("header", "true").csv(profiled_report_path, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
