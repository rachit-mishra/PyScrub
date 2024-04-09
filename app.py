# app.py

import json
from pyspark.sql import SparkSession
from src.pywash.cleaning import remove_outliers, normalize_column
from src.pywash.profiling import generate_statistics, data_quality_report

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

    # Similarly, call profiling functions as needed
    if "profiling_tasks" in config:
        for task in config["profiling_tasks"]:
            if task["task"] == "generate_statistics":
                stats_df = generate_statistics(df, **task["params"])
                stats_df.show()
            elif task["task"] == "data_quality_report":
                df = data_quality_report(df, **task["params"])

    spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1])
