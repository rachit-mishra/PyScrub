import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.pywash.cleaning import remove_outliers

class CleaningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a SparkSession
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("PyWashCleaningTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_remove_outliers(self):
        # Create a sample DataFrame for testing
        data = [(1, 10), (2, 20), (3, 1000), (4, 30), (5, 40)]
        df = self.spark.createDataFrame(data, ["id", "value"])

        # Assume remove_outliers removes rows where 'value' is an outlier
        cleaned_df = remove_outliers(df, "value")

        # Collect the results to a list of Row objects
        results = cleaned_df.collect()

        # Check the results
        expected_results = [Row(id=1, value=10), Row(id=2, value=20), Row(id=4, value=30), Row(id=5, value=40)]
        self.assertEqual(results, expected_results)

if __name__ == "__main__":
    unittest.main()
