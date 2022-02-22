import os
import unittest 

from create_test_tables_from_source_data import create_tables
from dotenv import load_dotenv
from process_data import get_or_create_spark, process_data, get_path_to_table, merge_into_target
from pyspark.sql.types import *

'''
    Your .env file must contain a `PYSPARK_PYTHON=python` entry
    otherwise you may get a `Python worker failed to connect back` exception
'''
load_dotenv()


class Testing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = get_or_create_spark('Local-Unittest-Spark-App')
        create_tables(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_data(self):
        source_df = self.spark.read.format('parquet').load('storage/input/students')
        new_df = process_data(self.spark, source_df)
        actual_count = new_df.count()
        expected_count = 28
        self.assertEqual(expected_count, actual_count, '--* DataFrame did not have expected number of row')

    def test_get_path(self):
        path = get_path_to_table(self.spark, 'students', 'input')
        expected_path = os.path.join(os.getcwd(), 'storage/input/students')
        self.assertEqual(expected_path, path, '--* Path incorrect')

    def test_merge(self):
        source_df = self.spark.read.format('parquet').load('storage/input/updates')
        target_path = os.path.join(os.getcwd(), 'storage/output/students')
        merge_into_target(self.spark, source_df, target_path, ['ID'])
        target_df = self.spark.read.format('delta').load(target_path)
        actual_count = target_df.count()
        expected_count = 30
        self.assertEqual(expected_count, actual_count, '--* Unexpected number of rows')

        # Also check presence of new IDs and changed IDs


      



if __name__ == '__main__':
    unittest.main()