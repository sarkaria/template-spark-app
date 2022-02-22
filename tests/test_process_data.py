import os
from turtle import update
import unittest 

from create_test_tables_from_source_data import create_tables
from dotenv import load_dotenv
from process_data import get_or_create_spark, process_data, get_path_to_table, merge_into_target
from pyspark.sql.types import *
from pyspark.sql.functions import *

'''
    Your .env file must contain a `PYSPARK_PYTHON=python` entry
    otherwise you may get a `Python worker failed to connect back` exception
'''
load_dotenv()


class Testing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = get_or_create_spark('Local-Unittest-Spark-App')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_data(self):
        create_tables(self.spark)
        source_df = self.spark.read.format('parquet').load('storage/input/students')
        new_df = process_data(self.spark, source_df)
        actual_count = new_df.count()
        expected_count = 28
        self.assertEqual(expected_count, actual_count, '--* DataFrame did not have expected number of row')

    def test_get_path(self):
        path = get_path_to_table(self.spark, 'students', 'input')
        expected_path = os.path.join(os.getcwd(), 'storage', 'input', 'students')
        self.assertEqual(expected_path, path, '--* Path incorrect')

    def test_merge(self):
        create_tables(self.spark)
        source_df = self.spark.read.format('parquet').load('storage/input/students')
        new_df = process_data(self.spark, source_df)

        target_path = os.path.join(os.getcwd(), 'storage', 'output', 'students')
        merge_into_target(self.spark, new_df, target_path, ['ID'])

        # Verify size of original data set
        students_table_df = self.spark.read.format('delta').load('storage/output/students')
        count_1 = students_table_df.count()
        expected_count = 28
        self.assertEqual(expected_count, count_1, '--* Incorrect count after 1st merge operation')

        # Verify value of an updated record
        actual_student_status = students_table_df.filter(col('ID') == 1).select('Status').collect()[0][0]
        expected_status = 'Graduate'
        self.assertEqual(expected_status, actual_student_status, '--* Unexpected student status after initial load')

        updates_source_df = self.spark.read.format('parquet').load('storage/input/updates')
        updates_df = process_data(self.spark, updates_source_df)
        merge_into_target(self.spark, updates_df, target_path, ['ID'])        

        # Verify size of data set after being updated
        updated_students_df = self.spark.read.format('delta').load(target_path)
        count_2 = updated_students_df.count()
        expected_count = 30
        self.assertEqual(expected_count, count_2, '--* Unexpected number of rows after 2nd merge operation')

        # Verify value of an updated record
        actual_student_status = updated_students_df.filter(col('ID') == 1).select('Status').collect()[0][0]
        expected_status = 'PostGrad'
        self.assertEqual(expected_status, actual_student_status, '--* Unexpected student status after update operation')

        


      



if __name__ == '__main__':
    unittest.main()