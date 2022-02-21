import unittest
import pyspark
from datetime import datetime
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from process_data import get_or_create_spark
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

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_process_data(self):
        # setup data

        join_df = process_data(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 2
        self.assertEqual(expected_count, actual_count)


      



if __name__ == '__main__':
    unittest.main()