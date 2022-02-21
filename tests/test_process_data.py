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

    def test_get_matching_records(self):
        # setup data
        de1 = datetime(2021,7,1,12,0)
        de2 = datetime(2021,7,1,12,30)
        de3 = datetime(2021,7,1,15,0)
        de4 = datetime(2021,8,1,12,0)
        de5 = datetime(2021,8,1,12,30)
        de6 = datetime(2021,8,1,15,0)
        events = [('1a', de1, de2, de3, 'SS COMMANDER'),
                  ('1b', de4, de5, de6, 'SS CORSAIR')]
        event_schema = StructType([StructField('EVENT_Guid', StringType()), \
                                   StructField('Start_Time', TimestampType()), \
                                   StructField('End_Time', TimestampType()), \
                                   StructField('LastChanged_Date', TimestampType()), \
                                   StructField('Tug', StringType())
                                ])
        events_df = self.spark.createDataFrame(data=events, schema=event_schema)
        events_df.show()
        events_df.printSchema()

        dt1 = datetime(2021,7,1,12,5)
        dt2 = datetime(2021,7,1,12,17)
        dt3 = datetime(2021,7,1,12,56)
        ts1 = int(dt1.timestamp())
        ts2 = int(dt2.timestamp())
        ts3 = int(dt3.timestamp())
        telem = [(ts1, 'Commander', dt1),
                 (ts2, 'Commander', dt2),
                 (ts3, 'Commander', dt3)]
        telem_schema = StructType([StructField('DAT_0', IntegerType()), \
                                   StructField('vessel_name', StringType()), \
                                   StructField('telem_datetime', TimestampType())
                                ])
        telem_df = self.spark.createDataFrame(data=telem, schema=telem_schema)
        telem_df.show()
        telem_df.printSchema()

        join_df = get_matching_events_telemetry(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 2
        self.assertEqual(expected_count, actual_count)


    def test_get_vessel_mismatch(self):
        # setup data
        de1 = datetime(2021,7,1,12,0)
        de2 = datetime(2021,7,1,12,30)
        de3 = datetime(2021,7,1,15,0)
        de4 = datetime(2021,8,1,12,0)
        de5 = datetime(2021,8,1,12,30)
        de6 = datetime(2021,8,1,15,0)
        events = [('1a', de1, de2, de3, 'SS COMOX'),
                  ('1b', de4, de5, de6, 'SS CORSAIR')]
        event_schema = StructType([StructField('EVENT_Guid', StringType()), \
                                   StructField('Start_Time', TimestampType()), \
                                   StructField('End_Time', TimestampType()), \
                                   StructField('LastChanged_Date', TimestampType()), \
                                   StructField('Tug', StringType())
                                ])
        events_df = self.spark.createDataFrame(data=events, schema=event_schema)

        dt1 = datetime(2021,7,1,12,5)
        dt2 = datetime(2021,7,1,12,17)
        dt3 = datetime(2021,7,1,12,56)
        ts1 = int(dt1.timestamp())
        ts2 = int(dt2.timestamp())
        ts3 = int(dt3.timestamp())
        telem = [(ts1, 'Commander', dt1),
                 (ts2, 'Commander', dt2),
                 (ts3, 'Commander', dt3)]
        telem_schema = StructType([StructField('DAT_0', IntegerType()), \
                                   StructField('vessel_name', StringType()), \
                                   StructField('telem_datetime', TimestampType())
                                ])
        telem_df = self.spark.createDataFrame(data=telem, schema=telem_schema)

        join_df = get_matching_events_telemetry(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 0
        self.assertEqual(expected_count, actual_count)


    def test_get_date_mismatch(self):
        # setup data
        de1 = datetime(2021,6,1,12,0)
        de2 = datetime(2021,6,1,12,30)
        de3 = datetime(2021,6,1,15,0)
        de4 = datetime(2021,8,1,12,0)
        de5 = datetime(2021,8,1,12,30)
        de6 = datetime(2021,8,1,15,0)
        events = [('1a', de1, de2, de3, 'SS COMMANDER'),
                  ('1b', de4, de5, de6, 'SS COMMANDER')]
        event_schema = StructType([StructField('EVENT_Guid', StringType()), \
                                   StructField('Start_Time', TimestampType()), \
                                   StructField('End_Time', TimestampType()), \
                                   StructField('LastChanged_Date', TimestampType()), \
                                   StructField('Tug', StringType())
                                ])
        events_df = self.spark.createDataFrame(data=events, schema=event_schema)

        dt1 = datetime(2021,7,1,12,5)
        dt2 = datetime(2021,7,1,12,17)
        dt3 = datetime(2021,7,1,12,56)
        ts1 = int(dt1.timestamp())
        ts2 = int(dt2.timestamp())
        ts3 = int(dt3.timestamp())
        telem = [(ts1, 'Commander', dt1),
                 (ts2, 'Commander', dt2),
                 (ts3, 'Commander', dt3)]
        telem_schema = StructType([StructField('DAT_0', IntegerType()), \
                                   StructField('vessel_name', StringType()), \
                                   StructField('telem_datetime', TimestampType())
                                ])
        telem_df = self.spark.createDataFrame(data=telem, schema=telem_schema)

        join_df = get_matching_events_telemetry(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 0
        self.assertEqual(expected_count, actual_count)


    def test_None_Start_Time(self):
        # setup data with None Start_Time
        de2 = datetime(2021,7,1,12,30)
        de3 = datetime(2021,7,1,15,0)
        de5 = datetime(2021,8,1,12,30)
        de6 = datetime(2021,8,1,15,0)
        events = [('1a', None, de2, de3, 'SS COMMANDER'),
                  ('1b', None, de5, de6, 'SS CORSAIR')]
        event_schema = StructType([StructField('EVENT_Guid', StringType()), \
                                   StructField('Start_Time', TimestampType()), \
                                   StructField('End_Time', TimestampType()), \
                                   StructField('LastChanged_Date', TimestampType()), \
                                   StructField('Tug', StringType())
                                ])
        events_df = self.spark.createDataFrame(data=events, schema=event_schema)
        events_df.show()
        events_df.printSchema()

        dt1 = datetime(2021,7,1,12,5)
        dt2 = datetime(2021,7,1,12,17)
        dt3 = datetime(2021,7,1,12,56)
        ts1 = int(dt1.timestamp())
        ts2 = int(dt2.timestamp())
        ts3 = int(dt3.timestamp())
        telem = [(ts1, 'Commander', dt1),
                 (ts2, 'Commander', dt2),
                 (ts3, 'Commander', dt3)]
        telem_schema = StructType([StructField('DAT_0', IntegerType()), \
                                   StructField('vessel_name', StringType()), \
                                   StructField('telem_datetime', TimestampType())
                                ])
        telem_df = self.spark.createDataFrame(data=telem, schema=telem_schema)
        telem_df.show()
        telem_df.printSchema()

        join_df = get_matching_events_telemetry(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 0
        self.assertEqual(expected_count, actual_count)


    def test_No_Events(self):
        # setup empty events dataframe
        events = []
        event_schema = StructType([StructField('EVENT_Guid', StringType()), \
                                   StructField('Start_Time', TimestampType()), \
                                   StructField('End_Time', TimestampType()), \
                                   StructField('LastChanged_Date', TimestampType()), \
                                   StructField('Tug', StringType())
                                ])
        events_df = self.spark.createDataFrame(data=events, schema=event_schema)
        events_df.show()
        events_df.printSchema()

        dt1 = datetime(2021,7,1,12,5)
        dt2 = datetime(2021,7,1,12,17)
        dt3 = datetime(2021,7,1,12,56)
        ts1 = int(dt1.timestamp())
        ts2 = int(dt2.timestamp())
        ts3 = int(dt3.timestamp())
        telem = [(ts1, 'Commander', dt1),
                 (ts2, 'Commander', dt2),
                 (ts3, 'Commander', dt3)]
        telem_schema = StructType([StructField('DAT_0', IntegerType()), \
                                   StructField('vessel_name', StringType()), \
                                   StructField('telem_datetime', TimestampType())
                                ])
        telem_df = self.spark.createDataFrame(data=telem, schema=telem_schema)
        telem_df.show()
        telem_df.printSchema()

        join_df = get_matching_events_telemetry(events_df, telem_df)
        actual_count = join_df.count()
        expected_count = 0
        self.assertEqual(expected_count, actual_count)        



if __name__ == '__main__':
    unittest.main()