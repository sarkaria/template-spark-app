from ast import Str
import os
from pyspark.sql.functions import lit
from datetime import datetime
from delta import *
from process_data import get_or_create_spark


def setup_spark_and_create_tables():
    '''
        This method creates a spark context and then creats the data tables.
    '''
    spark = get_or_create_spark(spark, 'Create-Test-Data-Local-Spark-App')
    create_tables()
    spark.stop()


def create_tables(spark):
    '''
        This method creates test parquet files from source data files, e.g. JSON.
    '''
    students_df = (spark.read
        .option("multiline",True)
        .format('json')
        .load(os.path.join(os.getcwd(), 'storage', 'JSON', 'students.json'))
    )

    students_df.printSchema()

    (students_df.write
        .format('parquet')
        .mode('overwrite')
        .option('header', True)
        .save(os.path.join(os.getcwd(), 'storage', 'input', 'students'))
    )

    updates_df = (spark.read
        .option("multiline",True)
        .format('json')
        .load(os.path.join(os.getcwd(), 'storage', 'JSON', 'updates.json'))
    )

    (updates_df.write
        .format('parquet')
        .mode('overwrite')
        .option('header', True)
        .save(os.path.join(os.getcwd(), 'storage', 'input', 'updates'))
    )


if __name__ == "__main__":
    setup_spark_and_create_tables()