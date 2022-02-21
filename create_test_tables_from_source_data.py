from ast import Str
import os
from pyspark.sql.functions import lit
from datetime import datetime
from delta import *
from process_data import get_or_create_spark


spark = get_or_create_spark('Create-Test-Data-Local-Spark-App')

test_data_df = (spark.read
    .option("multiline",True)
    .format('json')
    .load(os.path.join(os.getcwd(), 'storage', 'JSON', 'students.json'))
)

test_data_df.printSchema()

(test_data_df.write
    .format('parquet')
    .mode('overwrite')
    .option('header', True)
    .save(os.path.join(os.getcwd(), 'storage', 'input', 'students'))
)


spark.stop()