from ast import Str
import os
from pyspark.sql.functions import lit
from datetime import datetime
from delta import *
from process_data import get_or_create_spark


spark = get_or_create_spark('Create-Test-Data-Local-Spark-App')

test_data_df = (spark.read
    .option('header', True)
    .option('inferschema', True)
    .option("timestampFormat", "dd-LLL-yy")
    .format('csv')
    .load(os.path.join(os.getcwd(), 'storage', 'CSV', 'test_data.csv'))
)

test_data_df.printSchema()

(test_data_df.write
    .format('delta')
    .mode('overwrite')
    .option('header', True)
    .save(os.path.join(os.getcwd(), 'storage', 'input', 'history'))
)


spark.stop()