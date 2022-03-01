'''
    This file can be uploaded to the Databricks workspace and invoked at the command line
    as a python script.
'''

import argparse
import logging
import os
from typing import List
import pyspark

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *


log = logging.getLogger(__name__)

# Default logging level is WARNING
#   To see more output, change to logging.INFO
logging.basicConfig(level=logging.WARNING)

# Construct argument parser
ap = argparse.ArgumentParser()
ap.add_argument('-t', '--tablename', required=True, help='the name of the input data table')

'''
    Databricks configuration required:
    1. Add python-dotenv==0.15.0 to 'Libraries'
    2. Update Advanced options->Environment Variables
'''
load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

# Constants
BASE_STORAGE_URL = f'abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net'


def process_data_wrapper (spark):
    '''
        This method is a wrapper that fetches the arguments from the command line,
        gets any input data frames from storage and calls the main code that processes
        the data.
    '''
    log.info(f"+++ process_data_wrapper running in spark {spark.version}")
    args = vars(ap.parse_args())
    p_tablename : str = args['tablename']
    log.info(f'+++ p_tablename={p_tablename}')

    source_table_path = get_path_to_table(spark, p_tablename, 'input')
    source_df = spark.read.format('parquet').load(source_table_path)
    processed_df = process_data(spark, source_df)
    target_path = get_path_to_table(spark, 'students', 'output')
    merge_into_target(spark, processed_df, target_path, ['ID'])


def process_data (spark, source_df: DataFrame) -> DataFrame:
    '''
        This method does all the real work. The source dataframe is processed and
        the resulting dataframe returned.
 
    '''
    df = source_df.withColumn('student', explode('Full')).drop('Full')
    df = df.select(df.student.ID.alias('ID'),
                   df.student.LastName.alias('LastName'), 
                   df.student.FirstName.alias('FirstName'),
                   df.student.age.alias('age'),
                   df.student.Major.alias('Major'),
                   df.student.StudentStatus.alias('Status'))
    return df


def merge_into_target (spark, incoming_df: DataFrame, target_path: str, merge_column_list: List[str]):
    '''
        This method takes an incoming Dataframe and merges it into the target table.
        An exception is thrown if the merge fails. If the target table does not
        exist it is created.
    '''
    if not DeltaTable.isDeltaTable(spark, target_path):
        log.warning(f'+++ Creating new table @ {target_path}')
        (
            incoming_df.write
            .format('delta')
            .mode('overwrite')
            .save(target_path)
        )
    else:
    # Build merge predicate - must be comma separated and we assume there is at least one
        try:
            predicate_string = f'target.{merge_column_list[0]} = updates.{merge_column_list[0]}'
            for column in merge_column_list[1:]:
                predicate_string += " AND " + f'target.{column} = updates.{column}'

            log.warning(f'+++ merging with predicate: {predicate_string}')

            target_delta_table = DeltaTable.forPath(spark, target_path)
            target_delta_table.alias('target') \
                .merge(incoming_df.alias('updates'), predicate_string) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute() 
        except IndexError:
            log.error(f'+++ Exception merging')


def get_path_to_table(spark, name_of_table, level) -> str:
    '''
        Given the name of a table, this method will return a path
        to a local storage folder when running in spark local mode.
        Otherwise a path to blob storage will be returned.
    '''
    if spark.conf.get('spark.master') == 'local':
        return os.path.join(os.getcwd(), 'storage', f'{level}', f'{name_of_table}')
    else:
        return f'{BASE_STORAGE_URL}/{level}/{name_of_table}'


def get_or_create_spark(spark_app_name: str):
    '''
        This method is to factor out the same code that is used in a few
        places inside this project.
        Note that in Windows, a new spark context will create a temporary folder.
        These folders will be locate in the /tmp folder of this project.
        A known error is expected that indicates that this temp spark folder
        could not be deleted when the spark context stops. You will have to delete
        these folders in /tmp manually.
    '''
    builder = pyspark.sql.SparkSession.builder.appName(spark_app_name) \
        .config("spark.master", "local") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.local.dir", "./tmp/spark-temp")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


if __name__ == "__main__":
    # If running as main, context must be a real spark job

    # Ideally we don't want to pass in a storage access key here in clear text so we 
    # should fetch it from a keyvault. 
    # spark.conf.set(f'fs.azure.account.key.{os.environ["STORAGE_NAME"]}.dfs.core.windows.net', secret_client.get_secret(os.environ['STORAGE_ACCESS_SECRET']).value)

    # For now, get from `environments` which is a config section of the databricks compute
    spark.conf.set(f'fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net', STORAGE_ACCOUNT_KEY)
    
    # Now process data!
    process_data_wrapper(spark)
