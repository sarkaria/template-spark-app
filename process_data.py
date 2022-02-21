'''
    This file can be uploaded to the Databricks workspace and invoked at the command line
    as a python script.
'''

import argparse
import logging
import os
import pyspark

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
ap.add_argument('-f', '--filename', required=True, help='the name of the filename to use as source of input data')

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


def process_data (spark, source_dataframe) -> DataFrame:
    '''
        This is method does all the rea work. The source dataframe is processed
        and the content is merged into the target delta table. Upon success, a resulting
        dataframe is returned, otherwise an exception may be thrown. 
    '''
    df: DataFrame = None
    return df


def merge_into_target (incoming_df: DataFrame, target_delta_table: str):
    '''
        This method takes an incoming Dataframe and merges it into the target table.
        An exception is thrown if the merge fails.
    '''


def get_path_to_table(spark, name_of_table, subfolder='input', level='gold') -> str:
    '''
        Given the name of a table, this method will return a path
        to a local storage folder when running in spark local mode.
        Otherwise a path to blob storage will be returned.
    '''
    if spark.conf.get('spark.master') == 'local':
        return os.path.join(os.getcwd(), 'storage', f'{subfolder}', f'{name_of_table}')
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

    spark = pyspark.configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


if __name__ == "__main__":
    # If running as main, context must be a real spark job

    # Ideally we don't want to pass in a storage access key here in clear text so we 
    # should fetch it from a keyvault. 
    # spark.conf.set(f'fs.azure.account.key.{os.environ["STORAGE_NAME"]}.dfs.core.windows.net', secret_client.get_secret(os.environ['STORAGE_ACCESS_SECRET']).value)

    # For now, get from `environments` which is a config section of the databricks compute
    spark.conf.set(f'fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net', STORAGE_ACCOUNT_KEY)