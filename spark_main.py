from process_data import get_or_create_spark, process_data_wrapper

spark = get_or_create_spark('Local-Spark-App')

process_data_wrapper(spark)