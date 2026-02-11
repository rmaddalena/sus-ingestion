from pyspark import pipelines as dp
from pyspark.sql import functions as F

file_path = '/Volumes/workspace/sus_establishments/raw/STSP/*.csv'

@dp.table()
def stg_sus_establishments():

    # Renderiza o CSV e adiciona uma coluna com o nome do arquivo de origem
    df_spark = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .option("encoding", "MacRoman") \
        .load(file_path) \
        .withColumn('file_name', F.substring(F.col('_metadata.file_path'), -12, 8))
    
    return df_spark
