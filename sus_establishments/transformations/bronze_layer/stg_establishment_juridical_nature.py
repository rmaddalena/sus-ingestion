from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import unicodedata
import re

file_path = '/Volumes/workspace/sus_establishments/raw/sus_dictionary/natjur.csv'

def remove_accents(text):
    if text is None:
        return None
    text = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in text if not unicodedata.combining(c))

remove_accents_udf = F.udf(remove_accents, StringType())

def sanitize_dataframe(df):
    # Remover pontuação das colunas
    for col_name in df.columns:
        # Mantém apenas letras, números e remove o resto
        clean_name = re.sub(r'[^\w]', '', col_name).lower() 
        df = df.withColumnRenamed(col_name, clean_name)
    
    string_cols = [c for c, t in df.dtypes if t == "string"]
    
    for c in string_cols:
        df = df.withColumn(c, remove_accents_udf(F.col(c)))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^a-zA-Z0-9 ]", ""))
        df = df.withColumn(c, F.trim(F.col(c)))
        
    return df

@dp.table()
def stg_establishment_juridical_nature():
    df_spark = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "utf-8") \
        .load(file_path)
    
    return sanitize_dataframe(df_spark)