from pyspark import pipelines as dp
from pyspark.sql import functions as F
import re

file_path = '/Volumes/workspace/sus_establishments/raw/sus_dictionary/tp_estab.csv'

def sanitize_dataframe(df):
    # Remover pontuação das colunas
    for col_name in df.columns:
        # Mantém apenas letras, números e remove o resto
        clean_name = re.sub(r'[^\w]', '', col_name).lower() 
        df = df.withColumnRenamed(col_name, clean_name)

    # Remoção de caracteres especiais
    accents = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    replacements = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
    
    string_cols = [c for c, t in df.dtypes if t == "string"]
    
    for c in string_cols:
        # Primeiro removemos os acentos manualmente via translate
        df = df.withColumn(c, F.translate(F.col(c), accents, replacements))
        # Depois removemos qualquer caractere especial que tenha sobrado via Regex
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^a-zA-Z0-9 ]", ""))
        # Remove espaços extras
        df = df.withColumn(c, F.trim(F.col(c)))
        
    return df

@dp.table()
def stg_establishment_type():
    df_spark = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "ISO-8859-1") \
        .load(file_path)
    
    return sanitize_dataframe(df_spark)