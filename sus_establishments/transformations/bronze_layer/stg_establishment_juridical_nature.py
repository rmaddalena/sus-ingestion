from pyspark import pipelines as dp
from pyspark.sql import functions as F
import re

file_path = '/Volumes/workspace/sus_establishments/raw/sus_dictionary/natjur.csv'

def sanitize_dataframe(df):
    # Remover pontuação das colunas
    for col_name in df.columns:
        # Mantém apenas letras, números e remove o resto
        clean_name = re.sub(r'[^\w]', '', col_name).lower() 
        df = df.withColumnRenamed(col_name, clean_name)
        
    # Dicionário de grupos para evitar erros de posição
    mapping = {
        'a': '[áàâãä]',
        'e': '[éèêë]',
        'i': '[íìîï]',
        'o': '[óòôõö]',
        'u': '[úùûü]',
        'c': '[ç]',
        'A': '[ÁÀÂÃÄ]',
        'E': '[ÉÈÊË]',
        'I': '[ÍÌÎÏ]',
        'O': '[ÓÒÔÕÖ]',
        'U': '[ÚÙÛÜ]',
        'C': '[Ç]'
    }
    
    string_cols = [c for c, t in df.dtypes if t == "string"]
    
    for c in string_cols:
        for replacement, pattern in mapping.items():
            # Substitui cada grupo pelo seu equivalente sem acento
            df = df.withColumn(c, F.regexp_replace(F.col(c), pattern, replacement))
        
        # Remove o que não for alfanumérico e limpa espaços
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"[^a-zA-Z0-9 ]", ""))
        df = df.withColumn(c, F.trim(F.col(c)))
        
    return df

@dp.table()
def stg_establishment_juridical_nature():
    df_spark = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "ISO-8859-1") \
        .load(file_path)
    
    return sanitize_dataframe(df_spark)