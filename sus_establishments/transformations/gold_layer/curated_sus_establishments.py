from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from operator import add

rules = {
    "cnes_not_null": "cnes IS NOT NULL",
    "treated_cnpj_not_0": "treated_cnpj != '00000000000000'",
    "unique_key": "is_unique = 1"
}

@dp.table()
@dp.expect_all_or_fail(rules)
def curated_sus_establishments():

    df = spark.sql("""
        SELECT 
        *
        FROM int_sus_establishments
        QUALIFY 
        ROW_NUMBER() OVER(PARTITION BY cnes, treated_cnpj ORDER BY dt_atual, file_name DESC) = 1
    """
    )

    # Somando colunas de leitos e instalações
    cols_to_sum_inst = [F.coalesce(F.col(c), F.lit(0)) 
                   for c in df.columns if c.startswith("qtinst")]
    
    cols_to_sum_leit = [F.coalesce(F.col(c), F.lit(0)) 
                   for c in df.columns if c.startswith("qtleit")]
    
    # Faz um rownumber por cnes e treated_cnpj
    window_spec = Window.partitionBy("cnes", "treated_cnpj").orderBy(F.lit(1))

    # Retorna o df com as colunas de leitos e instalações somadas e altera a coluna is_unique
    return df.withColumns({
        "total_instalacoes": reduce(add, cols_to_sum_inst),
        "total_leitos": reduce(add, cols_to_sum_leit),
        "is_unique": F.row_number().over(window_spec)
    })
