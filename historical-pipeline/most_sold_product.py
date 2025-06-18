from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from datetime import datetime

def most_sold_product_by_quarters(transacoes_df: DataFrame) -> DataFrame:
    """
    Identifica o produto mais vendido em termos de quantidade e receita.
    """
    
    # Determina o último ano completo
    data_maxima = transacoes_df.agg(F.max("data_compra")).first()[0]

    # Seleciona os 4 últimos trimestres completos
    trimestres = []
    for i in range(4):
        trimestres_offset = (data_maxima.month - 1) // 3 - i
        anos_offset =  data_maxima.year
        if trimestres_offset < 1:
            trimestres_offset += 4
            anos_offset -= 1

        trimestres.append((anos_offset, trimestres_offset))

    # Filtro para os últimos 4 trimestres completos
    filtro = None
    for ano, trimestre in trimestres:
        mes_inicio = (trimestre - 1) * 3 + 1
        mes_fim = trimestre * 3

        t_inicio = datetime(ano, mes_inicio, 1)
        if mes_fim == 12:
            t_fim = datetime(ano + 1, 1, 1)
        else:
            t_fim = datetime(ano, mes_fim + 1, 1)

        cond = (F.col("data_compra") >= t_inicio) & (F.col("data_compra") < t_fim)
        if filtro is None:
            filtro = cond
        else:
            filtro |= cond
    
    df_filtrado = transacoes_df.filter(filtro)
    
    # Adiciona colunas de ano e trimestre
    transacoes_com_trimestre = df_filtrado.withColumn(
        "ano", F.year("data_compra")
    ).withColumn(
        "trimestre", F.quarter("data_compra")
    )

    # Soma as quantidade vendidas por produto, ano e trimestre
    vendas_por_trimestre = transacoes_com_trimestre.groupBy(
        "ano", "trimestre", "id_produto"
    ).agg(
        F.sum("quantidade").alias("total_vendido")
    )

    # Classifica os produtos por trimestre e total vendido e retorna os top 10
    windows_spec = Window.partitionBy("ano", "trimestre").orderBy(F.desc("total_vendido"))
    top_10_por_trimestre = vendas_por_trimestre.withColumn(
        "rank", F.row_number().over(windows_spec)
    ).filter(F.col("rank") <= 10)
    
    return top_10_por_trimestre