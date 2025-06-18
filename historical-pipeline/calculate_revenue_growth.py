from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

def calculate_revenue_growth(transacoes_df: DataFrame, clientes_df: DataFrame) -> DataFrame:
    """
    Calcula o crescimento da receita mês a mês, segmentado por cliente.
    
    1. Junta transações com dados dos clientes.
    2. Agrega a receita por mês e segmento.
    3. Usa uma Window Function para calcular o crescimento percentual.
    """
    
    # Adicionar uma coluna 'ano_mes' para facilitar o agrupamento e ordenação
    vendas_com_segmento_df = transacoes_df.join(clientes_df, "id_usuario", "left") \
        .withColumn("ano_mes", F.date_format(F.col("data_compra"), "yyyy-MM"))

    # Agrupar por mês e segmento para calcular a receita mensal
    receita_mensal_df = vendas_com_segmento_df \
        .groupBy("ano_mes", "segmento_cliente") \
        .agg(F.sum("valor_total_compra").alias("receita_total"))

    # Definir a Window Spec para olhar para o mês anterior dentro de cada segmento
    window_spec = Window.partitionBy("segmento_cliente").orderBy("ano_mes")

    # Usar a função lag() para trazer a receita do mês anterior para a linha atual
    receita_com_mes_anterior_df = receita_mensal_df.withColumn(
        "receita_mes_anterior", F.lag("receita_total", 1).over(window_spec)
    )

    # Calcular o crescimento percentual, tratando divisões por zero
    resultado_final_df = receita_com_mes_anterior_df.withColumn(
        "crescimento_percentual",
        F.when(
            (F.col("receita_mes_anterior").isNotNull()) & (F.col("receita_mes_anterior") > 0),
            (F.col("receita_total") - F.col("receita_mes_anterior")) / F.col("receita_mes_anterior")
        ).otherwise(None) # Retorna nulo se não houver mês anterior para comparar
    ).orderBy("segmento_cliente", "ano_mes")
    
    return resultado_final_df