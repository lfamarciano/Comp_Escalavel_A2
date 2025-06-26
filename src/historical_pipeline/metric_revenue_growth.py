from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

def revenue_growth(transacoes_df: DataFrame, clientes_df: DataFrame) -> DataFrame:
    """
    Calcula métricas de receita com granularidade DIÁRIA, segmentado por cliente.
    """
    
    # Truncando data para dia
    vendas_com_segmento_df = transacoes_df.join(clientes_df, "id_usuario", "left") \
        .withColumn("data", F.to_date(F.col("data_compra")))

    # Agrupando dados por dia e por segmento de cliente
    receita_diaria_df = vendas_com_segmento_df \
        .groupBy("data", "segmento_cliente") \
        .agg(
            F.sum("valor_total_compra").alias("receita_total_diaria"),
            F.count("id_transacao").alias("numero_de_pedidos")
        )

    # Definindo window_spec para olhar para dia anterior
    window_spec = Window.partitionBy("segmento_cliente").orderBy("data")

    # Calculando crescimento dia-a-dia
    receita_com_dia_anterior_df = receita_diaria_df.withColumn(
        "receita_dia_anterior", F.lag("receita_total_diaria", 1).over(window_spec)
    )

    resultado_final_df = receita_com_dia_anterior_df.withColumn(
        "crescimento_receita_d-1",
        F.when(
            (F.col("receita_dia_anterior").isNotNull()) & (F.col("receita_dia_anterior") > 0),
            (F.col("receita_total_diaria") - F.col("receita_dia_anterior")) / F.col("receita_dia_anterior")
        ).otherwise(None)
    ).orderBy(F.col("data").desc(), "segmento_cliente")
    
    return resultado_final_df