from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.dataframe import DataFrame

def clean_transactions_data(df: DataFrame) -> DataFrame:
    """Aplica a limpeza na tabela de transações."""
    return df.withColumn(
        "data_compra_ts", to_timestamp(col("data_compra"))
    ).withColumn(
        "valor_total_compra", col("valor_total_compra").cast("double")
    ).select(
        "id_transacao", "id_pedido", "id_usuario", "id_produto", "quantidade_produto",
        "valor_total_compra", col("data_compra_ts").alias("data_compra"),
        "metodo_pagamento", "status_pedido", "id_carrinho"
    )

def clean_clients_data(df: DataFrame) -> DataFrame:
    """Aplica a limpeza na tabela de clientes."""
    return df.withColumn(
        "data_cadastro_ts", to_timestamp(col("data_cadastro"))
    ).select(
        "id_usuario", "nome_usuario", "email_usuario",
        col("data_cadastro_ts").alias("data_cadastro"),
        "segmento_cliente", "cidade", "estado", "pais"
    )

def clean_products_data(df: DataFrame) -> DataFrame:
    """Aplica a limpeza na tabela de catálogo."""
    return df.withColumn(
        "preco_unitario", col("preco_unitario").cast("double")
        ).withColumn(
        "estoque_disponivel", col("estoque_disponivel").cast("integer")
        ).select(
            "id_produto", "nome_produto", "descricao_produto",
            "categoria", "preco_unitario", "estoque_disponivel"
            )
    
def clean_web_events_data(df: DataFrame) -> DataFrame:
    """Aplica a limpeza na tabela de eventos web."""
    return df.withColumn(
        "timestamp_evento_ts", to_timestamp(col("timestamp_evento"))
    ).select(
        "id_evento", "id_usuario", "id_sessao", "id_carrinho",
        "tipo_evento", "id_produto",
        col("timestamp_evento_ts").alias("timestamp_evento")
    )

if __name__ == "__main___":
    # Limpeza e tipagem para transacoes_vendas -> tv_df
    tv_df_cleaned = tv_df_raw.withColumn(
        "data_compra_ts", to_timestamp(col("data_compra"))
    ).withColumn(
        "valor_total_compra", col("valor_total_compra").cast("double")
    ).select(
        col("id_transacao"),
        col("id_pedido"),
        col("id_usuario"),
        col("id_produto"),
        col("quantidade_produto"),
        col("valor_total_compra"),
        col("data_compra_ts").alias("data_compra"),
        col("metodo_pagamento"),
        col("status_pedido"),
        col("id_carrinho")
    )

    # Limpeza e tipagem para dados_clientes -> dc_df
    dc_df_cleaned = dc_df_raw.withColumn(
        "data_cadastro_ts", to_timestamp(col("data_cadastro"))
    ).select(
        col("id_usuario"),
        col("nome_usuario"),
        col("email_usuario"),
        col("data_cadastro_ts").alias("data_cadastro"),
        col("segmento_cliente"),
        col("cidade"),
        col("estado"),
        col("pais")
    )