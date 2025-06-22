from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from datetime import datetime

def abandoned_cart_rate(transacoes_df: DataFrame, eventos_web_df: DataFrame) -> float:
    """
    Calcula a taxa de abandono de carrinho de compras.
    """

    # Filtrando eventos de carrinho criado
    carrinhos_criados = eventos_web_df.filter(
        F.col("tipo_evento") == "carrinho_criado"
    ).select(
        "id_carrinho"
    ).distinct()

    # Contando o total de carrinhos criados
    total_carrinhos = carrinhos_criados.count()

    # Calculando número de carrinhos finalizados e abandonados
    carrinhos_finalizados = transacoes_df.select("id_carrinho").distinct()
    carrinhos_abandonados = carrinhos_criados.join(
        carrinhos_finalizados,
        on="id_carrinho",
        how="left_anti"
    )

    total_carrinhos_abandonados = carrinhos_abandonados.count()

    # Calculando a taxa de abandono
    taxa_abandono = (
        total_carrinhos_abandonados / total_carrinhos * 100
        if total_carrinhos > 0 else 0
    )

    return taxa_abandono
