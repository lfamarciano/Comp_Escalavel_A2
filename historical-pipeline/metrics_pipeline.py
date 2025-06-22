from pyspark.sql import SparkSession

from pathlib import Path

from metric_revenue_growth import revenue_growth
from metric_most_sold_product import most_sold_product_by_quarters
from metric_abandoned_cart_rate import abandoned_cart_rate
from bronze_write_data import write_to_delta
from publish_to_redis import publish_dataframe_to_redis, publish_metric_to_redis

def metrics_pipeline():
    """Função principal que orquestra o cálculo das métricas."""
    
    # Criando sessão Spark
    spark = SparkSession.builder \
        .appName("CalculoMetricasEcommerce") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Definindo caminhos
    bronze_base_path = Path("deltalake/bronze")
    gold_base_path = Path("deltalake/gold")
    
    # Lendo dados da camada bronze
    print("Lendo tabelas da camada Bronze...")
    transacoes_df = spark.read.format("delta").load(str(bronze_base_path / "transacoes_vendas"))
    clientes_df = spark.read.format("delta").load(str(bronze_base_path / "dados_clientes"))
    eventos_web_df = spark.read.format("delta").load(str(bronze_base_path / "eventos_web"))

    # -- Métrica de crescimento de receita --
    print(10 * "=")
    print("\nCalculando crescimento de receita...")
    crescimento_receita_df = revenue_growth(transacoes_df, clientes_df)
    print("\nResultado - Crescimento da Receita por Segmento:")
    crescimento_receita_df.show(truncate=False)
    # Salvando resultado na camada OURO
    metric_name = "crescimento_receita"
    output_path = gold_base_path / metric_name
    print(f"\nSalvando resultado final DeltaLake (camada Ouro): {output_path}")
    write_to_delta(crescimento_receita_df, str(output_path))
    redis_key_name = f"historical:{metric_name}"
    print(f"Publicando resultado final no Redis na chave: {redis_key_name}")
    publish_dataframe_to_redis(crescimento_receita_df, redis_key_name)

    # -- Métrica de produtos mais vendidos --
    print(10 * "=")
    print("\nCalculando produtos mais vendidos por trimestre no último ano...")
    produtos_mais_vendidos_df = most_sold_product_by_quarters(transacoes_df)
    print("\nResultado - Produtos Mais Vendidos por Trimestre:")
    produtos_mais_vendidos_df.show(truncate=False)
    # Salvando resultado na camada OURO
    metric_name = "produtos_mais_vendidos"
    output_path = gold_base_path / metric_name
    print(f"\nSalvando resultado final DeltaLake (camada Ouro): {output_path}")
    write_to_delta(produtos_mais_vendidos_df, str(output_path))
    redis_key_name = f"historical:{metric_name}"
    print(f"Publicando resultado final no Redis na chave: {redis_key_name}")
    publish_dataframe_to_redis(produtos_mais_vendidos_df, redis_key_name)

    # -- Métrica de taxa de abandono de carrinho --
    print(10 * "=")
    print("\nCalculando taxa de abandono de carrinho...")
    metric_name = 'taxa_abandono'
    taxa_abandono = abandoned_cart_rate(transacoes_df, eventos_web_df)
    print("\nResultado - Taxa de Abandono de Carrinho:")
    print(f"A taxa de abandono de carrinho é: {taxa_abandono}")
    redis_key_name = f"historical:{metric_name}"
    print(f"Publicando resultado final no Redis na chave: {redis_key_name}")
    publish_metric_to_redis(taxa_abandono, redis_key_name)

    spark.stop()

if __name__ == "__main__":
    metrics_pipeline()