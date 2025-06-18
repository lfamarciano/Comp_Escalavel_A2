from pyspark.sql import SparkSession

from pathlib import Path

from metrics_jobs import calculate_daily_revenue_metrics, most_sold_product_by_quarters, abandoned_cart_rate

def main():
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

    # Calculando métrica de crescimento de receita
    print("Calculando crescimento de receita...")
    receita_diaria_historica_df = calculate_daily_revenue_metrics(transacoes_df, clientes_df)
    
    print("\nResultado - Crescimento da Receita por Segmento:")
    receita_diaria_historica_df.show(truncate=False)

    # Salvando resultado na camada OURO
    output_path = gold_base_path / "crescimento_receita"
    print(f"\nSalvando resultado final DeltaLake (camada Ouro): {output_path}")
    receita_diaria_historica_df.write.format("delta").mode("overwrite").save(str(output_path))

    print("\nMétrica de crescimento de receita calculada e salva com sucesso!")

    # Calculando métrica de produtos mais vendidos por trimestre no último ano
    print("Calculando produtos mais vendidos por trimestre no último ano...")
    produtos_mais_vendidos_df = most_sold_product_by_quarters(transacoes_df)

    print("\nResultado - Produtos Mais Vendidos por Trimestre:")
    produtos_mais_vendidos_df.show(truncate=False)

    print(f"\nMétrica  de produtos mais vendidos por trimestre calculada com sucesso!")

    # Calculando taxa de abandono de carrinho
    print("Calculando taxa de abandono de carrinho...")
    taxa_abandono = abandoned_cart_rate(transacoes_df, eventos_web_df)

    print("\nResultado - Taxa de Abandono de Carrinho:")
    print(f"A taxa de abandono de carrinho é: {taxa_abandono}")

    spark.stop()

if __name__ == "__main__":
    print(30 * "=")
    print("RUNNING MAIN")
    print(30 * "=")
    main()