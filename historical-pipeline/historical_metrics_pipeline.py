from pyspark.sql import SparkSession

from pathlib import Path

from metrics_jobs import calculate_revenue_growth

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
    # TODO: Substituiremos pelo Redis
    # gold_base_path = Path("deltalake/gold")
    
    # Lendo dados da camada bronze
    print("Lendo tabelas da camada Bronze...")
    transacoes_df = spark.read.format("delta").load(str(bronze_base_path / "transacoes_vendas"))
    clientes_df = spark.read.format("delta").load(str(bronze_base_path / "dados_clientes"))

    # Calculando métrica de crescimento de receita
    print("Calculando crescimento de receita...")
    crescimento_receita_df = calculate_revenue_growth(transacoes_df, clientes_df)
    
    print("\nResultado - Crescimento da Receita por Segmento:")
    crescimento_receita_df.show(truncate=False)

    # 5. SALVAR O RESULTADO NA CAMADA GOLD
    print(f"SALVANDO METRICA NO REDIS")
    # output_path = gold_base_path / "crescimento_receita"
    # print(f"\nSalvando resultado em: {output_path}")
    # crescimento_receita_df.write.format("delta").mode("overwrite").save(str(output_path))
    
    print("\nMétrica de crescimento de receita calculada e salva com sucesso!")
    spark.stop()

if __name__ == "__main__":
    print(30 * "=")
    print("RUNNING MAIN")
    print(30 * "=")
    main()