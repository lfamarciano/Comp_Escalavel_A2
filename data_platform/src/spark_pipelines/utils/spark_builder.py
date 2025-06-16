# 2_data_platform/src/spark_pipelines/utils/spark_builder.py

from pyspark.sql import SparkSession

class SparkBuilder:
    """
    Classe utilitária para construir uma SparkSession com as configurações
    necessárias para o projeto (Delta Lake).
    """
    def __init__(self, app_name: str = "DataPlatform"):
        self.app_name = app_name
        # Configurações essenciais para usar Delta Lake com Spark
        self.delta_config = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }

    def build(self) -> SparkSession:
        """
        Constrói e retorna a SparkSession.
        """
        print("Construindo a sessão Spark...")
        
        session = (
            SparkSession.builder.appName(self.app_name)
            .config(map=self.delta_config)
            .getOrCreate()
        )
        
        print("Sessão Spark construída com sucesso.")
        return session