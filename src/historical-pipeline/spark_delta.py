from pathlib import Path
from pyspark.sql.dataframe import DataFrame

deltalake_bronze_path = Path("deltalake\\bronze")

def write_to_delta(df: DataFrame, delta_path: str):
    """Escreve um DataFrame para uma tabela Delta, sobrescrevendo os dados."""
    print(f"Escrevendo dados em: {delta_path}")
    df.write.format("delta").mode("overwrite").save(delta_path)