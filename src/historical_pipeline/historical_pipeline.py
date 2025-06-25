from bronze_pipeline import bronze_pipeline
from metrics_pipeline import metrics_pipeline

print(40 * "=")
print(f"Running bronze pipeline...")
bronze_pipeline()
print(f"\nFinished bronze pipeline")

print(40 * "=")
print(f"Running bronze pipeline...")
metrics_pipeline()
print(f"\nFinished bronze pipeline")
