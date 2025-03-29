import pandas as pd
import pyarrow.parquet as pq

# Citim fișierul parquet
df = pd.read_parquet('veridion_product_deduplication_challenge.snappy.parquet')

# Afișăm primele câteva rânduri și informații despre DataFrame
print("\nPrimele 5 rânduri din DataFrame:")
print(df.head())

print("\nInformații despre DataFrame:")
print(df.info())

print("\nColoanele disponibile:")
