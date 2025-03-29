import pandas as pd
import pyarrow.parquet as pq

def convert_parquet_to_excel():
    # Citim fișierul parquet
    print("Se citește fișierul parquet...")
    df = pd.read_parquet('veridion_product_deduplication_challenge.snappy.parquet')
    
    # Salvăm în Excel
    output_file = 'veridion_product_deduplication_challenge.xlsx'
    print(f"Se salvează în Excel: {output_file}")
    df.to_excel(output_file, index=False)
    print("Conversie finalizată cu succes!")

if __name__ == "__main__":
    convert_parquet_to_excel()


