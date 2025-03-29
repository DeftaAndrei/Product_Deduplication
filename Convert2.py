import pandas as pd
import pyarrow.parquet as pq
import time

def convert_parquet_to_excel():
    print("Începe conversia fișierelor parquet în Excel...")
    
    # Citim fișierele parquet
    print("Se citesc fișierele parquet...")
    table1 = pq.read_table('veridion_product_deduplication_challenge.snappy.parquet')
   
    
    # Convertim în DataFrame
    data_set1 = table1.to_pandas()
    
    
    # Salvăm în Excel
    print("Se salvează Tensorflow.parquet în Excel...")
    data_set1.to_excel('1.xlsx', index=False)
    
    
   

if __name__ == "__main__":
    start_time = time.time()
    convert_parquet_to_excel()
    end_time = time.time()
    print(f"\nTimpul total de execuție: {end_time - start_time:.2f} secunde")
