import pandas as pd
import numpy as np
from difflib import SequenceMatcher

def similar(a, b):
    """Calculează similaritatea între două șiruri de caractere"""
    if pd.isna(a) or pd.isna(b):
        return 0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def merge_rows(row1, row2):
    """Unește două rânduri, păstrând informațiile cele mai complete"""
    merged = {}
    for col in row1.index:
        if pd.isna(row1[col]) and not pd.isna(row2[col]):
            merged[col] = row2[col]
        elif not pd.isna(row1[col]) and pd.isna(row2[col]):
            merged[col] = row1[col]
        else:
            # Dacă ambele au valori, păstrăm cea mai lungă
            merged[col] = row1[col] if len(str(row1[col])) >= len(str(row2[col])) else row2[col]
    return pd.Series(merged)

def deduplicate_products():
    print("Se citește fișierul Excel...")
    df = pd.read_excel('veridion_product_deduplication_challenge.xlsx')
    
    # Selectăm doar coloanele A-F
    columns_to_compare = df.columns[:6]
    print(f"Coloanele analizate: {columns_to_compare.tolist()}")
    
    # Inițializăm lista pentru produsele deduplicate
    deduplicated_products = []
    processed_indices = set()
    
    # Parcurgem fiecare rând
    for i in range(len(df)):
        if i in processed_indices:
            continue
            
        current_row = df.iloc[i]
        similar_rows = []
        
        # Căutăm rânduri similare
        for j in range(i + 1, len(df)):
            if j in processed_indices:
                continue
                
            similarity_score = 0
            for col in columns_to_compare:
                similarity_score += similar(current_row[col], df.iloc[j][col])
            
            # Calculăm media similarității
            avg_similarity = similarity_score / len(columns_to_compare)
            
            # Dacă similaritatea este mai mare de 0.8 (80%), considerăm că sunt același produs
            if avg_similarity > 0.8:
                similar_rows.append(j)
        
        # Dacă am găsit rânduri similare, le unificăm
        if similar_rows:
            merged_row = current_row
            for idx in similar_rows:
                merged_row = merge_rows(merged_row, df.iloc[idx])
                processed_indices.add(idx)
            
            deduplicated_products.append(merged_row)
            processed_indices.add(i)
        else:
            deduplicated_products.append(current_row)
            processed_indices.add(i)
    
    # Creăm un nou DataFrame cu produsele deduplicate
    result_df = pd.DataFrame(deduplicated_products)
    
    
    output_file = 'veridion_product_deduplication_challenge_deduplicated.xlsx'
    print(f"Se salvează rezultatele în: {output_file}")
    result_df.to_excel(output_file, index=False)
    
    print(f"\nStatistici:")
    print(f"Număr inițial de produse: {len(df)}")
    print(f"Număr final de produse după deduplicare: {len(result_df)}")
    print(f"Număr de produse duplicate găsite: {len(df) - len(result_df)}")

if __name__ == "__main__":
    deduplicate_products() 