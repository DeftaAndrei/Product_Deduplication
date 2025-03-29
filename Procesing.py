import numpy as np 
import pandas as pd
import Levenshtein
import logging
from tqdm import tqdm

# Configurăm logging pentru debug
logging.basicConfig(level=logging.DEBUG, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data(file_path):
    """Încarcă datele din fișierul Excel"""
    logger.info("Se încarcă datele din %s", file_path)
    try:
        df = pd.read_excel(file_path)
        logger.info("Date încărcate cu succes. Dimensiune: %s", df.shape)
        # Afișăm primele rânduri pentru verificare
        logger.debug("Primele rânduri din date:\n%s", df.head())
        logger.debug("Coloanele disponibile: %s", df.columns.tolist())
        return df
    except Exception as e:
        logger.error("Eroare la încărcarea datelor: %s", str(e))
        raise

def calculate_similarity(str1, str2):
    """Calculează similaritatea între două șiruri folosind Levenshtein"""
    try:
        if pd.isna(str1) or pd.isna(str2):
            return 0
        str1, str2 = str(str1).lower().strip(), str(str2).lower().strip()
        if str1 == str2:
            return 1.0
        if len(str1) == 0 or len(str2) == 0:
            return 0
        distance = Levenshtein.distance(str1, str2)
        max_len = max(len(str1), len(str2))
        similarity = 1 - (distance / max_len)
        return similarity
    except Exception as e:
        logger.error("Eroare la calculul similarității: %s", str(e))
        return 0

def merge_product_info(row1, row2):
    """Unește informațiile a două produse similare"""
    try:
        merged = {}
        for col in row1.index:
            # Dacă unul dintre câmpuri e gol, luăm valoarea non-nulă
            if pd.isna(row1[col]) and not pd.isna(row2[col]):
                merged[col] = row2[col]
            elif not pd.isna(row1[col]) and pd.isna(row2[col]):
                merged[col] = row1[col]
            else:
                # Pentru câmpuri text
                if isinstance(row1[col], str) and isinstance(row2[col], str):
                    # Dacă textele sunt identice, păstrăm unul
                    if row1[col].lower().strip() == row2[col].lower().strip():
                        merged[col] = row1[col]
                    else:
                        # Combinăm informațiile unice
                        combined = set(row1[col].split()) | set(row2[col].split())
                        merged[col] = ' '.join(combined)
                else:
                    # Pentru valori non-text, păstrăm prima valoare non-nulă
                    merged[col] = row1[col] if not pd.isna(row1[col]) else row2[col]
        return pd.Series(merged)
    except Exception as e:
        logger.error("Eroare la unificarea rândurilor: %s", str(e))
        raise

def find_similar_products(df, threshold=0.85):
    """Găsește produse similare în DataFrame"""
    logger.info("Începe căutarea produselor similare cu threshold %s", threshold)
    similar_groups = []
    processed_indices = set()
    
    # Convertim primele 6 coloane la string pentru comparație
    comparison_df = df.iloc[:, :6].astype(str)
    total_rows = len(df)
    
    for i in tqdm(range(total_rows), desc="Procesare produse"):
        if i in processed_indices:
            continue
            
        current_group = [i]
        current_row = comparison_df.iloc[i]
        
        # Comparăm cu restul produselor
        for j in range(i + 1, total_rows):
            if j in processed_indices:
                continue
                
            # Calculăm similaritatea pentru fiecare coloană
            similarities = []
            for col in comparison_df.columns:
                sim = calculate_similarity(current_row[col], comparison_df.iloc[j][col])
                similarities.append(sim)
            
            # Media similarităților
            avg_similarity = np.mean(similarities)
            
            if avg_similarity > threshold:
                current_group.append(j)
                logger.debug(f"Produs similar găsit: {i} - {j} (similaritate: {avg_similarity:.2f})")
        
        if len(current_group) > 1:
            similar_groups.append(current_group)
            processed_indices.update(current_group)
            logger.info(f"Grup nou găsit: {current_group}")
        else:
            processed_indices.add(i)
    
    return similar_groups

def deduplicate_products(input_file, output_file):
    """Funcția principală pentru deduplicarea produselor"""
    logger.info("Începe procesul de deduplicare")
    try:
        # Încărcăm datele
        df = load_data(input_file)
        initial_count = len(df)
        logger.info("Număr inițial de produse: %s", initial_count)
        
        # Găsim grupurile de produse similare
        similar_groups = find_similar_products(df)
        logger.info("Număr de grupuri similare găsite: %s", len(similar_groups))
        
        # Procesăm grupurile și creăm noul DataFrame
        deduplicated_products = []
        processed_indices = set()
        
        # Procesăm grupurile de produse similare
        for group in similar_groups:
            merged_row = df.iloc[group[0]]
            for idx in group[1:]:
                merged_row = merge_product_info(merged_row, df.iloc[idx])
            deduplicated_products.append(merged_row)
            processed_indices.update(group)
        
        # Adăugăm produsele unice (care nu au duplicate)
        for i in range(len(df)):
            if i not in processed_indices:
                deduplicated_products.append(df.iloc[i])
        
        # Creăm DataFrame-ul final
        result_df = pd.DataFrame(deduplicated_products)
        
        # Salvăm rezultatele
        result_df.to_excel(output_file, index=False)
        final_count = len(result_df)
        
        # Afișăm statistici
        logger.info("Rezultate salvate în: %s", output_file)
        logger.info("Statistici finale:")
        logger.info("- Produse inițiale: %d", initial_count)
        logger.info("- Produse după deduplicare: %d", final_count)
        logger.info("- Duplicate găsite și unificate: %d", initial_count - final_count)
        
        return result_df
    
    except Exception as e:
        logger.error("Eroare în procesul de deduplicare: %s", str(e))
        raise

if __name__ == "__main__":
    try:
        input_file = 'veridion_product_deduplication_challenge.xlsx'
        output_file = 'veridion_product_deduplication_challenge_deduplicated.xlsx'
        deduplicate_products(input_file, output_file)
    except Exception as e:
        logger.error("Eroare la rularea programului: %s", str(e))