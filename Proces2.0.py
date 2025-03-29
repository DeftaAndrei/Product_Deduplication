import pandas as pd 
import numpy as np 
import pyarrow.parquet as pq
from fuzzywuzzy import fuzz 
from collections import defaultdict
import logging
from tqdm import tqdm
import warnings
import os

# Configurare logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def preprocess_text(text):
  
    try:
        if pd.isna(text):
            return ""
        # Convertim la lowercase și eliminăm whitespace
        text = str(text).lower().strip()
        # Eliminăm caracterele speciale
        text = ''.join(c for c in text if c.isalnum() or c.isspace())
        return text
    except Exception as e:
        logger.error(f"Eroare la preprocesarea textului: {str(e)}")
        return ""

def calculate_similarity(text1, text2):
    try:
        if pd.isna(text1) or pd.isna(text2):
            return 0
        text1 = preprocess_text(text1)
        text2 = preprocess_text(text2)
        
        if text1 == text2:
            return 1.0
        if not text1 or not text2:
            return 0
        
        # Folosim mai multe metode de similaritate
        ratio = fuzz.ratio(text1, text2)
        partial_ratio = fuzz.partial_ratio(text1, text2)
        token_sort_ratio = fuzz.token_sort_ratio(text1, text2)
        
        # Ponderăm rezultatele
        weighted_similarity = (ratio * 0.4 + partial_ratio * 0.4 + token_sort_ratio * 0.2)
        return weighted_similarity / 100.0
    except Exception as e:
        logger.error(f"Eroare la calculul similarității: {str(e)}")
        return 0

def merge_descriptions(descriptions):
  
    try:
        if not descriptions:
            return ""
        
        # Eliminăm valorile nule și convertim la string
        valid_descriptions = [str(d).strip() for d in descriptions if pd.notna(d)]
        if not valid_descriptions:
            return ""
        
        # Eliminăm duplicatele exacte
        unique_descriptions = list(dict.fromkeys(valid_descriptions))
        
        # Sortăm descrierile după lungime (cele mai scurte primele) Nu stiu cum sa fac asta precis dar incerc asa 
        unique_descriptions.sort(key=len)
        
        # Combinăm descrierile, evitând repetarea informațiilor in asa fel daca gasim un produs duplicat il eliminam si ii adaugam ii facem o descriere mai ampla 
        final_descriptions = []
        for desc in unique_descriptions:
            # Verificăm dacă descrierea curentă este conținută în vreuna din descrierile anterioare
            is_contained = False
            for prev_desc in final_descriptions:
                if desc.lower() in prev_desc.lower():
                    is_contained = True
                    break
            
            if not is_contained:
                final_descriptions.append(desc)
        
        # Conectăm descrierile cu " | " și adăugăm puncte la final dacă e necesar
        result = " | ".join(final_descriptions)
        if not result.endswith('.'):
            result += '.'
        
        return result
    except Exception as e:
        logger.error(f"Eroare la combinarea descrierilor: {str(e)}")
        return ""

def find_duplicates_new(df):
   
    logger.info("Începe identificarea duplicatelor...")
    
    try:
        similarity_groups = defaultdict(list)
        processed_indices = set()
        total_rows = len(df)
        
        for i in tqdm(range(total_rows), desc="Analiză duplicate"):
            if i in processed_indices:
                continue
                
            current_group = [i]
            current_title = df.iloc[i]['product_title']
            current_name = df.iloc[i]['product_name']
            
            for j in range(i + 1, total_rows):
                if j in processed_indices:
                    continue
                
                # Calculăm similaritatea pentru ambele coloane
                title_similarity = calculate_similarity(current_title, df.iloc[j]['product_title'])
                name_similarity = calculate_similarity(current_name, df.iloc[j]['product_name'])
                
                # Considerăm duplicate dacă ambele coloane sunt similare
                if title_similarity > 0.85 and name_similarity > 0.85:
                    current_group.append(j)
                    processed_indices.add(j)
            
            if len(current_group) > 1:
                group_key = f"group_{len(similarity_groups)}"
                similarity_groups[group_key] = current_group
            processed_indices.add(i)
        
        logger.info(f"Grupuri de duplicate găsite: {len(similarity_groups)}")
        return similarity_groups
    
    except Exception as e:
        logger.error(f"Eroare la identificarea duplicatelor: {str(e)}")
        raise

def process_data():
    """Procesează fișierul Excel și salvează rezultatele"""
    try:
        
        excel_files = [f for f in os.listdir('.') if f.endswith('.xlsx') and not f.startswith('~$')]
        if not excel_files:
            raise FileNotFoundError("Nu s-a găsit niciun fișier Excel valid în directorul curent!")
        
        input_file = excel_files[0] 
        logger.info(f"Fișier Excel găsit: {input_file}")
        
      
        logger.info("Citire fișier Excel...")
        df = pd.read_excel(input_file)
        logger.info(f"Date încărcate cu succes. Dimensiune inițială: {df.shape}")
        logger.info(f"Coloane disponibile: {df.columns.tolist()}")
        
        
        required_columns = ['product_title', 'product_name', 'product_summary']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Coloanele necesare nu există în fișier: {missing_columns}")
        
        duplicate_groups = find_duplicates_new(df)
        
       
        logger.info("Procesare grupurile de duplicate...")
        result_rows = []
        processed_indices = set()
        
        
        for name, indices in duplicate_groups.items():
            logger.debug(f"Procesare grup cu {len(indices)} duplicate")
            group_df = df.iloc[indices]
            
           
            new_row = group_df.iloc[0].copy()  # Luăm primul rând ca bază
            
            # Combinăm descrierile din product_summary :)) Tot mai sus
            descriptions = group_df['product_summary'].dropna().unique()
            new_row['product_summary'] = merge_descriptions(descriptions)
            
            result_rows.append(new_row)
            processed_indices.update(indices)
        
        # Adăugăm rândurile unice (cele care nu sunt în niciun grup)
        unique_rows = df[~df.index.isin(processed_indices)]
        result_rows.extend(unique_rows.to_dict('records'))
        
        # Creăm DataFrame-ul final și salvăm în Excel
        logger.info("Creare și salvare rezultat final...")
        result_df = pd.DataFrame(result_rows)
        result_df.to_excel('Rezult.xlsx', index=False)
        
        # Afișăm statistici
        logger.info(f"\nStatistici finale:")
        logger.info(f"Număr inițial de rânduri: {len(df)}")
        logger.info(f"Număr final de rânduri: {len(result_df)}")
        logger.info(f"Reducere: {((len(df) - len(result_df)) / len(df) * 100):.2f}%")
        logger.info(f"Rezultate salvate în: Rezult.xlsx")
        
        return result_df
    
    except Exception as e:
        logger.error("Eroare în procesul de analiză:")
        logger.exception(e)
        raise

if __name__ == "__main__":
    try:
        logger.info("Începe procesarea fișierului Excel...")
        result_df = process_data()
        logger.info("Procesare finalizată cu succes!")
    except FileNotFoundError as e:
        logger.error(f"Fișierul nu a fost găsit: {str(e)}")
    except Exception as e:
        logger.error(f"Eroare neașteptată: {str(e)}")
        logger.exception("Detalii eroare:")
