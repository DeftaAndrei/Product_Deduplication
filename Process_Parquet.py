import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from fuzzywuzzy import fuzz
from collections import defaultdict
import re
import logging
from tqdm import tqdm
import warnings
from pathlib import Path
import os

# Configurare logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def preprocess_url(url):
    """Preprocesează URL-urile pentru comparare"""
    try:
        if pd.isna(url):
            return ""
        # Convertim la lowercase și eliminăm whitespace
        url = str(url).lower().strip()
        # Eliminăm protocol (http/https)
        url = re.sub(r'https?://', '', url)
        # Eliminăm www
        url = re.sub(r'www\.', '', url)
        # Eliminăm parametrii query
        url = url.split('?')[0]
        # Eliminăm trailing slash
        url = url.rstrip('/')
        return url
    except Exception as e:
        logger.error(f"Eroare la preprocesarea URL: {str(e)}")
        return ""

def calculate_url_similarity(url1, url2):
    """Calculează similaritatea între două URL-uri"""
    try:
        if pd.isna(url1) or pd.isna(url2):
            return 0
        
        url1 = preprocess_url(url1)
        url2 = preprocess_url(url2)
        
        if url1 == url2:
            return 1.0
        if not url1 or not url2:
            return 0
        
        # Folosim mai multe metode de similaritate
        ratio = fuzz.ratio(url1, url2)
        partial_ratio = fuzz.partial_ratio(url1, url2)
        token_sort_ratio = fuzz.token_sort_ratio(url1, url2)
        
        # Ponderăm rezultatele
        weighted_similarity = (ratio * 0.4 + partial_ratio * 0.4 + token_sort_ratio * 0.2)
        return weighted_similarity / 100.0
    except Exception as e:
        logger.error(f"Eroare la calculul similarității URL: {str(e)}")
        return 0

def merge_product_info(products_group):
    """Unifică informațiile pentru un grup de produse similare"""
    try:
        if len(products_group) == 0:
            return None
        if len(products_group) == 1:
            return products_group.iloc[0]
        
        merged = {}
        for col in products_group.columns:
            values = products_group[col].dropna().unique()
            if len(values) == 0:
                merged[col] = None
            elif len(values) == 1:
                merged[col] = values[0]
            else:
                # Pentru câmpuri text
                if all(isinstance(v, str) for v in values):
                    # Combinăm informațiile unice, păstrând ordinea
                    merged[col] = ' | '.join(sorted(set(values)))
                else:
                    # Pentru non-text, luăm prima valoare non-nulă
                    merged[col] = values[0]
        return pd.Series(merged)
    except Exception as e:
        logger.error(f"Eroare la unificarea produselor: {str(e)}")
        raise

def find_similar_products(df):
    """Identifică produse similare bazate pe root_domain și page_url"""
    logger.info("Începe identificarea produselor similare...")
    
    try:
        similarity_groups = defaultdict(list)
        processed_indices = set()
        total_rows = len(df)
        
        for i in tqdm(range(total_rows), desc="Analiză similaritate"):
            if i in processed_indices:
                continue
                
            current_group = [i]
            current_domain = df.iloc[i]['root_domain']
            current_url = df.iloc[i]['page_url']
            
            for j in range(i + 1, total_rows):
                if j in processed_indices:
                    continue
                
                # Verificăm mai întâi domain-ul
                if current_domain == df.iloc[j]['root_domain']:
                    # Calculăm similaritatea URL-urilor
                    url_similarity = calculate_url_similarity(
                        current_url,
                        df.iloc[j]['page_url']
                    )
                    
                    if url_similarity > 0.85:  # Prag de similaritate
                        current_group.append(j)
                        processed_indices.add(j)
            
            if len(current_group) > 1:
                group_key = f"group_{len(similarity_groups)}"
                similarity_groups[group_key] = current_group
            processed_indices.add(i)
        
        return similarity_groups
    except Exception as e:
        logger.error(f"Eroare la identificarea produselor similare: {str(e)}")
        raise

def process_parquet_file():
    """Procesează fișierul Parquet și salvează rezultatele în Excel"""
    try:
        input_file = 'veridion_product_deduplication_challenge.snappy.parquet'
        logger.info(f"Începe procesarea fișierului: {input_file}")
        
        # Verificăm existența fișierului
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Fișierul {input_file} nu există în directorul curent!")
        
        # Citim fișierul Parquet
        logger.info("Citire fișier Parquet...")
        table = pq.read_table(input_file)
        df = table.to_pandas()
        logger.info(f"Date încărcate cu succes. Dimensiune inițială: {df.shape}")
        logger.info(f"Coloane disponibile: {df.columns.tolist()}")
        
        # Verificăm existența coloanelor necesare
        required_columns = ['root_domain', 'page_url']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Coloanele necesare nu există în fișier: {missing_columns}")
        
        # Găsim grupurile de produse similare
        logger.info("Începe identificarea produselor similare...")
        similarity_groups = find_similar_products(df)
        logger.info(f"Grupuri de similaritate găsite: {len(similarity_groups)}")
        
        # Procesăm rezultatele
        logger.info("Procesare grupurile de produse similare...")
        result_products = []
        processed_indices = set()
        
        # Procesăm grupurile de produse similare
        for group_key, group_indices in similarity_groups.items():
            logger.debug(f"Procesare grup {group_key} cu {len(group_indices)} produse")
            group_df = df.iloc[group_indices]
            merged_product = merge_product_info(group_df)
            if merged_product is not None:
                result_products.append(merged_product)
            processed_indices.update(group_indices)
        
        # Adăugăm produsele unice
        logger.info("Adăugare produse unice...")
        unique_products = df[~df.index.isin(processed_indices)]
        result_products.extend(unique_products.to_dict('records'))
        
        # Creăm DataFrame-ul final și salvăm în Excel
        logger.info("Creare și salvare rezultat final...")
        result_df = pd.DataFrame(result_products)
        result_df.to_excel('Result.xlsx', index=False)
        
        # Afișăm statistici
        logger.info(f"\nStatistici finale:")
        logger.info(f"Număr inițial de produse: {len(df)}")
        logger.info(f"Număr final de produse: {len(result_df)}")
        logger.info(f"Reducere: {((len(df) - len(result_df)) / len(df) * 100):.2f}%")
        logger.info(f"Rezultate salvate în: Result.xlsx")
        
        return result_df
    
    except Exception as e:
        logger.error("Eroare în procesul de analiză:")
        logger.exception(e)
        raise

if __name__ == "__main__":
    try:
        logger.info("Începe procesarea fișierului Parquet...")
        result_df = process_parquet_file()
        logger.info("Procesare finalizată cu succes!")
    except FileNotFoundError as e:
        logger.error(f"Fișierul nu a fost găsit: {str(e)}")
    except Exception as e:
        logger.error(f"Eroare neașteptată: {str(e)}")
        logger.exception("Detalii eroare:")