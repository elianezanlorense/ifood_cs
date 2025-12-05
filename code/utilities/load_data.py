
import requests
import tarfile
import io
import urllib.request
import gzip
import json
import pandas as pd
from pathlib import Path 
import numpy as np

### Load data
def load_data(url: str):
    """
    Lê dados a partir de uma URL.
    Funciona automaticamente para:
    - CSV
    - CSV.gz
    - TAR.GZ contendo um único CSV (ignora arquivos ocultos do macOS)
    Retorna um DataFrame Pandas.
    """
  
    try:
        df = pd.read_csv(url, compression="infer")
        return df
    except Exception:
        pass 

   
    try:
        response = requests.get(url)
        response.raise_for_status()

        tar_bytes = io.BytesIO(response.content)

        with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
            membros = tar.getnames()
            
           
            membros_validos = [m for m in membros if not m.startswith("._")]

            if len(membros_validos) != 1:
                raise Exception(f"Multiple or no CSV files found: {membros_validos}")

            nome_csv = membros_validos[0]
            arquivo_csv = tar.extractfile(nome_csv)

            df = pd.read_csv(arquivo_csv)
            return df

    except Exception as e:
        print(f" Erro ao carregar dados da URL: {e}")
        return None

#### Load orders
def load_orders(
    url: str,
    customer_ids: list,
    columns_to_drop: list = None,
    max_lines: int = None
):
    """
    Lê um arquivo .json.gz linha a linha de uma URL,
    filtra pelos customer_ids fornecidos e retorna um DataFrame.

    Parâmetros:
    -----------
    url : str
        URL do arquivo .json.gz
    customer_ids : list
        Lista de customer_ids válidos (string ou int)
    columns_to_drop : list (opcional)
        Lista de colunas que serão removidas
    max_lines : int (opcional)
        Máximo de linhas a processar (None = arquivo completo)
    """

    customer_ids = set(map(str, customer_ids))  
    columns_to_drop = columns_to_drop or []     

    resp = urllib.request.urlopen(url)
    gz = gzip.GzipFile(fileobj=resp)
    text_stream = io.TextIOWrapper(gz, encoding="utf-8")

    rows = []

    for i, line in enumerate(text_stream, start=1):
        record = json.loads(line)

        # filtrar pelo customer_id
        if str(record.get("customer_id")) in customer_ids:

            # remover colunas
            for col in columns_to_drop:
                record.pop(col, None)

            rows.append(record)

        # limite opcional de linhas
        if max_lines and i >= max_lines:
            break

    return pd.DataFrame(rows)

##### Checa duplicidade
def check_key_uniqueness(df: pd.DataFrame, cols):
    """
    Verifica NOT NULL e UNIQUE nas colunas fornecidas.
    Retorna:
      - (False, df_nulls, null_counts, null_indices) se houver nulos
      - (False, df_dupes, None, None) se houver duplicações
      - (True, None, None, None) se estiver tudo OK
    """
    # Garante que 'cols' é lista
    if isinstance(cols, str):
        cols = [cols]

    # 1. Verificar NOT NULL
    df_nulls = df[df[cols].isna().any(axis=1)]
    if not df_nulls.empty:
        null_counts = df[cols].isna().sum()          # soma de nulos por coluna
        null_indices = df_nulls.index.tolist()        # índices com nulos
        
        print(f"❌ Colunas {cols} contêm valores nulos.")
        print("\nSoma de nulos por coluna:")
        print(null_counts)
        print("\nÍndices com nulos:")
        print(null_indices)

        return False, df_nulls, null_counts, null_indices

    # 2. Verificar UNIQUE
    df_dupes = df[df.duplicated(subset=cols, keep=False)]
    if not df_dupes.empty:
        print(f"❌ Colunas {cols} possuem duplicações.")
        return False, df_dupes, None, None

    # Tudo OK
    print(f"✅ Colunas {cols} são NOT NULL e UNIQUE.")
    return True, None, None, None

##### Count null
def count_null(df: pd.DataFrame):
    total = df.isnull().sum().sort_values(ascending=False)
    percent = (df.isnull().sum())/df.isnull().count().sort_values(ascending=False)
    missing_data = pd.concat([total, percent], axis=1, keys=['Total','Percent'], sort=False).sort_values('Total', ascending=False)
    missing_data.head(150)
    return missing_data