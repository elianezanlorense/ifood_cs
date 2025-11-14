import pandas as pd
import pandas as pd
import requests
import tarfile
import io

def load_data(url: str):
    """
    Lê dados a partir de uma URL.
    Funciona automaticamente para:
    - CSV
    - CSV.gz
    - TAR.GZ contendo um único CSV (ignora arquivos ocultos do macOS)
    Retorna um DataFrame Pandas.
    """
    # Tenta primeiro como CSV normal (.csv ou .csv.gz)
    try:
        df = pd.read_csv(url, compression="infer")
        return df
    except Exception:
        pass  # tenta como TAR.GZ

    # Tenta ler como TAR.GZ contendo CSV
    try:
        response = requests.get(url)
        response.raise_for_status()

        tar_bytes = io.BytesIO(response.content)

        with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
            membros = tar.getnames()
            
            # Ignora arquivos ocultos do macOS como "._arquivo.csv"
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


def join_group_count(df1, df2, key, how, group_col):
    return (
        df1.merge(df2, on=key, how=how)
           .groupby(group_col)
           .size()
           .reset_index(name="count")
           .sort_values(group_col)
    )

from pathlib import Path

def save_parquet(df, folder_name, filename):
    folder = Path(folder_name)
    folder.mkdir(parents=True, exist_ok=True)

    path = folder / filename
    df.to_parquet(path, index=False)

    print(f"Arquivo Parquet salvo em: {path.resolve()}")
    return path

def merge_df(df1, df2, key, how='left'):
    """
    Junta dois DataFrames e agrupa/conta, garantindo que os valores NULL 
    na coluna de agrupamento (resultantes da junção) sejam incluídos.
    """
    
    # NOTA: O 'left' ou 'outer' merge é crucial para preservar as linhas 
    # que podem ter NULL na coluna de agrupamento após a junção.
    merged_df = df1.merge(df2, on=key, how=how)

    return merged_df


def join_group_count_with_nulls(df1, df2, key, how='left', group_col=''):
    """
    Junta dois DataFrames e agrupa/conta, garantindo que os valores NULL 
    na coluna de agrupamento (resultantes da junção) sejam incluídos.
    """
    
    # NOTA: O 'left' ou 'outer' merge é crucial para preservar as linhas 
    # que podem ter NULL na coluna de agrupamento após a junção.
    merged_df = df1.merge(df2, on=key, how=how)
    
    # O Pandas, por padrão, agrupa NaN em sua própria categoria
    return (
        merged_df
           .groupby(group_col)
           .size()
           .reset_index(name="count")
           .sort_values("count", ascending=False)
    )