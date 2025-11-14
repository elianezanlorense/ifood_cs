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


def matriz_migracao(df, mes_0, mes_1):
    """
    Gera matriz de migração entre dois meses:
        - mes_0 → mês anterior
        - mes_1 → mês seguinte
    Retorna DataFrame com:
        mes_{mes_0}, mes_{mes_1}, is_target, total_clientes
    """

    nome_col_0 = f"mes_{mes_0}"
    nome_col_1 = f"mes_{mes_1}"

    # 1) Criar flags por cliente
    clientes_temp = (
        df
        .groupby(["customer_id", "is_target"])
        .agg(
            **{
                nome_col_0: ("order_created_month", lambda s: int((s == mes_0).any())),
                nome_col_1: ("order_created_month", lambda s: int((s == mes_1).any())),
            }
        )
        .reset_index()
    )

    # 2) Contar clientes na matriz
    resultado = (
        clientes_temp
        .groupby([nome_col_0, nome_col_1, "is_target"])["customer_id"]
        .nunique()  # conta clientes únicos
        .reset_index(name="total_clientes")
        .sort_values([nome_col_0, nome_col_1])
    )

    return resultado




    """
    Pandas implementation of the order processing pipeline:
      - Creates unique_order_hash
      - Calculates num_pedidos (historic and monthly)
      - Calculates prev_order_time and diff_days
      - Calculates monthly distribution (df_percent)
    
    Returns:
      - df_final (Pandas DataFrame) already sorted by:
          customer_id DESC, order_created_at ASC
      - df_percent (Pandas DataFrame with monthly distributions)
    """

    # Ensure 'order_created_at' is datetime and drop 'order_id'
   
    df['order_created_at'] = pd.to_datetime(df['order_created_at'])
    df['order_created_month'] = df['order_created_at'].dt.month
    df = df.drop(columns=['order_id'], errors='ignore')

    # 1️⃣ Create unique_order_hash
    # Using a simple combination for Pandas.
    df["unique_order_hash"] = (
        df["customer_id"].astype(str) + "||" + 
        df["order_created_at"].dt.strftime('%Y-%m-%d %H:%M:%S')
    )

    # --- Metrics and Distributions ---

    # 2️⃣ Contagens (df_percent)
    df_counts = (
        df.groupby(["order_created_month", "is_target", "active"])
        .size()
        .reset_index(name='count')
    )
    
    # Calculate total_month and percentual using a grouped transform/apply
    df_percent = df_counts.copy()
    
    # Calculate total_month (like PySpark's Window.partitionBy)
    df_percent["total_month"] = (
        df_percent.groupby("order_created_month")["count"].transform('sum')
    )

    df_percent["percentual"] = (
        (df_percent["count"] / df_percent["total_month"]) * 100
    ).round(2)
    
    # 3️⃣ Calculate num_pedidos (Historic and Monthly)
    
    # Orders per user per month
    df['num_pedidos_mes'] = (
        df.groupby(["customer_id", "is_target", "order_created_month"])
        .unique_order_hash.transform('count')
    )
    
    # Orders per user historic
    df['num_pedidos_hist'] = (
        df.groupby(["customer_id", "is_target"])
        .unique_order_hash.transform('count')
    )
    
    # 4️⃣ Calculate prev_order_time and diff_days (Window function)
    
    # Sort the data first, which is essential for the shift/lag operation
    df = df.sort_values(by=['customer_id', 'order_created_at'], ascending=[True, True])
    
    # Calculate previous order time (Lag/Shift) partitioned by customer_id
    df["prev_order_time"] = (
        df.groupby("customer_id")['order_created_at']
        .shift(1) # shift(1) is equivalent to lag(1)
    )
    
    # Calculate difference in days (datediff)
    df["diff_days"] = (
        df['order_created_at'] - df['prev_order_time']
    ).dt.days # .dt.days extracts the integer day difference

    # 5️⃣ Final Sorting and Return
    # Certifique-se de que a variável de retorno é a df_final classificada
    df_final = df.sort_values(
        by=['customer_id', 'order_created_at'], 
        ascending=[False, True] # customer_id DESC, order_created_at ASC
    )

    # CORREÇÃO: Retornar a tupla (df_final, df_percent)
    return df_final, df_percent

def resumo_coorte_ativa(df: pd.DataFrame, mes_coorte_inicio: int) -> pd.DataFrame:
    """
    Filtra clientes ativos no mês de início (coorte), pivota a contagem de pedidos
    para o mês de início e o mês seguinte, e retorna um DataFrame resumido
    com o total de clientes por padrão de pedidos (Target/Mês Fim/Mês Início).

    Args:
        df (pd.DataFrame): DataFrame principal com colunas 'customer_id', 'is_target', 
                           'order_created_month', e 'num_pedidos_mes'.
        mes_coorte_inicio (int): O mês inicial da coorte (e.g., 12 para Dezembro).

    Returns:
        pd.DataFrame: DataFrame com Total_Clientes por combinação de 
                      is_target, e pedidos dos dois meses.
    """
    
    # 1. DEFINIÇÃO DA COORTE E CÁLCULO DO MÊS FINAL
    # Calcula o mês seguinte, tratando a virada do ano (12 -> 1)
    mes_coorte_fim = 1 if mes_coorte_inicio == 12 else mes_coorte_inicio + 1
    
    # Identificar IDs de clientes ativos no mês de início
    id_coorte_inicio = df[df['order_created_month'] == mes_coorte_inicio]['customer_id'].unique()
    
    # Filtrar o DataFrame apenas para os clientes da coorte
    # e apenas para os dois meses de interesse
    df_coorte = df[
        (df['customer_id'].isin(id_coorte_inicio)) & 
        (df['order_created_month'].isin([mes_coorte_inicio, mes_coorte_fim]))
    ].reset_index(drop=True)

    # 2. PIVOTAMENTO DA COORTE
    df_pivotado = df_coorte.pivot(
        index=['customer_id', 'is_target'], 
        columns='order_created_month',
        values='num_pedidos_mes'
    ).fillna(0).reset_index()

    # Remover o nome do cabeçalho
    df_pivotado.columns.name = None 

    # 3. RENOMEAÇÃO DE COLUNAS
    
    # Mapeamento padrão dos nomes das colunas de pedidos
    mapeamento_renomeacao = {
        mes_coorte_inicio: f'Total_Pedidos_Mes_{mes_coorte_inicio}',
        mes_coorte_fim: f'Total_Pedidos_Mes_{mes_coorte_fim}',
        'customer_id': 'ID_Cliente'
    }
    
    # Tratamento especial para Dezembro (12) e Janeiro (1)
    if mes_coorte_inicio == 12 and mes_coorte_fim == 1:
        mapeamento_renomeacao = {
            12: 'Total_Pedidos_Dezembro',
            1: 'Total_Pedidos_Janeiro',
            'customer_id': 'ID_Cliente'
        }

    df_final = df_pivotado.rename(columns=mapeamento_renomeacao)
    
    # 4. AGRUPAMENTO FINAL (Resumo por Padrão de Pedidos)
    
    # Define os nomes de coluna finais
    col_inicio = mapeamento_renomeacao.get(mes_coorte_inicio, f'Total_Pedidos_Mes_{mes_coorte_inicio}')
    col_fim = mapeamento_renomeacao.get(mes_coorte_fim, f'Total_Pedidos_Mes_{mes_coorte_fim}')
    
    columns_to_group_by = [
        'is_target', 
        col_fim,
        col_inicio
    ] 

    # Agrupar e contar, usando 'is_target' como valor para a contagem
    df_resumo_por_pedidos = df_final.groupby(columns_to_group_by).agg(
        Total_Clientes=('is_target', 'count')
    ).reset_index()

    df_resumo_por_pedidos.columns.name = None
    
    return df_resumo_por_pedidos



def process_orders_pandas(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pandas implementation of the order processing pipeline.
    
    Includes calculated metrics per customer per month using 'order_total_amount':
      - total_amount_mes (Total Amount by Customer/Month)
      - ticket_medio (Average Amount / AOV by Customer/Month)
      - num_pedidos_mes (Total Orders / Frequency by Customer/Month)
    
    Returns:
      - df_final (Pandas DataFrame) with calculated metrics.
      - df_percent (Pandas DataFrame with monthly distributions).
    """
    
    # Faz uma cópia para evitar SettingWithCopyWarning
    df = df.copy() 

    # --- Pré-Processamento e Limpeza ---
    
    # Ensure 'order_created_at' is datetime
    df['order_created_at'] = pd.to_datetime(df['order_created_at'])
    df['order_created_month'] = df['order_created_at'].dt.month
    df = df.drop(columns=['order_id'], errors='ignore')

    # 1️⃣ Create unique_order_hash (para contagem robusta de pedidos)
    df["unique_order_hash"] = (
        df["customer_id"].astype(str) + "||" + 
        df["order_created_at"].dt.strftime('%Y-%m-%d %H:%M:%S')
    )

    # --- Metrics and Distributions ---

    # 2️⃣ Contagens (df_percent) - Mantido para distribuição
    df_counts = (
        df.groupby(["order_created_month", "is_target", "active"])
        .size()
        .reset_index(name='count')
    )
    
    df_percent = df_counts.copy()
    
    df_percent["total_month"] = (
        df_percent.groupby("order_created_month")["count"].transform('sum')
    )

    df_percent["percentual"] = (
        (df_percent["count"] / df_percent["total_month"]) * 100
    ).round(2)
    
    # 3️⃣ MÉTICAS CHAVE POR CLIENTE E MÊS (Valor e Frequência)
    
    group_cols = ["customer_id", "is_target", "order_created_month"]

    # a) Total Amount por Cliente por Mês
    df['total_amount_mes'] = (
        df.groupby(group_cols)['order_total_amount'].transform('sum')
    )
    
    # b) Ticket Médio (AOV) por Cliente por Mês
    df['ticket_medio'] = (
        df.groupby(group_cols)['order_total_amount'].transform('mean')
    )
    
    # c) Total de Pedidos (Frequência) por Cliente por Mês
    df['num_pedidos_mes'] = (
        df.groupby(group_cols).unique_order_hash.transform('count')
    )

    # Pedidos Históricos (Geral)
    df['num_pedidos_hist'] = (
        df.groupby(["customer_id", "is_target"]).unique_order_hash.transform('count')
    )
    
    
    # 4️⃣ Calculate prev_order_time and diff_days (Tempo entre pedidos)
    
    # Sort the data first
    df = df.sort_values(by=['customer_id', 'order_created_at'], ascending=[True, True])
    
    # Calculate previous order time (Lag/Shift)
    df["prev_order_time"] = (
        df.groupby("customer_id")['order_created_at'].shift(1)
    )
    
    # Calculate difference in days (datediff)
    df["diff_days"] = (
        df['order_created_at'] - df['prev_order_time']
    ).dt.days

    # 5️⃣ Final Sorting and Return
    df_final = df.sort_values(
        by=['customer_id', 'order_created_at'], 
        ascending=[False, True]
    )

    return df_final, df_percent