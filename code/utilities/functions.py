
import requests
import tarfile
import io
import urllib.request
import gzip
import json
import pandas as pd
from statsmodels.stats.proportion import proportions_ztest
from pathlib import Path 
import numpy as np

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




def save_parquet(df, folder_name, filename):
    """
    Salva DataFrame em formato Parquet na pasta dados/<folder_name>/
    """
    import os
    from pathlib import Path
    
    # Caminho: dados/silver/, dados/gold/, etc.
    target_folder = Path("dados") / folder_name
    
    # Cria a pasta se não existir
    target_folder.mkdir(parents=True, exist_ok=True)
    
    # Caminho completo do arquivo
    path = target_folder / filename
    
    # Salva o arquivo
    df.to_parquet(path, index=False)
    
    print(f"💾 SALVO: {path}")
    print(f"📁 Local: {path.absolute()}")
    
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

def resumo_coorte_ativa(df: pd.DataFrame, mes_coorte_inicio: int,mes_coorte_fim: int) -> pd.DataFrame:
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
    #mes_coorte_fim = 1 if mes_coorte_inicio == 12 else mes_coorte_inicio + 1
    
    # Identificar IDs de clientes ativos no mês de início
    id_coorte_inicio = df[df['order_created_month'] == mes_coorte_inicio]['customer_id'].unique()
    (id_coorte_inicio)
    # Filtrar o DataFrame apenas para os clientes da coorte
    # e apenas para os dois meses de interesse
    df_coorte = df[
        (df['customer_id'].isin(id_coorte_inicio)) & 
        (df['order_created_month'].isin([mes_coorte_inicio, mes_coorte_fim]))
    ].reset_index(drop=True)

    df_pivotado = (df_coorte
    .groupby(['customer_id', 'is_target', 'order_created_month'])['num_pedidos_mes']
    .sum()
    .reset_index()
    .pivot(index=['customer_id', 'is_target'], columns='order_created_month', values='num_pedidos_mes')
    .fillna(0)
    .reset_index()
    .rename(columns={
        mes_coorte_inicio: f'Total_Pedidos_Mes_{mes_coorte_inicio}',
        mes_coorte_fim: f'Total_Pedidos_Mes_{mes_coorte_fim}'
    })
)   
    col_inicio = f'Total_Pedidos_Mes_{mes_coorte_inicio}'
    col_fim = f'Total_Pedidos_Mes_{mes_coorte_fim}'

    columns_to_group_by = [
    'is_target', 
    col_fim,
    col_inicio
] 

    df_resumo_por_pedidos = df_pivotado.groupby(columns_to_group_by).agg(
    Total_Clientes=('is_target', 'count')
    ).reset_index()

    df_resumo_por_pedidos.columns.name = None

    
    return df_resumo_por_pedidos



def process_orders_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    
    Includes calculated metrics per customer per month using 'order_total_amount':
      - total_amount_mes (Total Amount by Customer/Month)
      - ticket_medio (Average Amount / AOV by Customer/Month)
      - num_pedidos_mes (Total Orders / Frequency by Customer/Month)
   
    """

    df['order_created_at'] = pd.to_datetime(df['order_created_at'])
    df['order_created_month'] = df['order_created_at'].dt.month
    df = df.drop(columns=['order_id'], errors='ignore')


    df["unique_order_hash"] = (
        df["customer_id"].astype(str) + "||" + 
        df["order_created_at"].dt.strftime('%Y-%m-%d %H:%M:%S')
    )

    df_counts = (
        df.groupby(["order_created_month", "is_target", "active"])
        .size()
        .reset_index(name='count')
    )
   
    group_cols = ["customer_id", "is_target", "order_created_month"]

    df['total_amount_mes'] = (
        df.groupby(group_cols)['order_total_amount'].transform('sum')
    )
 
    df['ticket_medio'] = (
        df.groupby(group_cols)['order_total_amount'].transform('mean')
    )

    df['num_pedidos_mes'] = (
        df.groupby(group_cols).unique_order_hash.transform('count')
    )

    df['num_pedidos_hist'] = (
        df.groupby(["customer_id", "is_target"]).unique_order_hash.transform('count')
    )
    
    df = df.sort_values(by=['customer_id', 'order_created_at'], ascending=[True, True])
    
    df["prev_order_time"] = (
        df.groupby("customer_id")['order_created_at'].shift(1)
    )

    df["diff_days"] = (
        df['order_created_at'] - df['prev_order_time']
    ).dt.days

    df = df.sort_values(
        by=['customer_id', 'order_created_at'], 
        ascending=[False, True]
    )

    return df

def summary(dataframe):
    summary=dataframe.describe(include='all') 
    summary=summary.transpose()
    return summary.head(len(summary))

def retidos(df, mes0, mes1, pedidos):
    # mantém apenas os dois meses de interesse
    df = df[df["order_created_month"].isin([mes0, mes1])]

    # pivot por cliente x grupo, meses viram colunas
    wide = df.pivot_table(
        index=["customer_id", "is_target"],
        columns="order_created_month",
        values=["num_pedidos_mes", "total_amount_mes"],
        fill_value=0
    )

    # renomeia colunas: num_pedidos_mes_12, num_pedidos_mes_1, etc
    wide.columns = [f"{var}_{mes}" for (var, mes) in wide.columns]
    wide = wide.reset_index()

    # deltas de frequência e GMV
    wide["delta_freq"] = wide[f"num_pedidos_mes_{mes1}"] - wide[f"num_pedidos_mes_{mes0}"]
    wide["delta_gmv"] = wide[f"total_amount_mes_{mes1}"] - wide[f"total_amount_mes_{mes0}"]

    # base = clientes ativos no mes0
    base = wide[wide[f"num_pedidos_mes_{mes0}"] > 0].copy()

    # retido = fez pelo menos `pedidos` no mes1
    base["retido"] = (base[f"num_pedidos_mes_{mes1}"] > pedidos).astype(int)

    # agregação por grupo
    resumo = base.groupby("is_target").agg(
        retidos=("retido", "sum"),
        base=("retido", "count")
    )

    # taxa de retenção
    resumo["taxa_retencao"] = resumo["retidos"] / resumo["base"]

    return base, resumo


def gerar_resumo_decis(df_decil):
    """
    Creates a complete decil summary grouped by:
    - decil_12
    - decil_1
    - is_target
    
    Includes:
      total_clientes
      avg_amount_12 / avg_amount_1
      avg_pedidos_12 / avg_pedidos_1
      ticket_medio_12 / ticket_medio_1

    Rounds everything to 2 decimals and saves to CSV.
    
    Returns:
        clientes_por_decil_target (DataFrame)
    """

    # -------------------------------------------------------------
    # 1. Groupby calculation
    # -------------------------------------------------------------
    clientes_por_decil_target = (
        df_decil
        .groupby(['decil_12', 'decil_1', 'is_target'])
        .agg(
            total_clientes=('customer_id', 'nunique'),
            avg_amount_12=('total_amount_mes_12', 'mean'),
            avg_amount_1=('total_amount_mes_1', 'mean'),
            avg_pedidos_12=('num_pedidos_mes_12', 'mean'),
            avg_pedidos_1=('num_pedidos_mes_1', 'mean')
        )
        .reset_index()
    )

    # -------------------------------------------------------------
    # 2. Compute ticket médios
    # -------------------------------------------------------------
    clientes_por_decil_target["ticket_medio_12"] = (
        clientes_por_decil_target["avg_amount_12"] / clientes_por_decil_target["avg_pedidos_12"]
    )

    clientes_por_decil_target["ticket_medio_1"] = (
        clientes_por_decil_target["avg_amount_1"] / clientes_por_decil_target["avg_pedidos_1"]
    )

    # -------------------------------------------------------------
    # 3. Round everything to 2 decimals
    # -------------------------------------------------------------
    clientes_por_decil_target = clientes_por_decil_target.round(2)

    return clientes_por_decil_target


    
def cria_base_decil_wide(
    df,
    mes_0,
    mes_1,
    col_cliente="customer_id",
    col_mes="order_created_month",
    col_pedidos="num_pedidos_mes",
    col_valor="total_amount_mes",
    col_target="is_target",
    n_decis=10,
    prefixo_decil="decil",
    prefixo_pedidos="num_pedidos_mes",
    prefixo_valor="total_amount_mes"
):
    """
    Creates a wide dataset with:
    - Decil calculated on mes_0
    - Same decil applied to mes_1 using the min/max dictionary
    - Wide-format columns decil_{mes}, num_pedidos_mes_{mes}, total_amount_mes_{mes}
    - Keeps the original target/control column untouched
    
    Returns:
      df_wide, decil_dict
    """
    cols_base = [col_cliente, col_mes, col_pedidos, col_valor, col_target]
    df_base = df[cols_base].drop_duplicates().reset_index(drop=True)

    base_m0 = df_base[df_base[col_mes] == mes_0].copy()
    col_decil_m0 = f"{prefixo_decil}_{mes_0}"

    base_m0[col_decil_m0] = pd.qcut(
        base_m0[col_valor],
        n_decis,
        labels=[f"decil {i+1}" for i in range(n_decis)]
    )

    faixas_decil = (
        base_m0.groupby(col_decil_m0)[col_valor]
        .agg(["min", "max"])
        .reset_index()
    )

    decil_dict = {
        row[col_decil_m0]: (row["min"], row["max"])
        for _, row in faixas_decil.iterrows()
    }

    base_m1 = df_base[df_base[col_mes] == mes_1].copy()

    def atribui_decil(valor, decil_dict):
        for decil, (vmin, vmax) in decil_dict.items():
            if vmin <= valor <= vmax:
                return decil
        return np.nan

    col_decil_m1 = f"{prefixo_decil}_{mes_1}"
    base_m1[col_decil_m1] = base_m1[col_valor].apply(
        lambda x: atribui_decil(x, decil_dict)
    )

    base_m0["decil"] = base_m0[col_decil_m0]
    base_m1["decil"] = base_m1[col_decil_m1]

    colunas_finais = [
        col_cliente,
        col_mes,
        col_pedidos,
        col_valor,
        col_target,
        "decil"
    ]

    base_m0_final = base_m0[colunas_finais].copy()
    base_m1_final = base_m1[colunas_finais].copy()

    
    base_long = pd.concat([base_m0_final, base_m1_final], ignore_index=True)


    wide = base_long.copy()

    wide["decil_col"] = prefixo_decil + "_" + wide[col_mes].astype(str)
    wide["pedidos_col"] = prefixo_pedidos + "_" + wide[col_mes].astype(str)
    wide["valor_col"] = prefixo_valor + "_" + wide[col_mes].astype(str)

    wide_decil  = wide.pivot(index=col_cliente, columns="decil_col",  values="decil")
    wide_ped    = wide.pivot(index=col_cliente, columns="pedidos_col", values=col_pedidos)
    wide_valor  = wide.pivot(index=col_cliente, columns="valor_col",  values=col_valor)


    target_df = (
        base_long[[col_cliente, col_target]]
        .drop_duplicates()
        .set_index(col_cliente)
    )

    wide_final = (
        target_df
        .join(wide_decil, how="outer")
        .join(wide_ped,   how="outer")
        .join(wide_valor, how="outer")
        .reset_index()
    )

    return wide_final, decil_dict  # AGORA RETORNA AMBOS



def calcula_viabilidade(df, 
                        mes_campanha=12, 
                        mes_seguinte=1, 
                        coupon_value=10.0, 
                        margin_rate=0.12):
    """
    df: DataFrame com as colunas:
        - customer_id
        - is_target ('target' / 'control')
        - order_created_month (int)
        - num_pedidos_mes
        - total_amount_mes
    mes_campanha: mês em que o cupom foi disponibilizado (ex: 12)
    mes_seguinte: mês seguinte para retenção (ex: 1)
    coupon_value: valor médio do cupom (R$)
    margin_rate: margem líquida sobre GMV (0.12 = 12%)
    """

    # Filtra só meses relevantes
    df_use = df[df["order_created_month"].isin([mes_campanha, mes_seguinte])].copy()

    # Agrega por grupo e mês
    agg = (
        df_use
        .groupby(["is_target", "order_created_month"])
        .agg(
            pedidos=("num_pedidos_mes", "sum"),
            gmv=("total_amount_mes", "sum"),
            clientes=("customer_id", "nunique")
        )
        .reset_index()
    )

    # Helper para pegar linha de cada grupo/mês
    def get_row(grupo, mes):
        return agg[(agg["is_target"] == grupo) & (agg["order_created_month"] == mes)].iloc[0]

    # Dezembro (mês da campanha)
    row_t_dec = get_row("target", mes_campanha)
    row_c_dec = get_row("control", mes_campanha)

    # Janeiro (mês seguinte)
    row_t_jan = get_row("target", mes_seguinte)
    row_c_jan = get_row("control", mes_seguinte)

    # --- Pedidos por cliente (normalizados) ---
    # dezembro
    pedidos_cli_t_dec = row_t_dec["pedidos"] / row_t_dec["clientes"]
    pedidos_cli_c_dec = row_c_dec["pedidos"] / row_c_dec["clientes"]

    # janeiro
    pedidos_cli_t_jan = row_t_jan["pedidos"] / row_t_jan["clientes"]
    pedidos_cli_c_jan = row_c_jan["pedidos"] / row_c_jan["clientes"]

    # --- GMV por cliente ---
    gmv_cli_t_dec = row_t_dec["gmv"] / row_t_dec["clientes"]
    gmv_cli_c_dec = row_c_dec["gmv"] / row_c_dec["clientes"]

    gmv_cli_t_jan = row_t_jan["gmv"] / row_t_jan["clientes"]
    gmv_cli_c_jan = row_c_jan["gmv"] / row_c_jan["clientes"]

    # --- Incremento de pedidos (Target vs Controle) ---
    inc_pedidos_dec = (pedidos_cli_t_dec - pedidos_cli_c_dec) * row_t_dec["clientes"]
    inc_pedidos_jan = (pedidos_cli_t_jan - pedidos_cli_c_jan) * row_t_jan["clientes"]
    inc_pedidos_total = inc_pedidos_dec + inc_pedidos_jan

    # --- Incremento de GMV ---
    inc_gmv_dec = (gmv_cli_t_dec - gmv_cli_c_dec) * row_t_dec["clientes"]
    inc_gmv_jan = (gmv_cli_t_jan - gmv_cli_c_jan) * row_t_jan["clientes"]
    inc_gmv_total = inc_gmv_dec + inc_gmv_jan

    # --- Margem incremental ---
    margem_incremental = inc_gmv_total * margin_rate

    # --- Custo da campanha ---
    # premissa: cupom usado 1x por cliente target que fez pedido em dezembro
    base_target_dec = df_use[
        (df_use["order_created_month"] == mes_campanha) & 
        (df_use["is_target"] == "target") &
        (df_use["num_pedidos_mes"] > 0)
    ]["customer_id"].nunique()

    custo_campanha = base_target_dec * coupon_value

    # --- ROI ---
    lucro_incremental = margem_incremental - custo_campanha
    roi = lucro_incremental / custo_campanha if custo_campanha > 0 else None

    resultados = {
        "pedidos_por_cliente_dec_control": pedidos_cli_c_dec,
        "pedidos_por_cliente_dec_target": pedidos_cli_t_dec,
        "pedidos_por_cliente_jan_control": pedidos_cli_c_jan,
        "pedidos_por_cliente_jan_target": pedidos_cli_t_jan,
        "inc_pedidos_dec": inc_pedidos_dec,
        "inc_pedidos_jan": inc_pedidos_jan,
        "inc_pedidos_total": inc_pedidos_total,
        "inc_gmv_dec": inc_gmv_dec,
        "inc_gmv_jan": inc_gmv_jan,
        "inc_gmv_total": inc_gmv_total,
        "margem_incremental": margem_incremental,
        "base_target_dec": base_target_dec,
        "custo_campanha": custo_campanha,
        "lucro_incremental": lucro_incremental,
        "roi": roi,
        "coupon_value": coupon_value,
        "margin_rate": margin_rate,
    }

    return resultados, agg


def analisar_retencao(df):

    # taxas de retenção
    target_retencao = df[df['is_target'] == 'target']['retido'].mean()
    control_retencao = df[df['is_target'] == 'control']['retido'].mean()

    # lifts
    lift_absoluto = target_retencao - control_retencao
    lift_relativo = (lift_absoluto / control_retencao * 100) if control_retencao > 0 else 0

    # contagens
    target_success = df[df['is_target'] == 'target']['retido'].sum()
    target_total = df[df['is_target'] == 'target'].shape[0]

    control_success = df[df['is_target'] == 'control']['retido'].sum()
    control_total = df[df['is_target'] == 'control'].shape[0]

    # teste estatístico
    stat, p_value = proportions_ztest(
        [target_success, control_success],
        [target_total, control_total]
    )
    resultado = {
        "target_retencao": round(target_retencao, 6),
        "control_retencao": round(control_retencao, 6),
        "lift_absoluto": round(lift_absoluto, 6),
        "lift_relativo_percent": round(lift_relativo, 2),
        "target_success": int(target_success),
        "target_total": int(target_total),
        "control_success": int(control_success),
        "control_total": int(control_total),
        "z_stat": round(float(stat), 6),
        "p_value": round(float(p_value), 10),
        "significativo": p_value < 0.05
    }

    return resultado 

def segmentacao_3_otimizada(df):
    """
    Classifica mobilidade e direção de forma VETORIZADA
    """
    # Mobilidade (vetorizado)
    conditions_mob = [
        df['delta_decil'] == 0,
        abs(df['delta_decil']) == 1,
        abs(df['delta_decil']) > 1
    ]
    choices_mob = ["Estável", "Mobilidade Moderada", "Alta Mobilidade"]
    df['segmento_mobilidade'] = np.select(conditions_mob, choices_mob, default="Estável")
    
    # Direção (vetorizado)
    conditions_dir = [
        df['delta_decil'] < 0,
        df['delta_decil'] > 0,
        df['delta_decil'] == 0
    ]
    choices_dir = ["Upgrade", "Downgrade", "Estável"]
    df['direcao'] = np.select(conditions_dir, choices_dir, default="Estável")
    
    return df

# df com colunas:
# ['customer_id', 'is_target', 'decil_1', 'decil_12',
#  'num_pedidos_mes_1', 'num_pedidos_mes_12',
#  'total_amount_mes_1', 'total_amount_mes_12',
#  'decil_1_num', 'decil_12_num', 'delta_decil',
#  'segmento_mobilidade', 'direcao', 'categoria_segmento']

def retidos11(df, mes0=1, mes1=12, pedidos=1, por_segmento=False):
    """
    Calcula retenção A/B a partir da base wide já agregada por cliente.

    Parâmetros
    ----------
    df : DataFrame
        Base wide por cliente (1 linha por customer_id).
    mes0 : int
        Mês base (ex.: 1).
    mes1 : int
        Mês de comparação (ex.: 12).
    pedidos : int
        Número mínimo de pedidos no mes1 para considerar retido.
        Ex.: 0  -> pelo menos 1 pedido
             1  -> pelo menos 2 pedidos
    por_segmento : bool
        Se True, devolve também resumo por segmento (categoria_segmento /
        segmento_mobilidade / direcao).

    Retorno
    -------
    base : DataFrame
        Base de clientes ativos no mes0 com coluna 'retido'.
    resumo_global : DataFrame
        Resumo por is_target (control x target).
    resumo_segmento : DataFrame (opcional)
        Resumo por segmento + is_target (se por_segmento=True).
    """

    # nomes das colunas de frequência e GMV dinamicamente
    freq0_col = f"num_pedidos_mes_{mes0}"
    freq1_col = f"num_pedidos_mes_{mes1}"
    gmv0_col = f"total_amount_mes_{mes0}"
    gmv1_col = f"total_amount_mes_{mes1}"

    # garante que as colunas existem
    for c in [freq0_col, freq1_col, gmv0_col, gmv1_col]:
        if c not in df.columns:
            raise ValueError(f"Coluna '{c}' não encontrada no DataFrame.")

    base = df.copy()

    # deltas de frequência e GMV
    base["delta_freq"] = base[freq1_col] - base[freq0_col]
    base["delta_gmv"] = base[gmv1_col] - base[gmv0_col]

    # base = clientes ativos no mes0
    base = base[base[freq0_col] > 0].copy()

    # retido = fez mais que `pedidos` no mes1
    # pedidos=0 => >0 pedidos, pedidos=1 => >1 pedidos, etc.
    base["retido"] = (base[freq1_col] > pedidos).astype(int)

    # resumo global A/B
    resumo_global = (
        base.groupby("is_target")
            .agg(
                retidos=("retido", "sum"),
                base=("retido", "count")
            )
    )
    resumo_global["taxa_retencao"] = resumo_global["retidos"] / resumo_global["base"]

    if not por_segmento:
        return base, resumo_global

    # resumo por segmento (categoria + mobilidade + direção)
    resumo_segmento = (
        base
        .groupby(["categoria_segmento", "segmento_mobilidade", "direcao", "is_target"])
        .agg(
            retidos=("retido", "sum"),
            base=("retido", "count")
        )
        .reset_index()
    )
    resumo_segmento["taxa_retencao"] = resumo_segmento["retidos"] / resumo_segmento["base"]

    return base, resumo_global, resumo_segmento

def calcula_viabilidade_wide(df,
                             mes_campanha=12,
                             mes_seguinte=1,
                             coupon_value=10.0,
                             margin_rate=0.12):
    """
    Calcula viabilidade econômica (ROI) da campanha usando base WIDE já agregada por cliente.

    Parâmetros
    ----------
    df : DataFrame
        Deve conter, no mínimo:
        - customer_id
        - is_target  ('target' / 'control')
        - num_pedidos_mes_<mes_campanha>
        - num_pedidos_mes_<mes_seguinte>
        - total_amount_mes_<mes_campanha>
        - total_amount_mes_<mes_seguinte>

        Exemplo de colunas para mes_campanha=12 e mes_seguinte=1:
        - num_pedidos_mes_12, num_pedidos_mes_1
        - total_amount_mes_12, total_amount_mes_1

    mes_campanha : int
        Mês em que o cupom foi disponibilizado (ex: 12).
    mes_seguinte : int
        Mês seguinte para retenção (ex: 1).
    coupon_value : float
        Valor médio do cupom (R$).
    margin_rate : float
        Margem líquida sobre o GMV (0.12 = 12%).

    Retorno
    -------
    resultados : dict
        Métricas agregadas de incremento, margem e ROI.
    agg : DataFrame
        Resumo por is_target (pedidos, gmv, clientes) para cada mês.
    """

    # nomes das colunas dinamicamente
    freq_camp_col = f"num_pedidos_mes_{mes_campanha}"
    freq_seg_col  = f"num_pedidos_mes_{mes_seguinte}"
    gmv_camp_col  = f"total_amount_mes_{mes_campanha}"
    gmv_seg_col   = f"total_amount_mes_{mes_seguinte}"

    # checagem básica de colunas
    for c in [freq_camp_col, freq_seg_col, gmv_camp_col, gmv_seg_col]:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória '{c}' não encontrada no DataFrame.")

    # agrega por grupo (target/control) para cada mês
    agg = (
        df.groupby("is_target")
          .agg(
              pedidos_campanha=(freq_camp_col, "sum"),
              gmv_campanha=(gmv_camp_col, "sum"),
              pedidos_seguinte=(freq_seg_col, "sum"),
              gmv_seguinte=(gmv_seg_col, "sum"),
              clientes=("customer_id", "nunique")
          )
    )

    # helper para pegar linha por grupo
    row_t = agg.loc["target"]
    row_c = agg.loc["control"]

    # --- Pedidos por cliente em cada mês ---
    pedidos_cli_t_dec = row_t["pedidos_campanha"]   / row_t["clientes"]
    pedidos_cli_c_dec = row_c["pedidos_campanha"]   / row_c["clientes"]

    pedidos_cli_t_jan = row_t["pedidos_seguinte"]   / row_t["clientes"]
    pedidos_cli_c_jan = row_c["pedidos_seguinte"]   / row_c["clientes"]

    # --- GMV por cliente em cada mês ---
    gmv_cli_t_dec = row_t["gmv_campanha"] / row_t["clientes"]
    gmv_cli_c_dec = row_c["gmv_campanha"] / row_c["clientes"]

    gmv_cli_t_jan = row_t["gmv_seguinte"] / row_t["clientes"]
    gmv_cli_c_jan = row_c["gmv_seguinte"] / row_c["clientes"]

    # --- Incremento de pedidos (Target vs Controle) ---
    # Escalando pelo número de clientes TARGET (mesma lógica da função original)
    inc_pedidos_dec = (pedidos_cli_t_dec - pedidos_cli_c_dec) * row_t["clientes"]
    inc_pedidos_jan = (pedidos_cli_t_jan - pedidos_cli_c_jan) * row_t["clientes"]
    inc_pedidos_total = inc_pedidos_dec + inc_pedidos_jan

    # --- Incremento de GMV ---
    inc_gmv_dec = (gmv_cli_t_dec - gmv_cli_c_dec) * row_t["clientes"]
    inc_gmv_jan = (gmv_cli_t_jan - gmv_cli_c_jan) * row_t["clientes"]
    inc_gmv_total = inc_gmv_dec + inc_gmv_jan

    # --- Margem incremental ---
    margem_incremental = inc_gmv_total * margin_rate

    # --- Custo da campanha ---
    # premissa: cupom usado 1x por cliente target que fez pedido no mes_campanha
    base_target_camp = df[
        (df["is_target"] == "target") &
        (df[freq_camp_col] > 0)
    ]["customer_id"].nunique()

    custo_campanha = base_target_camp * coupon_value

    # --- ROI ---
    lucro_incremental = margem_incremental - custo_campanha
    roi = lucro_incremental / custo_campanha if custo_campanha > 0 else None

    resultados = {
        "pedidos_por_cliente_camp_control": pedidos_cli_c_dec,
        "pedidos_por_cliente_camp_target": pedidos_cli_t_dec,
        "pedidos_por_cliente_seg_control": pedidos_cli_c_jan,
        "pedidos_por_cliente_seg_target": pedidos_cli_t_jan,
        "inc_pedidos_camp": inc_pedidos_dec,
        "inc_pedidos_seg": inc_pedidos_jan,
        "inc_pedidos_total": inc_pedidos_total,
        "inc_gmv_camp": inc_gmv_dec,
        "inc_gmv_seg": inc_gmv_jan,
        "inc_gmv_total": inc_gmv_total,
        "margem_incremental": margem_incremental,
        "base_target_camp": base_target_camp,
        "custo_campanha": custo_campanha,
        "lucro_incremental": lucro_incremental,
        "roi": roi,
        "coupon_value": coupon_value,
        "margin_rate": margin_rate,
        "mes_campanha": mes_campanha,
        "mes_seguinte": mes_seguinte,
    }

    return resultados, agg
