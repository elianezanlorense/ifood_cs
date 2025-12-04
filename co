
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


###decil


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
        for decil in sorted(decil_dict.keys(), reverse=True):
            vmin = decil_dict[decil][0]
            if valor >= vmin:
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

    return wide_final, decil_dict  


####Viabilidade financeira

def calcula_viabilidade(df, 
                                mes_campanha=12, 
                                mes_seguinte=1, 
                                coupon_value=10.0, 
                                margin_rate=0.12):
    """
    FUNÇÃO: Calcula a viabilidade financeira e retorna DataFrame completo
    RETORNA: DataFrame único com TODOS os parâmetros calculados
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
        result = agg[(agg["is_target"] == grupo) & (agg["order_created_month"] == mes)]
        return result.iloc[0] if len(result) > 0 else None

    # Busca os dados
    row_t_dec = get_row("target", mes_campanha)
    row_c_dec = get_row("control", mes_campanha)
    row_t_jan = get_row("target", mes_seguinte)
    row_c_jan = get_row("control", mes_seguinte)

    # Verifica se todos os dados necessários existem
    if any(row is None for row in [row_t_dec, row_c_dec, row_t_jan, row_c_jan]):
        missing = []
        if row_t_dec is None: missing.append(f"target_{mes_campanha}")
        if row_c_dec is None: missing.append(f"control_{mes_campanha}")
        if row_t_jan is None: missing.append(f"target_{mes_seguinte}")
        if row_c_jan is None: missing.append(f"control_{mes_seguinte}")
        raise ValueError(f"Dados faltantes para: {', '.join(missing)}")


    # Pedidos
    pedidos_cli_t_dec = row_t_dec["pedidos"] / row_t_dec["clientes"]
    pedidos_cli_c_dec = row_c_dec["pedidos"] / row_c_dec["clientes"]
    pedidos_cli_t_jan = row_t_jan["pedidos"] / row_t_jan["clientes"]
    pedidos_cli_c_jan = row_c_jan["pedidos"] / row_c_jan["clientes"]

    pedidos_tot_t_dec = row_t_dec["pedidos"]
    pedidos_tot_c_dec = row_c_dec["pedidos"]
    pedidos_tot_t_jan = row_t_jan["pedidos"]
    pedidos_tot_c_jan = row_c_jan["pedidos"]


    inc_pedidos_dec = (pedidos_cli_t_dec - pedidos_cli_c_dec) * row_t_dec["clientes"]
    inc_pedidos_jan = (pedidos_cli_t_jan - pedidos_cli_c_jan) * row_t_jan["clientes"]
    inc_pedidos_total = inc_pedidos_dec + inc_pedidos_jan

    # GMV
    gmv_cli_t_dec = row_t_dec["gmv"] / row_t_dec["clientes"]
    gmv_cli_c_dec = row_c_dec["gmv"] / row_c_dec["clientes"]
    gmv_cli_t_jan = row_t_jan["gmv"] / row_t_jan["clientes"]
    gmv_cli_c_jan = row_c_jan["gmv"] / row_c_jan["clientes"]

    gmv_tot_t_dec = row_t_dec["gmv"]
    gmv_tot_c_dec = row_c_dec["gmv"]
    gmv_tot_t_jan = row_t_jan["gmv"]
    gmv_tot_c_jan = row_c_jan["gmv"]

    inc_gmv_dec = (gmv_cli_t_dec - gmv_cli_c_dec) * row_t_dec["clientes"]  
    inc_gmv_jan = (gmv_cli_t_jan - gmv_cli_c_jan) * row_t_jan["clientes"]  
    
    inc_gmv_total = inc_gmv_dec + inc_gmv_jan  #

    # Receitas
    receita_ifood_t_dec = gmv_tot_t_dec * margin_rate
    receita_ifood_c_dec = gmv_tot_c_dec * margin_rate
    receita_ifood_t_jan = gmv_tot_t_jan * margin_rate
    receita_ifood_c_jan = gmv_tot_c_jan * margin_rate

    # Margens
    margem_incremental_dec = receita_ifood_t_dec - receita_ifood_c_dec
    margem_incremental_jan = receita_ifood_t_jan - receita_ifood_c_jan
    margem_incremental_total = margem_incremental_dec + margem_incremental_jan

    # Custos
    base_target_dec = df_use[
        (df_use["order_created_month"] == mes_campanha) & 
        (df_use["is_target"] == "target") &
        (df_use["num_pedidos_mes"] > 0)
    ]["customer_id"].nunique()

    custo_campanha = base_target_dec * coupon_value
    dezembro_pos = receita_ifood_t_dec - custo_campanha

    # ROI
    lucro_incremental = margem_incremental_total - custo_campanha
    roi = (lucro_incremental / custo_campanha) if custo_campanha > 0 else 0

    # --- CRIA DATASET ÚNICO COM TUDO ---
    dados_completos = {
        # Parâmetros de entrada
        "mes_campanha": mes_campanha,
        "mes_seguinte": mes_seguinte,
        "coupon_value": coupon_value,
        "margin_rate": margin_rate,
        
        # Clientes
        "clientes_target_dec": row_t_dec["clientes"],
        "clientes_control_dec": row_c_dec["clientes"],
        "clientes_target_jan": row_t_jan["clientes"],
        "clientes_control_jan": row_c_jan["clientes"],
        "base_target_dec": base_target_dec,
        
        # Pedidos totais
        "pedidos_tot_target_dec": pedidos_tot_t_dec,
        "pedidos_tot_control_dec": pedidos_tot_c_dec,
        "pedidos_tot_target_jan": pedidos_tot_t_jan,
        "pedidos_tot_control_jan": pedidos_tot_c_jan,
        
        "pedidos_total_target": pedidos_tot_t_dec + pedidos_tot_t_jan,
        "pedidos_total_control": pedidos_tot_c_dec + pedidos_tot_c_jan,
        # Pedidos por cliente
        "pedidos_por_cliente_dec_control": pedidos_cli_c_dec,
        "pedidos_por_cliente_dec_target": pedidos_cli_t_dec,
        "pedidos_por_cliente_jan_control": pedidos_cli_c_jan,
        "pedidos_por_cliente_jan_target": pedidos_cli_t_jan,
        
        # Incrementos de pedidos
        "inc_pedidos_dec": inc_pedidos_dec,
        "inc_pedidos_jan": inc_pedidos_jan,
        "inc_pedidos_total": inc_pedidos_total,

        # GMV totais
        "gmv_tot_target_dec": gmv_tot_t_dec,
        "gmv_tot_control_dec": gmv_tot_c_dec,
        "gmv_tot_target_jan": gmv_tot_t_jan,
        "gmv_tot_control_jan": gmv_tot_c_jan,
        
        # GMV por cliente
        "gmv_por_cliente_dec_control": gmv_cli_c_dec,
        "gmv_por_cliente_dec_target": gmv_cli_t_dec,
        "gmv_por_cliente_jan_control": gmv_cli_c_jan,
        "gmv_por_cliente_jan_target": gmv_cli_t_jan,

        # Incrementos de GMV
        "inc_gmv_dec": inc_gmv_dec,
        "inc_gmv_jan": inc_gmv_jan,
        "inc_gmv_total": inc_gmv_total,

        # Receitas iFood
        "receita_ifood_t_dec": receita_ifood_t_dec,
        "receita_ifood_c_dec": receita_ifood_c_dec,
        "receita_ifood_t_jan": receita_ifood_t_jan,
        "receita_ifood_c_jan": receita_ifood_c_jan,

        # Margens incrementais
        "margem_incremental_dec": margem_incremental_dec,
        "margem_incremental_jan": margem_incremental_jan,
        "margem_incremental_total": margem_incremental_total,

        # Custos e lucro
        "custo_campanha": custo_campanha,
        "dezembro_pos": dezembro_pos,
        "lucro_incremental": lucro_incremental,
        "roi": roi,

        # Métricas consolidadas
        "gmv_total_target": gmv_tot_t_dec + gmv_tot_t_jan,
        "gmv_total_control": gmv_tot_c_dec + gmv_tot_c_jan
    }

    # Cria DataFrame único
    dataset_completo = pd.DataFrame([dados_completos])
    
    return dataset_completo

#Retencao

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

#Segmentacao
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
    choices_dir = ["Downgrade","Upgrade", "Estável"]
    df['direcao'] = np.select(conditions_dir, choices_dir, default="Estável")
    
    return df

### retidos

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




import numpy as np
import pandas as pd
from statsmodels.stats.proportion import proportions_ztest, proportion_confint

import numpy as np
from statsmodels.stats.power import TTestIndPower
import math


import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportions_ztest

from scipy import stats
from scipy.stats import mannwhitneyu, ttest_ind
from statsmodels.stats.proportion import proportions_ztest


from statsmodels.stats.power import TTestIndPower

def calcular_mde_por_janela(df, power_desejado=0.8, alpha=0.05):
    """
    Calcula MDE para cada janela temporal do teste A/B
    """
    resultados_mde = []
    colunas_flags = [c for c in df.columns if c.startswith("converteu_")]
    
    for col in colunas_flags:
        # Filtra apenas os conversores desta janela
        df_janela = df[df[col] == 1]
        
        target_data = df_janela[df_janela['is_target'] == 'target']['order_total_amount']
        control_data = df_janela[df_janela['is_target'] == 'control']['order_total_amount']
        
        n_target = len(target_data)
        n_control = len(control_data)
        
        if n_target > 1 and n_control > 1:
            std_pooled = np.sqrt(((n_target-1)*target_data.std()**2 + (n_control-1)*control_data.std()**2) / (n_target + n_control - 2))
            
            # Calcula MDE
            analysis = TTestIndPower()
            effect_size_mde = analysis.solve_power(
                nobs1=n_target,
                power=power_desejado,
                alpha=alpha,
                ratio=n_control/n_target
            )
            
            mde_reais = effect_size_mde * std_pooled
            
            # Effect size real observado
            effect_size_real = (target_data.mean() - control_data.mean()) / std_pooled
            
        else:
            mde_reais = effect_size_mde = effect_size_real = std_pooled = np.nan
        
        resultados_mde.append({
            'janela': col,
            'mde_reais': mde_reais,
            'mde_effect_size': effect_size_mde,
            'effect_size_real': effect_size_real,
            'amostra_target': n_target,
            'amostra_control': n_control,
            'std_pooled': std_pooled
        })
    
    return pd.DataFrame(resultados_mde)

def verificar_poder_janelas(df_resultados, alpha=0.05):
    """
    Verifica o poder estatístico para cada janela do teste A/B
    """
    power_analysis = TTestIndPower()
    resultados = []
    
    for _, row in df_resultados.iterrows():
        if not pd.isna(row['cohen_d']) and row['total_target'] > 0:
            poder = power_analysis.solve_power(
                effect_size=abs(row['cohen_d']),  # valor absoluto
                nobs1=row['total_target'],
                alpha=alpha
            )
        else:
            poder = None
        
        resultados.append({
            'janela': row['janela'],
            'cohen_d': row['cohen_d'],
            'amostra_target': row['total_target'],
            'amostra_control': row['total_control'],
            'poder_estatistico': poder
        })
    
    return pd.DataFrame(resultados)

def teste_ab_completo_por_janela(df, alpha=0.05):
    """
    Realiza teste A/B completo para múltiplas janelas temporais
    """
    
    resultados = []
    colunas_flags = [c for c in df.columns if c.startswith("converteu_")]

    control_full = df[df['is_target'] == 'control']
    target_full = df[df['is_target'] == 'target']

    for col in colunas_flags:
        # ---------- TESTE DE PROPORÇÃO ----------
        conv_control = control_full[col].sum()
        conv_target = target_full[col].sum()

        total_control = control_full['customer_id'].nunique()
        total_target = target_full['customer_id'].nunique()

        taxa_control = conv_control / total_control if total_control > 0 else 0
        taxa_target = conv_target / total_target if total_target > 0 else 0
        diferenca_taxa = taxa_target - taxa_control

        if total_control > 0 and total_target > 0 and conv_control + conv_target > 0:
            count = np.array([conv_target, conv_control])
            nobs = np.array([total_target, total_control])
            z_stat, p_value_prop = proportions_ztest(count, nobs, alternative='two-sided')
            
            ep_prop = np.sqrt(taxa_target*(1-taxa_target)/total_target + taxa_control*(1-taxa_control)/total_control)
            z_critico = stats.norm.ppf(1 - alpha/2)
            ic_inf_prop = diferenca_taxa - z_critico * ep_prop
            ic_sup_prop = diferenca_taxa + z_critico * ep_prop
        else:
            z_stat, p_value_prop = np.nan, np.nan
            ic_inf_prop, ic_sup_prop = np.nan, np.nan

        decisao_prop = "Rejeitar H0" if p_value_prop < alpha else "Não rejeitar H0"

        # ---------- TESTES NOS VALORES ----------
        valores_control = control_full.loc[control_full[col] == 1, 'order_total_amount']
        valores_target = target_full.loc[target_full[col] == 1, 'order_total_amount']

        if len(valores_control) > 1 and len(valores_target) > 1:
            # Mann-Whitney U Test
            stat_mw, p_value_mw = mannwhitneyu(valores_target, valores_control, alternative='two-sided')
            decisao_mw = "Rejeitar H0" if p_value_mw < alpha else "Não rejeitar H0"

            # Teste T de Welch
            t_stat_w, p_value_w = ttest_ind(valores_target, valores_control, equal_var=False)
            decisao_welch = "Rejeitar H0" if p_value_w < alpha else "Não rejeitar H0"

            # Teste T tradicional
            t_stat_n, p_value_n = ttest_ind(valores_target, valores_control)
            decisao_t_normal = "Rejeitar H0" if p_value_n < alpha else "Não rejeitar H0"

            # Estatísticas descritivas
            media_control = valores_control.mean()
            media_target = valores_target.mean()
            dp_control = valores_control.std()
            dp_target = valores_target.std()
            n_control = len(valores_control)
            n_target = len(valores_target)
            diferenca_medias = media_target - media_control

            # Cohen's d
            dp_pooled = np.sqrt(((n_control-1)*dp_control**2 + (n_target-1)*dp_target**2) / (n_control + n_target - 2))
            cohen_d = diferenca_medias / dp_pooled if dp_pooled > 0 else 0

            # IC 95% da diferença de médias
            ep_diferenca = np.sqrt(dp_control**2/n_control + dp_target**2/n_target)
            t_critico = stats.t.ppf(1 - alpha/2, df=n_control+n_target-2)
            ic_inferior = diferenca_medias - t_critico * ep_diferenca
            ic_superior = diferenca_medias + t_critico * ep_diferenca

        else:
            stat_mw = p_value_mw = np.nan
            t_stat_w = p_value_w = np.nan
            t_stat_n = p_value_n = np.nan
            media_control = media_target = np.nan
            dp_control = dp_target = np.nan
            n_control = n_target = 0
            diferenca_medias = cohen_d = np.nan
            ic_inferior = ic_superior = np.nan
            decisao_mw = decisao_welch = decisao_t_normal = "Dados insuficientes"

        resultados.append({
            'janela': col,
            'total_control': total_control,
            'conv_control': conv_control,
            'total_target': total_target,
            'conv_target': conv_target,
            'taxa_control_%': round(taxa_control * 100, 4),
            'taxa_target_%': round(taxa_target * 100, 4),
            'diferenca_taxa_%': round(diferenca_taxa * 100, 4),
            'z_stat': z_stat,
            'p_value_proporcao': p_value_prop,
            'ic_inf_prop_%': round(ic_inf_prop * 100, 4) if not np.isnan(ic_inf_prop) else np.nan,
            'ic_sup_prop_%': round(ic_sup_prop * 100, 4) if not np.isnan(ic_sup_prop) else np.nan,
            'decisao_proporcao': decisao_prop,
            'media_valor_control': media_control,
            'media_valor_target': media_target,
            'diferenca_medias': diferenca_medias,
            'cohen_d': cohen_d,
            'ic_inf_media': ic_inferior,
            'ic_sup_media': ic_superior,
            'mw_stat': stat_mw,
            'mw_p_value': p_value_mw,
            'decisao_mw': decisao_mw,
            't_welch_stat': t_stat_w,
            't_welch_p_value': p_value_w,
            'decisao_welch': decisao_welch,
            't_normal_stat': t_stat_n,
            't_normal_p_value': p_value_n,
            'decisao_t_normal': decisao_t_normal
        })

    return pd.DataFrame(resultados)




def teste_proporcao_por_janela(df):
    resultados = []

    # colunas de conversão (flags)
    colunas_flags = [c for c in df.columns if c.startswith("converteu_")]

    for col in colunas_flags:

        # separar grupos
        control = df[df['is_target'] == 'control']
        target  = df[df['is_target'] == 'target']

        # conversões
        conv_control = control[col].sum()
        conv_target  = target[col].sum()

        # totais
        total_control = control['customer_id'].nunique()
        total_target  = target['customer_id'].nunique()

        # teste de proporção
        count = np.array([conv_target, conv_control])
        nobs  = np.array([total_target, total_control])

        z_stat, p_value = proportions_ztest(count, nobs)

        taxa_control = conv_control / total_control if total_control > 0 else 0
        taxa_target  = conv_target / total_target if total_target > 0 else 0

        resultados.append({
            'janela': col,
            'total_control': total_control,
            'conv_control': conv_control,
            'total_target': total_target,
            'conv_target': conv_target,
            'taxa_control': round(taxa_control*100, 2),
            'taxa_target': round(taxa_target*100, 2),
            'z_stat': z_stat,
            'p_value': p_value,
            'significativo_5%': p_value < 0.05
        })

    return pd.DataFrame(resultados)



def teste_proporcao_por_janela(df):
    resultados = []

    # colunas de conversão (flags)
    colunas_flags = [c for c in df.columns if c.startswith("converteu_")]

    for col in colunas_flags:

        # separar grupos
        control = df[df['is_target'] == 'control']
        target  = df[df['is_target'] == 'target']

        # conversões
        conv_control = control[col].sum()
        conv_target  = target[col].sum()

        # totais
        total_control = control['customer_id'].nunique()
        total_target  = target['customer_id'].nunique()

        # teste de proporção
        count = np.array([conv_target, conv_control])
        nobs  = np.array([total_target, total_control])

        z_stat, p_value = proportions_ztest(count, nobs)

        taxa_control = conv_control / total_control if total_control > 0 else 0
        taxa_target  = conv_target / total_target if total_target > 0 else 0

        resultados.append({
            'janela': col,
            'total_control': total_control,
            'conv_control': conv_control,
            'total_target': total_target,
            'conv_target': conv_target,
            'taxa_control': round(taxa_control*100, 2),
            'taxa_target': round(taxa_target*100, 2),
            'z_stat': z_stat,
            'p_value': p_value,
            'significativo_5%': p_value < 0.05
        })

    return pd.DataFrame(resultados)


def calcular_poder_do_output(resultado_ab, alpha=0.05):
    """
    Calcula o poder estatístico do teste A/B a partir do seu formato de output
    """
    
    try:
        # Extrair dados do grupo target e control
        target_row = resultado_ab[resultado_ab['is_target'] == 'target'].iloc[0]
        control_row = resultado_ab[resultado_ab['is_target'] == 'control'].iloc[0]
        
        total_target = int(target_row['total_clientes'])
        total_control = int(control_row['total_clientes'])
        
        # Converter taxa de conversão de porcentagem para decimal
        p_target = float(target_row['taxa_conversao']) / 100
        p_control = float(control_row['taxa_conversao']) / 100
        
        # ratio entre tamanhos
        ratio = total_target / total_control

        # effect size (Cohen's h) para duas proporções
        def cohen_h(p1, p2):
            return 2 * (math.asin(math.sqrt(p1)) - math.asin(math.sqrt(p2)))

        h = abs(cohen_h(p_target, p_control))

        analysis = TTestIndPower()
        
        power = analysis.solve_power(
            effect_size=h,
            nobs1=total_control,  # grupo menor como referência
            alpha=alpha,
            ratio=ratio,
            alternative='two-sided'
        )

        return {
            'taxa_control': p_control,
            'taxa_target': p_target,
            'total_control': total_control,
            'total_target': total_target,
            'effect_size_h': h,
            'alpha': alpha,
            'power': power,
            'interpretacao': 'Poder adequado (>80%)' if power > 0.8 else 'Poder insuficiente'
        }
        
    except Exception as e:
        return {'erro': f'Falha ao calcular poder: {str(e)}'}



def analisar_ab_completo(
    df_base,
    alpha=0.05
):
    """
    df_base deve conter:
    customer_id, is_target, converteu, order_total_amount, valor_desconto
    """

    # Resumo por grupo (igual ao seu formato desejado)
    resumo = (
        df_base.groupby("is_target", as_index=False)
        .agg(
            total_clientes=('customer_id', 'nunique'),
            clientes_convertidos=('converteu', 'sum'),
            total_amount=('order_total_amount', 'sum'),
            total_desconto=('valor_desconto', 'sum')
        )
        .assign(
            taxa_conversao=lambda x: (x['clientes_convertidos'] / x['total_clientes'] * 100).round(2),
            total_amount_liquido=lambda x: x['total_amount'] - x['total_desconto'],
            #window_dias=window_days,
            #desconto_percentual=desconto_p * 100,
            #mes_base=mes_base
        )
    )

    # Análise estatística
    resultados_estatisticos = []

    # Extrair valores
    try:
        total_target = resumo.loc[resumo['is_target'] == 'target', 'total_clientes'].values[0]
        total_control = resumo.loc[resumo['is_target'] == 'control', 'total_clientes'].values[0]

        conversoes_target = resumo.loc[resumo['is_target'] == 'target', 'clientes_convertidos'].values[0]
        conversoes_control = resumo.loc[resumo['is_target'] == 'control', 'clientes_convertidos'].values[0]
    except:
        return resumo  # Retorna pelo menos o resumo básico

    if total_target > 0 and total_control > 0 and (conversoes_target + conversoes_control) > 0:

        count = np.array([conversoes_target, conversoes_control])
        nobs = np.array([total_target, total_control])

        z_stat, p_value = proportions_ztest(count, nobs)

        taxa_target = conversoes_target / total_target
        taxa_control = conversoes_control / total_control
        diferenca_absoluta = taxa_target - taxa_control
        lift_relativo = diferenca_absoluta / taxa_control if taxa_control > 0 else 0

        # Intervalos de confiança
        ci_target = proportion_confint(conversoes_target, total_target, alpha=alpha)
        ci_control = proportion_confint(conversoes_control, total_control, alpha=alpha)

        # Adicionar métricas estatísticas ao resumo
        resumo['z_stat'] = z_stat
        resumo['p_value'] = p_value
        resumo['significativo'] = p_value < alpha
        resumo['lift_relativo'] = lift_relativo
        resumo['diferenca_absoluta'] = diferenca_absoluta
        resumo['ic_target_inf'] = ci_target[0]
        resumo['ic_target_sup'] = ci_target[1]
        resumo['ic_control_inf'] = ci_control[0]
        resumo['ic_control_sup'] = ci_control[1]

    return resumo

# Exemplo de uso:
####################
def conversao_imediata(
    df,
    mes_base=12,
    r_ordem=2,
    window_start=1,  # NOVO: início da janela
    window_end=1,    # NOVO: fim da janela  
    fill_value=-999
):
    """
    Versão atualizada que aceita janela range (start, end)
    """
    # 1) Filtra pedidos do mês
    df_publico = df[df['order_created_month'] == mes_base].copy()

    # 2) Clientes que fizeram r_ordem-ésimo pedido no mês
    df_d = df_publico[df_publico['rank_month'] == r_ordem].drop_duplicates().copy()

    # 3) Flag de conversão: dentro da janela range em dias
    df_d['converteu'f"{window_start}-{window_end}d"] = (df_d['days_since_first_order_month'] >= window_start) & (df_d['days_since_first_order_month'] <= window_end)

    # 4) Mantém só o que precisamos da 2ª compra
    df_d = df_d[['customer_id', 'converteu'f"{window_start}-{window_end}d", 'days_since_first_order_month', 'order_total_amount']]

    # 5) Base de todos os clientes ativos no mês (um registro por customer_id x is_target)
    df_publico = df_publico[['customer_id', 'is_target']].drop_duplicates().merge(
        df_d,
        on='customer_id',
        how='left'
    )

    # 6) Tratar tipos / NaNs
    df_publico['converteu'f"{window_start}-{window_end}d"] = df_publico['converteu'f"{window_start}-{window_end}d"].fillna(False).astype(int)
    df_publico['order_total_amount'] = df_publico['order_total_amount'].fillna(0).astype(float)

    # Se quiser -999 onde ainda sobrou NaN (ex: days_since_first_order_month)
    df_publico = df_publico.fillna(fill_value)
    df_publico['windows']=f"{window_start}-{window_end}d"
    # 8) Resumo por grupo
    resumo = (
        df_publico.groupby("is_target", as_index=False)
                  .agg(
                      total_clientes=('customer_id', 'nunique'),
                      clientes_convertidos=('converteu'f"{window_start}-{window_end}d", 'sum'),
                      total_amount=('order_total_amount', 'sum'),
                     total_amount_convertido=('order_total_amount',lambda x: x[df_publico.loc[x.index, 'converteu'f"{window_start}-{window_end}d"] == 1].sum())
                  ).assign(
                taxa_conversao=lambda x: (x['clientes_convertidos'] / x['total_clientes'] * 100).round(2),
                window_range=f"{window_start}-{window_end}d",

                mes_base=mes_base
            ).reset_index(drop=True)
    )

    return resumo, df_publico
#######################
def pedidos_group(df, group_vars=None):
    """
    Gera análise completa por mês e categoria de pedidos:
    - Classifica clientes em 1_pedido ou 2+_pedidos
    - Calcula totais e percentuais por mês

    Retorna um DataFrame com métricas e percentuais.
    """

    # Definir variáveis de agrupamento padrão
    if group_vars is None:
        group_vars = []
    # Se group_vars for string, converter para lista
    elif isinstance(group_vars, str):
        group_vars = [group_vars]
    
    # Variáveis base para agrupamento
    base_group_vars = ['order_created_month', 'customer_id'] + group_vars

    df_analise_completa = (
        df.groupby(base_group_vars)
        .agg(
            total_pedidos=('unique_order_hash', 'count'),
            total_amount=('order_total_amount', 'sum')
        )
        .reset_index()
        .assign(
            categoria_pedidos=lambda x: x['total_pedidos']
                .apply(lambda y: '1_pedido' if y == 1 else '2+_pedidos')
        )
        .groupby(['order_created_month', 'categoria_pedidos'] + group_vars)
        .agg(
            total_clientes=('customer_id', 'nunique'),
            total_pedidos=('total_pedidos', 'sum'),
            total_amount=('total_amount', 'sum'),
            avg_amount_por_cliente=('total_amount', 'mean')
        )
        .reset_index()
    )

    # Calcular totais por mês (e variáveis adicionais se houver)
    total_vars = ['order_created_month'] + group_vars
    
    totais_mes = (
        df_analise_completa
        .groupby(total_vars)
        .agg(
            total_clientes_mes=('total_clientes', 'sum'),
            total_pedidos_mes=('total_pedidos', 'sum'),
            total_amount_mes=('total_amount', 'sum')
        )
        .reset_index()
    )

    df_analise_completa = (
        df_analise_completa
        .merge(totais_mes, on=total_vars)
        .assign(
            perc_clientes=lambda x: (x['total_clientes'] / x['total_clientes_mes'] * 100).round(2),
            perc_pedidos=lambda x: (x['total_pedidos'] / x['total_pedidos_mes'] * 100).round(2),
            perc_amount=lambda x: (x['total_amount'] / x['total_amount_mes'] * 100).round(2)
        )
        .drop(['total_clientes_mes', 'total_pedidos_mes', 'total_amount_mes'], axis=1)
        .sort_values(['order_created_month', 'categoria_pedidos'] + group_vars)
        .reset_index(drop=True)
    )

    return df_analise_completa


def criacao_ordens(df, group_vars=None):
    
    # Definir variáveis de agrupamento padrão
    if group_vars is None:
        group_vars = []
    # Se group_vars for string, converter para lista
    elif isinstance(group_vars, str):
        group_vars = [group_vars]
    
    # Variáveis base para agrupamento
    base_group_vars = ['customer_id', 'order_created_month'] + group_vars
    
    df = df.sort_values(base_group_vars + ['order_created_at']).copy()

    g = df.groupby(base_group_vars, sort=False)

    # Rank da ordem
    df['rank_month'] = g.cumcount() + 1

    # Datas anterior e próxima
    prev_date = g['order_created_at'].shift()
    next_date = g['order_created_at'].shift(-1)

    # Diferença entre pedidos
    df['days_between_orders'] = (df['order_created_at'] - prev_date).dt.days
    df.loc[df['rank_month'] == 1, 'days_between_orders'] = -9999.0

    # Dias desde a primeira ordem
    first_date = g['order_created_at'].transform('min')
    df['days_since_first_order_month'] = (df['order_created_at'] - first_date).dt.days
    df.loc[df['rank_month'] == 1, 'days_since_first_order_month'] = -999.0

    # Diferença de valor
    prev_amount = g['order_total_amount'].shift()
    df['amount_diff_from_previous'] = df['order_total_amount'] - prev_amount
    df.loc[df['rank_month'] == 1, 'amount_diff_from_previous'] = -999.0

    # % variação
    df['amount_pct_change_from_previous'] = (
        (df['order_total_amount'] - prev_amount) / prev_amount * 100
    ).replace([np.inf, -np.inf], 0).fillna(0)

    # Dias até próxima ordem
    df['days_until_next_order'] = (next_date - df['order_created_at']).dt.days.fillna(-999.0)

    # Última ordem do mês
    df['is_last_order_month'] = df['order_created_at'].eq(
        g['order_created_at'].transform('max')
    )

    # Métrica final
    avg_days_first_to_second = df.loc[
        df['rank_month'] == 2,
        'days_since_first_order_month'
    ].mean()

    return df, avg_days_first_to_second
#################################

    """
    Gera análise completa por mês e categoria de pedidos:
    - Classifica clientes em 1_pedido ou 2+_pedidos
    - Calcula totais e percentuais por mês

    Retorna um DataFrame com métricas e percentuais.
    """

    # Definir variáveis de agrupamento padrão
    if group_vars is None:
        group_vars = []
    
    # Variáveis base para agrupamento
    base_group_vars = ['order_created_month', 'customer_id'] + group_vars

    df_analise_completa = (
        df.groupby(base_group_vars)
        .agg(
            total_pedidos=('unique_order_hash', 'count'),
            total_amount=('order_total_amount', 'sum')
        )
        .reset_index()
        .assign(
            categoria_pedidos=lambda x: x['total_pedidos']
                .apply(lambda y: '1_pedido' if y == 1 else '2+_pedidos')
        )
        .groupby(['order_created_month', 'categoria_pedidos'] + group_vars)
        .agg(
            total_clientes=('customer_id', 'nunique'),
            total_pedidos=('total_pedidos', 'sum'),
            total_amount=('total_amount', 'sum'),
            avg_amount_por_cliente=('total_amount', 'mean')
        )
        .reset_index()
    )

    # Calcular totais por mês (e variáveis adicionais se houver)
    total_vars = ['order_created_month'] + group_vars
    
    totais_mes = (
        df_analise_completa
        .groupby(total_vars)
        .agg(
            total_clientes_mes=('total_clientes', 'sum'),
            total_pedidos_mes=('total_pedidos', 'sum'),
            total_amount_mes=('total_amount', 'sum')
        )
        .reset_index()
    )

    df_analise_completa = (
        df_analise_completa
        .merge(totais_mes, on=total_vars)
        .assign(
            perc_clientes=lambda x: (x['total_clientes'] / x['total_clientes_mes'] * 100).round(2),
            perc_pedidos=lambda x: (x['total_pedidos'] / x['total_pedidos_mes'] * 100).round(2),
            perc_amount=lambda x: (x['total_amount'] / x['total_amount_mes'] * 100).round(2)
        )
        .drop(['total_clientes_mes', 'total_pedidos_mes', 'total_amount_mes'], axis=1)
        .sort_values(['order_created_month', 'categoria_pedidos'] + group_vars)
        .reset_index(drop=True)
    )

    return df_analise_completa


############# plot_indicadores_mes_plataforma

def plot_indicadores_mes_plataforma(summary_mes):
    """
    Gera gráfico de barras verticais com:
    - Indicadores (% Clientes, % Pedidos, % Valor Total)
    - Eixo X organizado por Plataforma
    - Agrupamento visual por Mês
    """

    metrics = ['perc_clientes', 'perc_pedidos', 'perc_amount']
    metric_names = ['% Clientes', '% Pedidos', '% Valor Total']

    data = summary_mes.copy()

    # Ordenar por mês e plataforma
    data = data.sort_values(['order_created_month', 'origin_platform'])

    # Label por barra (plataforma)
    data['label'] = data['origin_platform']

    labels = data['label'].tolist()
    meses = data['order_created_month'].tolist()

    x = np.arange(len(labels))
    width = 0.25
    multiplier = 0

    fig, ax = plt.subplots(figsize=(16, 7), layout='constrained')

    indicadores = {
        '% Clientes': data['perc_clientes'].values,
        '% Pedidos': data['perc_pedidos'].values,
        '% Valor Total': data['perc_amount'].values
    }

    for nome_indicador, valores in indicadores.items():
        offset = width * multiplier
        rects = ax.bar(x + offset, valores, width, label=nome_indicador)
        ax.bar_label(rects, padding=3)
        multiplier += 1

    # === EIXO X COM PLATAFORMA ===
    ax.set_xticks(x + width)
    ax.set_xticklabels(labels, rotation=0)

    # === AGRUPAMENTO VISUAL POR MÊS ===
    unique_months = data['order_created_month'].unique()
    start = 0

    for mes in unique_months:
        count = (data['order_created_month'] == mes).sum()
        center = start + count / 2 - 0.5
        ax.text(center, -0.05, str(mes),
                ha='center', va='top',
                transform=ax.get_xaxis_transform(),
                fontsize=11, fontweight='bold')
        start += count

    ax.set_xlabel('Plataforma')
    ax.set_ylabel('Percentual')
    ax.set_title('Indicadores por Mês e Plataforma', fontsize=16, fontweight='bold')
    ax.legend()

    plt.show()


##################
def gerar_stats(df, group_cols):
    """
    df         -> seu DataFrame original
    group_cols -> lista de colunas para agrupar (ex: ["weekday"], ["weekday","hour"], etc.)
    """

    df_stats_mes = (
        df.groupby(['order_created_month'] + group_cols)
          .agg(
              total_clientes=('customer_id', 'nunique'),
              total_pedidos=('unique_order_hash', 'count'),
              total_ordem=('order_total_amount', 'sum')
          )
          .reset_index()
          .assign(
              perc_clientes=lambda d: d.groupby('order_created_month')['total_clientes']
                                         .transform(lambda x: x / x.sum() * 100),
              perc_pedidos=lambda d: d.groupby('order_created_month')['total_pedidos']
                                        .transform(lambda x: x / x.sum() * 100),
              perc_amount=lambda d: d.groupby('order_created_month')['total_ordem']
                                         .transform(lambda x: x / x.sum() * 100)
          )
          .round(2)
          .sort_values(['order_created_month', 'total_clientes'], ascending=[True, False])
          .reset_index(drop=True)
    )

    return df_stats_mes

#################
def cria_base_decil_wide(
    df,
    mes_0,
    col_cliente="customer_id",
    col_mes="order_created_month",
    col_valor="total_amount_mes", 
    col_target="is_target",
    n_decis=10,
    prefixo_decil="decil",
):
    # 1) Preparar base do mês de referência
    cols_base = [col_cliente, col_mes, col_valor, col_target]
    base_m0 = (
        df[df[col_mes] == mes_0][cols_base]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    col_decil_m0 = f"{prefixo_decil}_{mes_0}"

    # 2) Criar decis 
    labels = [f"decil {i+1}" for i in range(n_decis)]

    base_m0[col_decil_m0], bins = pd.qcut(
        base_m0[col_valor],
        q=n_decis,
        labels=labels,
        retbins=True,
        duplicates="drop"
    )

    # 3) Criar dicionário no formato CORRETO: {decil: (min, max)}
    decil_dict_corrigido = {}
    for i in range(len(bins) - 1):
        decil_num = i + 1
        decil_dict_corrigido[decil_num] = (bins[i], bins[i+1])

    return decil_dict_corrigido


#############################################################

def matriz_migracao_n(df, mes_0, mes_1, group_by_extra=None):
    """Versão compacta"""
    nome_col_0, nome_col_1 = f"mes_{mes_0}", f"mes_{mes_1}"
    
    index_cols = ['customer_id', 'is_target'] + ([] if group_by_extra is None else 
                [group_by_extra] if isinstance(group_by_extra, str) else group_by_extra)
    
    clientes_temp = (
        df[df['order_created_month'].isin([mes_0, mes_1])]
        .drop_duplicates(['customer_id', 'order_created_month'] + index_cols[2:])
        .assign(presenca=1)
        .pivot_table(index=index_cols, columns='order_created_month', values='presenca', fill_value=0)
        .reset_index()
        .rename(columns={mes_0: nome_col_0, mes_1: nome_col_1})
    )
    
    for col in [nome_col_0, nome_col_1]:
        if col not in clientes_temp.columns:
            clientes_temp[col] = 0
        clientes_temp[col] = clientes_temp[col].astype(int)
    
    group_by_cols = [nome_col_0, nome_col_1, 'is_target'] + ([] if group_by_extra is None else 
                   [group_by_extra] if isinstance(group_by_extra, str) else group_by_extra)
    
    return (clientes_temp.groupby(group_by_cols).size()
                        .reset_index(name='total_clientes')
                        .sort_values([nome_col_0, nome_col_1]))

