import pandas as pd

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

def matriz_migracao(df, mes_0, mes_1, group_by_extra=None):
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



   