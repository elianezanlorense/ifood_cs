def matriz_migracao(df, mes_0, mes_1):
    """
    Gera matriz de migração entre dois meses - VERSÃO OTIMIZADA
    """
    nome_col_0 = f"mes_{mes_0}"
    nome_col_1 = f"mes_{mes_1}"

    # 1) Filtrar apenas os meses relevantes
    df_filtrado = df[df['order_created_month'].isin([mes_0, mes_1])]
    
    # 2) Criar matriz usando pivot_table (mais eficiente)
    clientes_temp = (
        df_filtrado
        .drop_duplicates(['customer_id', 'order_created_month'])
        .assign(presenca=1)
        .pivot_table(
            index=['customer_id', 'is_target'],
            columns='order_created_month',
            values='presenca',
            fill_value=0
        )
        .reset_index()
        .rename(columns={mes_0: nome_col_0, mes_1: nome_col_1})
    )

    # 3) Garantir que as colunas existam
    for col in [nome_col_0, nome_col_1]:
        if col not in clientes_temp.columns:
            clientes_temp[col] = 0

    # 4) Converter para 0/1
    clientes_temp[nome_col_0] = clientes_temp[nome_col_0].astype(int)
    clientes_temp[nome_col_1] = clientes_temp[nome_col_1].astype(int)

    # 5) Contar clientes
    resultado = (
        clientes_temp
        .groupby([nome_col_0, nome_col_1, 'is_target'])
        .size()
        .reset_index(name='total_clientes')
        .sort_values([nome_col_0, nome_col_1])
    )

    return resultado