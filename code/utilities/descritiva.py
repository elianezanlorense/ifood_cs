import pandas as pd

import matplotlib.pyplot as plt
import numpy as np

import matplotlib.pyplot as plt
import numpy as np

import numpy as np
###################################
import numpy as np

import numpy as np
import numpy as np


##############
import numpy as np
import pandas as pd



import numpy as np
import pandas as pd
from statsmodels.stats.proportion import proportions_ztest, proportion_confint

import numpy as np
from statsmodels.stats.power import TTestIndPower
import math

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

#### 

def conversao_imediata(
    df,
    mes_base=12,
    r_ordem=2,
    desconto_p=0.1,
    window_days=1,
    fill_value=-999
):
    """
    Analisa o efeito do cupom em um mês específico.

    Parâmetros
    ----------
    df : DataFrame
        Base de pedidos, com colunas:
        - customer_id
        - is_target
        - order_created_month
        - rank_month
        - days_since_first_order_month
        - order_total_amount

    mes_base : int
        Mês que você quer analisar (ex: 12 = dezembro).

    r_ordem : int
        Ordem do pedido que conta como conversão (ex: 2 = segundo pedido).

    desconto_p : float
        Percentual de desconto aplicado no grupo target (ex: 0.1 = 10%).

    window_days : int
        Janela em dias desde o primeiro pedido para considerar conversão.

    fill_value : valor para preencher NaNs restantes (default = -999).

    Retorna
    -------
    resumo : DataFrame
        Métricas agregadas por is_target.
    df_publico : DataFrame
        Base cliente a cliente do mês analisado, com flag de conversão e valor_desconto.
    """

    # 1) Filtra pedidos do mês
    df_publico = df[df['order_created_month'] == mes_base].copy()

    # 2) Clientes que fizeram r_ordem-ésimo pedido no mês
    df_d = df_publico[df_publico['rank_month'] == r_ordem].drop_duplicates().copy()

    # 3) Flag de conversão: dentro da janela em dias
    df_d['converteu'] = df_d['days_since_first_order_month'] <= window_days

    # 4) Mantém só o que precisamos da 2ª compra
    df_d = df_d[['customer_id', 'converteu', 'days_since_first_order_month', 'order_total_amount']]

    # 5) Base de todos os clientes ativos no mês (um registro por customer_id x is_target)
    df_publico = df_publico[['customer_id', 'is_target']].drop_duplicates().merge(
        df_d,
        on='customer_id',
        how='left'
    )

    # 6) Tratar tipos / NaNs
    df_publico['converteu'] = df_publico['converteu'].fillna(False).astype(int)
    df_publico['order_total_amount'] = df_publico['order_total_amount'].fillna(0).astype(float)

    # Se quiser -999 onde ainda sobrou NaN (ex: days_since_first_order_month)
    df_publico = df_publico.fillna(fill_value)

    # 7) Calcular valor_desconto só para target que converteu
    df_publico['valor_desconto'] = np.where(
        (df_publico['is_target'] == 'target') & (df_publico['converteu'] == 1),
        df_publico['order_total_amount'] * desconto_p,
        0.0
    )

    df_publico['amount_pos_desc'] = np.where(
        (df_publico['is_target'] == 'target') & (df_publico['converteu'] == 1),
        (df_publico['order_total_amount'] - df_publico['valor_desconto'])
        ,
        df_publico['order_total_amount']
    )

    # 8) Resumo por grupo
    resumo = (
        df_publico.groupby("is_target", as_index=False)
                  .agg(
                      total_clientes=('customer_id', 'nunique'),
                      clientes_convertidos=('converteu', 'sum'),
                      total_amount=('order_total_amount', 'sum'),
                      total_desconto=('valor_desconto', 'sum'),
                  ).assign(
                taxa_conversao=lambda x: (x['clientes_convertidos'] / x['total_clientes'] * 100).round(2),
                total_amount_liquido=lambda x: x['total_amount'] - x['total_desconto'],
                window_dias=window_days,
                desconto_percentual=desconto_p * 100,
                mes_base=mes_base
            ).reset_index(drop=True)
    )

    return resumo, df_publico

#######################



def criacao_ordens(df):
    
    df = df.sort_values(['customer_id', 'order_created_month', 'order_created_at']).copy()

    g = df.groupby(['customer_id', 'order_created_month'], sort=False)

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
def pedidos_group(df):
    """
    Gera análise completa por mês e categoria de pedidos:
    - Classifica clientes em 1_pedido ou 2+_pedidos
    - Calcula totais e percentuais por mês

    Retorna um DataFrame com métricas e percentuais.
    """

    df_analise_completa = (
        df.groupby(['order_created_month', 'customer_id'])
        .agg(
            total_pedidos=('unique_order_hash', 'count'),
            total_amount=('order_total_amount', 'sum')
        )
        .reset_index()
        .assign(
            categoria_pedidos=lambda x: x['total_pedidos']
                .apply(lambda y: '1_pedido' if y == 1 else '2+_pedidos')
        )
        .groupby(['order_created_month', 'categoria_pedidos'])
        .agg(
            total_clientes=('customer_id', 'nunique'),
            total_pedidos=('total_pedidos', 'sum'),
            total_amount=('total_amount', 'sum'),
            avg_amount_por_cliente=('total_amount', 'mean')
        )
        .reset_index()
    )

    df_analise_completa = (
        df_analise_completa
        .merge(
            df_analise_completa.groupby('order_created_month')['total_clientes']
            .sum()
            .rename('total_clientes_mes'),
            on='order_created_month'
        )
        .assign(
            perc_clientes=lambda x: (x['total_clientes'] / x['total_clientes_mes'] * 100).round(2),
            perc_pedidos=lambda x: (
                x['total_pedidos'] /
                x.groupby('order_created_month')['total_pedidos'].transform('sum') * 100
            ).round(2),
            perc_amount=lambda x: (
                x['total_amount'] /
                x.groupby('order_created_month')['total_amount'].transform('sum') * 100
            ).round(2)
        )
        .drop('total_clientes_mes', axis=1)
        .sort_values(['order_created_month', 'categoria_pedidos'])
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



   