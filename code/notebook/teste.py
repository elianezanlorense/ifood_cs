import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportions_ztest
from scipy import stats
from scipy.stats import mannwhitneyu, ttest_ind
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