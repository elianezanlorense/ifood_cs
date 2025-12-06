
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
from utilities.functions import (
    
    conversao_imediata
  
)

import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportions_ztest


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

def testes(
    df, 
    ranges_janelas, 
    mes_base=12, 
    r_ordem=2, 
    alpha=0.05
):
    """
    Versão OTIMIZADA - análise silenciosa de janelas
    """
    
    resultados_combinados = []
    
    # PRÉ-FILTRAGEM: Filtrar por mês uma vez
    mask_mes = df['order_created_month'] == mes_base
    df_mes = df[mask_mes].copy()
    
    for janela_inicio, janela_fim in ranges_janelas:
        # Executa função de conversão UMA VEZ por janela
        resumo, df_publico_mes = conversao_imediata(
            df_mes,
            mes_base=mes_base,
            r_ordem=r_ordem,
            window_start=janela_inicio,
            window_end=janela_fim,
            fill_value=-999
        )
        
        # Extração otimizada dos dados
        try:
            resumo_target = resumo[resumo['is_target'] == 'target'].iloc[0]
            resumo_control = resumo[resumo['is_target'] == 'control'].iloc[0]
            
            total_target = resumo_target['total_clientes']
            total_control = resumo_control['total_clientes']
            conversoes_target = resumo_target['clientes_convertidos']
            conversoes_control = resumo_control['clientes_convertidos']
            
            # 🔥 ANÁLISE DE PROPORÇÃO
            if total_target > 0 and total_control > 0:
                count = np.array([conversoes_target, conversoes_control])
                nobs = np.array([total_target, total_control])
                
                z_stat_prop, p_value_prop = proportions_ztest(count, nobs)
                taxa_target = conversoes_target / total_target
                taxa_control = conversoes_control / total_control
                diferenca_absoluta = taxa_target - taxa_control
                lift_relativo = diferenca_absoluta / taxa_control if taxa_control > 0 else 0
                
                ci_target = proportion_confint(conversoes_target, total_target, alpha=alpha)
                ci_control = proportion_confint(conversoes_control, total_control, alpha=alpha)
                
                # Adiciona métricas ao resumo
                for col, valor in [
                    ('z_stat_proporcao', z_stat_prop),
                    ('p_value_proporcao', p_value_prop),
                    ('significativo_proporcao', p_value_prop < alpha),
                    ('lift_relativo', lift_relativo),
                    ('diferenca_absoluta', diferenca_absoluta)
                ]:
                    resumo[col] = valor
                
                resumo.loc[resumo['is_target'] == 'target', 'ic_target_inf'] = ci_target[0]
                resumo.loc[resumo['is_target'] == 'target', 'ic_target_sup'] = ci_target[1]
                resumo.loc[resumo['is_target'] == 'control', 'ic_control_inf'] = ci_control[0]
                resumo.loc[resumo['is_target'] == 'control', 'ic_control_sup'] = ci_control[1]
        except Exception:
            resumo['z_stat_proporcao'] = np.nan
            resumo['p_value_proporcao'] = np.nan
            resumo['significativo_proporcao'] = False
        
        # 🔥 ANÁLISE DOS VALORES
        try:
            mask_periodo = (
                (df_publico_mes['days_since_first_order_month'] >= janela_inicio) &
                (df_publico_mes['days_since_first_order_month'] <= janela_fim) &
                (df_publico_mes['order_total_amount'] >= 0)
            )
            
            df_filtrado = df_publico_mes[mask_periodo]
            
            valores_control = df_filtrado[df_filtrado['is_target'] == 'control']['order_total_amount']
            valores_target = df_filtrado[df_filtrado['is_target'] == 'target']['order_total_amount']
            
            if len(valores_control) > 0 and len(valores_target) > 0:
                n_control = len(valores_control)
                n_target = len(valores_target)
                
                media_control = valores_control.mean()
                media_target = valores_target.mean()
                dp_control = valores_control.std()
                dp_target = valores_target.std()
                
                # Testes estatísticos
                stat_mw, p_value_mw = mannwhitneyu(valores_target, valores_control, alternative='two-sided')
                t_stat_w, p_value_w = ttest_ind(valores_target, valores_control, equal_var=False)
                
                # Cohen's d
                dp_pooled = np.sqrt(((n_control-1)*dp_control**2 + (n_target-1)*dp_target**2) / (n_control + n_target - 2))
                cohen_d = (media_target - media_control) / dp_pooled
                
                # IC da diferença
                ep_diferenca = np.sqrt(dp_control**2/n_control + dp_target**2/n_target)
                t_critico = stats.t.ppf(0.975, df=n_control+n_target-2)
                diferenca = media_target - media_control
                
                # Adiciona métricas
                metricas = {
                    'mannwhitney_stat': stat_mw,
                    'mannwhitney_pvalue': p_value_mw,
                    'ttest_welch_stat': t_stat_w,
                    'ttest_welch_pvalue': p_value_w,
                    'cohen_d': cohen_d,
                    'diferenca_medias': diferenca,
                    'ic_medias_inferior': diferenca - t_critico * ep_diferenca,
                    'ic_medias_superior': diferenca + t_critico * ep_diferenca,
                    'media_control': media_control,
                    'media_target': media_target,
                    'significativo_valores': p_value_w < alpha,
                    'n_transacoes_target': n_target,
                    'n_transacoes_control': n_control,
                    'soma_real_target': valores_target.sum(),
                    'soma_real_control': valores_control.sum()
                }
                
                for col, valor in metricas.items():
                    resumo[col] = valor
                    
        except Exception:
            resumo['mannwhitney_pvalue'] = np.nan
            resumo['ttest_welch_pvalue'] = np.nan
            resumo['cohen_d'] = np.nan
            resumo['significativo_valores'] = False
        
        # Adiciona informações da janela
        resumo = resumo.copy()
        resumo['janela_inicio'] = janela_inicio
        resumo['janela_fim'] = janela_fim
        resumo['janela_range'] = f"{janela_inicio}-{janela_fim}d"
        
        resultados_combinados.append(resumo)
    
    # Combina resultados
    if resultados_combinados:
        df_resultados_completo = pd.concat(resultados_combinados, ignore_index=True)
    else:
        df_resultados_completo = pd.DataFrame()
    
    return df_resultados_completo