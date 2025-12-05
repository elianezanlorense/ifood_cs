import numpy as np
from scipy.stats import shapiro,zscore

def outlier_method(df,var=''):
    """
    Escolha automática baseada nos dados
    """
    
    # Teste de normalidade
    stat, p_val = shapiro(df[var])
    
    if p_val > 0.05:  # Normal
        print("Use Z-Score (dados normais)")
        return "zscore"
    else:  # Não-normal
        skew = abs(df[var].skew())
        if skew > 1: 
            print("Use MAD (assimétrico + robusto)")
            return "mad"
        else:
            print("Use IQR (simples e eficaz)")
            return "iqr"
        

def mark_outliers_iqr_zscore_mad(df, mad_thresh=3.5):
    """Marca outliers usando IQR, Z-Score e MAD agrupado por mês/grupo"""
    
    # Inicializar colunas
    df[['outlier_iqr', 'outlier_zscore', 'outlier_mad', 
        'lim_inf_iqr', 'lim_sup_iqr', 'zscore', 'mad_score']] = False, False, False, np.nan, np.nan, np.nan, np.nan
    
    # Processar cada combinação mês/grupo
    for (mes, grupo), group_df in df[df['order_total_amount'] > 0].groupby(['order_created_month', 'is_target']):
        if len(group_df) < 5:
            continue
            
        mask = (df['order_created_month'] == mes) & (df['is_target'] == grupo)
        data = df.loc[mask, 'order_total_amount']
        
        # IQR
        p25, p75 = np.percentile(data, [25, 75])
        iqr = p75 - p25
        lim_sup, lim_inf = p75 + 1.5*iqr, max(p25 - 1.5*iqr, 0.01)
        
        df.loc[mask, ['lim_inf_iqr', 'lim_sup_iqr']] = lim_inf, lim_sup
        df.loc[mask & ((data < lim_inf) | (data > lim_sup)), 'outlier_iqr'] = True
        
        # Z-Score
        z_vals = zscore(data)
        df.loc[mask, 'zscore'] = z_vals
        df.loc[mask & (np.abs(z_vals) > 3), 'outlier_zscore'] = True
        
        # MAD
        med = np.median(data)
        mad = np.median(np.abs(data - med))
        if mad > 0:
            mad_vals = 0.6745 * (data - med) / mad
            df.loc[mask, 'mad_score'] = mad_vals
            df.loc[mask & (np.abs(mad_vals) > mad_thresh), 'outlier_mad'] = True
    
    return df