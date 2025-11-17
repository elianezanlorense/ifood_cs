#!/usr/bin/env python3
"""
Análise dos dados do iFood - Conversão do Jupyter Notebook
"""

import sys
import os
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    import utilities.functions as functions
    import importlib
    importlib.reload(functions)
    
    from utilities.functions import (
        summary,
        matriz_migracao,
        save_parquet,
        gerar_resumo_decis,
        resumo_coorte_ativa,
        cria_base_decil_wide
    )
except ImportError as e:
    print(f": {e}")

def executar_analise_completa():
    """
    Define matirx de migracao, pedidos por mes e decil
    """
   
    
    try:
        # Load data
        df_pub_u = pd.read_parquet("../dados/gold/df_pub_un.parquet")
  
        
        # Summary
        summary(df_pub_u)
        
        # Clientes por mês e target
        clientes_mes = (df_pub_u.groupby(['order_created_month', 'is_target'])
                      ['customer_id']
                      .nunique()
                      .reset_index(name='numero_clientes_distintos'))
        print(clientes_mes)
        
        # Matriz de migração
        matriz_migracao(df_pub_u, mes_0=12, mes_1=1)
        print(matriz_migracao)
        
        # Divide em janeiro e dezmenro
        id_both_monht = df_pub_u[df_pub_u['order_created_month']==12]['customer_id'].unique()
        publico_janeiro_dezembro = df_pub_u[df_pub_u['customer_id'].isin(id_both_monht)].reset_index(drop=True)
        publico_janeiro = df_pub_u[~df_pub_u['customer_id'].isin(id_both_monht)].reset_index(drop=True)
        
        # Salva publico de dezembro
        save_parquet(publico_janeiro_dezembro, "gold", "publico_janeiro_dezembro.parquet")
        
        # Pedidos
        pedidos_total = (
            publico_janeiro_dezembro.groupby(['is_target','order_created_month'])['num_pedidos_mes']
              .sum()
              .reset_index(name='numero_de_pedidos_total')
        )
        print(pedidos_total)
        
        # Descritiva amount
        df_stats_mes = publico_janeiro_dezembro.groupby(['order_created_month', 'is_target'])['total_amount_mes'].agg(
            Média=('mean'),
            Mediana=('median'),
            Mínimo=('min'),
            Máximo=('max'),
            Desvio_Padrão=('std')
        ).round(2)
        print(df_stats_mes)
        
        # Consytoi decil
        df_decil, decil_dict = cria_base_decil_wide(
            publico_janeiro_dezembro,
            mes_0=12,  
            mes_1=1,  
            col_cliente="customer_id",
            col_mes="order_created_month",
            col_pedidos="num_pedidos_mes",
            col_valor="total_amount_mes",
            col_target="is_target",  
            n_decis=10,
            prefixo_decil="decil",
            prefixo_pedidos="num_pedidos_mes",
            prefixo_valor="total_amount_mes"
        )
        
        #Salva decil
        save_parquet(df_decil, "gold", "df_decil.parquet")
        
        decil_dict_json = {k: list(v) for k, v in decil_dict.items()}
        with open('../dados/gold/decil_dictionary.json', 'w') as f:
            json.dump(decil_dict_json, f, indent=2)
        
        # Descritiva decil
        decil_summary = gerar_resumo_decis(df_decil)
        decil_summary.to_csv('../Resultados/decil_summary.csv', index=False)
        
        # Corte - numero de pedidos
        pedidos_hist = resumo_coorte_ativa(publico_janeiro_dezembro,mes_coorte_inicio=12,mes_coorte_fim=1)
        pedidos_hist.to_csv('../Resultados/pedidos_hist.csv', index=False)
        return True
        
    except Exception as e:
        print(f": {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    executar_analise_completa()