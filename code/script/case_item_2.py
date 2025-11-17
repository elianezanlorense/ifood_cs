#!/usr/bin/env python
# coding: utf-8

# In[22]:


import importlib
import utilities.functions as functions
import pandas as pd
importlib.reload(functions)
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import json

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utilities.functions as functions

from utilities.functions import (
    retidos11,
    calcula_viabilidade_wide,
    segmentacao_3_otimizada


)


# In[19]:

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code'))


import numpy as np
os.makedirs('../Resultados', exist_ok=True)
df = pd.read_parquet("dados/gold/df_decil.parquet")

df.head()


# In[20]:


df['decil_1_num'] = df['decil_1'].str.extract(r'(\d+)').astype(int)
df['decil_12_num'] = df['decil_12'].str.extract(r'(\d+)').astype(int)
df['delta_decil'] = df['decil_12_num'] - df['decil_1_num']


# In[23]:


df=segmentacao_3_otimizada(df)


# In[24]:


tabela_segmento = (
    df
    .groupby(['segmento_mobilidade', 'direcao', 'is_target'])
    .agg(
        clientes=('customer_id', 'nunique'),
        decil_inicial_medio=('decil_1_num', 'mean'),
        decil_final_medio=('decil_12_num', 'mean'),
        pedidos_medios_mes_1=('num_pedidos_mes_1', 'mean'),
        pedidos_medios_mes_12=('num_pedidos_mes_12', 'mean'),
        ticket_medio_mes_1=('total_amount_mes_1', 'mean'),
        ticket_medio_mes_12=('total_amount_mes_12', 'mean')
    )
    .reset_index()
)




# In[25]:


tabela_segmento.to_csv('../Resultados/tabela_segmento.csv', index=False)


# Marcar segmentacao - baseada no amount

# In[28]:


conditions = [
    (df['segmento_mobilidade'] == 'Alta Mobilidade') & (df['direcao'] == 'Upgrade'),
    (df['segmento_mobilidade'] == 'Estável') & (df['direcao'] == 'Estável'),
    (df['segmento_mobilidade'] == 'Mobilidade Moderada') & (df['direcao'] == 'Upgrade')
]

choices = [1, 2, 3]

df['categoria_segmento'] = np.select(conditions, choices, default=None)


resumo_categorias = (
    df
    .groupby(['categoria_segmento', 'is_target'])['customer_id']
    .nunique()
    .reset_index(name='num_clientes')
    .sort_values(['categoria_segmento', 'is_target'])
)


# In[29]:


df11=df[df['categoria_segmento']==1]
df12=df[df['categoria_segmento']==2]
df13=df[df['categoria_segmento']==3]


# In[30]:


df_1,clientes_retidos_1=retidos11(df11, mes0=12, mes1=1,pedidos=1)
print(clientes_retidos_1)
df_2,clientes_retidos_2=retidos11(df12, mes0=12, mes1=1,pedidos=1)
print(clientes_retidos_1)
df_3,clientes_retidos_3=retidos11(df13, mes0=12, mes1=1,pedidos=1)
print(clientes_retidos_3)


# In[31]:


resultados, agg = calcula_viabilidade_wide(df_1,
                                           mes_campanha=12,
                                           mes_seguinte=1,
                                           coupon_value=10.0,
                                           margin_rate=0.12)

print(resultados)
print(agg)


# In[32]:


resultados, agg = calcula_viabilidade_wide(df_2,
                                           mes_campanha=12,
                                           mes_seguinte=1,
                                           coupon_value=10.0,
                                           margin_rate=0.12)

print(resultados)
print(agg)


# In[33]:


resultados, agg = calcula_viabilidade_wide(df_3,
                                           mes_campanha=12,
                                           mes_seguinte=1,
                                           coupon_value=10.0,
                                           margin_rate=0.12)

print(resultados)
print(agg)

