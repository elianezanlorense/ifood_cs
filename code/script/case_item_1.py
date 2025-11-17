#!/usr/bin/env python
# coding: utf-8

# In[4]:
import sys
import os

# CORRIGIR A IMPORTAÇÃO - adicionar path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import importlib
import utilities.functions as functions
import pandas as pd
importlib.reload(functions)
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import json



from utilities.functions import (
    retidos,
    calcula_viabilidade,
    analisar_retencao,


)


# In[5]:


publico_janeiro_dezembro = pd.read_parquet("dados/gold/publico_janeiro_dezembro.parquet")


# In[6]:


publico_janeiro_dezembro.head(10)


# Calculando retencao considerando dois ou + pedidos

# In[9]:


df_1,clientes_retidos_1=retidos(publico_janeiro_dezembro, mes0=12, mes1=1,pedidos=1)
print(clientes_retidos_1)


# In[10]:


retencao_pedido_1=analisar_retencao(df_1)
retencao_pedido_1


# Calculando retencao considerando tres ou + pedidos

# In[11]:


df_2,clientes_retidos_2=retidos(publico_janeiro_dezembro, mes0=12, mes1=1,pedidos=2)
print(clientes_retidos_2)


# In[12]:


retencao_pedido_2=analisar_retencao(df_2)
retencao_pedido_2


# Viabilidade 

# In[ ]:


resultados, agg = calcula_viabilidade(
    publico_janeiro_dezembro,
    mes_campanha=12,
    mes_seguinte=1,
    coupon_value=10.0,   
    margin_rate=0.12     
)

resultados


# Calculando retencao separadamente para outliers

# In[13]:


orderns_de=publico_janeiro_dezembro[publico_janeiro_dezembro['order_created_month']==12]


# In[14]:


distance = 1.5 * (np.nanpercentile(orderns_de['total_amount_mes'], 75) - np.nanpercentile(orderns_de['total_amount_mes'], 25))
lim_sup=distance + np.nanpercentile(orderns_de['total_amount_mes'], 75)


# In[15]:


red_square = dict(markerfacecolor='r', markeredgecolor='r', marker='.')
orderns_de['total_amount_mes'].plot(kind='box', xlim=(0, 500), vert=False, flierprops=red_square, figsize=(16,2))


# In[19]:


orderns_de['total_amount_mes'].describe().round(2)


# In[20]:


pb_drop_dj = publico_janeiro_dezembro.drop(
    publico_janeiro_dezembro[
        (publico_janeiro_dezembro['total_amount_mes'] == 0) |
        (publico_janeiro_dezembro['total_amount_mes'] > lim_sup)
    ].index,
    axis=0
)


# In[21]:


pb_drop_dj


# In[25]:


plt.figure(figsize=(12, 6))

sns.boxplot(
    data=pb_drop_dj,
    x='order_created_month',               
    y='total_amount_mes',   
    hue='is_target',           
    showfliers=False          
)

plt.title('Distribuição do valor gasto por mês e target')
plt.xlabel('Mês')
plt.ylabel('Total Gasto no Mês')
plt.legend(title='Grupo')
plt.tight_layout()
plt.show()


# In[26]:


pb_drop_dj_1,clientes_retidos_1=retidos(pb_drop_dj, mes0=12, mes1=1,pedidos=1)
print(clientes_retidos_1)


# In[27]:


pb_drop_dj_s=analisar_retencao(pb_drop_dj_1)
pb_drop_dj_s


# In[29]:


resultados_out, agg_out = calcula_viabilidade(
    pb_drop_dj,
    mes_campanha=12,
    mes_seguinte=1,
    coupon_value=10.0,   
    margin_rate=0.12     
)

resultados_out

