import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def boxplot_meses(df, meses):
    for mes in meses:
        control = df[(df['order_created_month']==mes) & (df['is_target']=='control')]['order_total_amount']
        target = df[(df['order_created_month']==mes) & (df['is_target']=='target')]['order_total_amount']
        
        plt.figure(figsize=(10, 2))
        plt.boxplot([control, target], 
                    vert=False, 
                    labels=['Control', 'Target'],
                    flierprops={'markerfacecolor': 'red', 'marker': 'o', 'markeredgecolor': 'red'})
        plt.title(f"Distribuição  order total amount por mês e grupo - mês {mes}")
        plt.xlabel('Valor do pedido')
        plt.show()