import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def boxplot_meses(df, var_cat,var_cont):
    unique_domain=df[var_cat].unique()
    for i in unique_domain:
        control = df[(df[var_cat]==i) & (df['is_target']=='control')][var_cont]
        target = df[(df[var_cat]==i) & (df['is_target']=='target')][var_cont]
        
        plt.figure(figsize=(10, 2))
        plt.boxplot([control, target], 
                    vert=False, 
                    labels=['Control', 'Target'],
                    flierprops={'markerfacecolor': 'red', 'marker': 'o', 'markeredgecolor': 'red'})
        plt.title(f"Distribuição  {var_cont}:{i}")
        plt.xlabel('Valor do pedido')
        plt.show()





def plot_metricas(
    df,
    eixo_x,                    # ex: 'day', 'hour'
    metrics,                   # dict: {'coluna_df': 'Nome da Métrica'}
    highlight_metrics=None,    # lista de colunas a destacar (subset de metrics.keys())
    group_col='is_target',     
    groups=['target', 'control'],
    group_names={'target': 'Target', 'control': 'Control'},
    month_col='order_created_month',
    months=[12, 1],
    month_names={12: 'Dezembro', 1: 'Janeiro'},
    top_n=3
):
    """
    Plota várias métricas ao longo de um eixo (ex: dia/hora), separando por grupos (ex: target/control).
    - metrics: dicionário {coluna: label}
    - highlight_metrics: lista de colunas dentro de metrics que terão picos destacados (pontos + texto).
    """

    if highlight_metrics is None:
        # Se não passar nada, destaca só a primeira métrica
        highlight_metrics = [next(iter(metrics.keys()))]

    for month in months:
        month_data = df[df[month_col] == month]
        if month_data.empty:
            continue

        fig, axes = plt.subplots(1, len(groups), figsize=(16, 6))
        fig.suptitle(
            f'Métricas por {eixo_x} - {month_names.get(month, month)}',
            fontsize=16,
            fontweight='bold'
        )

        if len(groups) == 1:
            axes = [axes]

        for ax, group in zip(axes, groups):
            g_data = (
                month_data[month_data[group_col] == group]
                .sort_values(eixo_x)
            )

            if g_data.empty:
                ax.set_visible(False)
                continue

            x_vals = g_data[eixo_x]

            # Plotar todas as métricas como linhas
            for col, label in metrics.items():
                ax.plot(
                    x_vals,
                    g_data[col],
                    marker='o',
                    linewidth=2,
                    label=label
                )

                # Destacar picos só nas métricas escolhidas
                if col in highlight_metrics:
                    top_points = g_data.nlargest(top_n, col)
                    ax.scatter(top_points[eixo_x], top_points[col], s=120, zorder=5)

                    for _, row in top_points.iterrows():
                        ax.text(
                            row[eixo_x],
                            row[col] * 1.03,
                            f"{row[col]:.1f}",
                            ha='center',
                            va='bottom',
                            fontsize=9,
                            fontweight='bold'
                        )

            # Configuração dos eixos
            ax.set_title(group_names.get(group, group), fontsize=14, fontweight='bold')
            ax.set_xlabel(eixo_x.capitalize(), fontsize=12)
            ax.set_ylabel('Valor', fontsize=12)
            ax.legend(fontsize=10)

            # Ajuste automático do Y
            y_max = g_data[list(metrics.keys())].max().max() * 1.15
            ax.set_ylim(0, y_max)

            # Sem linhas de grade
            # (nada de ax.grid(...))

        plt.tight_layout(rect=[0, 0, 1, 0.9])
        #plt.show()
