import sys
import os
import logging
import importlib
import pandas as pd


sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import utilities.functions as functions
importlib.reload(functions)

from utilities.functions import (
    load_data,
    check_key_uniqueness,
    save_parquet,
    merge_df,
    load_orders,
    process_orders_pandas,
)

def criar_diretorios_dados():
    """
    Cria a estrutura de diretórios para os dados se não existirem
    """
    diretorios = [
        "dados/stage",
        "dados/bronze", 
        "dados/silver",
        "dados/gold"
    ]
    

    
    for diretorio in diretorios:
        if os.path.exists(diretorio):
            print(f"{diretorio} (já existe)")
        else:
            os.makedirs(diretorio, exist_ok=True)
            print(f"{diretorio} (criado)")

def main():
    """
   Load and clean data
    """
    try:
       
        
        # Verifica se os repo existem caso contratio cria
        criar_diretorios_dados()
        
        # Load data
        # Orders
        URL_CONSUMER = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz"
        df_consumer = load_data(URL_CONSUMER)[["customer_id", "active","created_at"]]
    
        # Restaurante
        URL_RESTAURANT = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz"
        df_restaurant = load_data(URL_RESTAURANT)
        
        # A/B
        ab_test_url = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
        df_ab = load_data(ab_test_url)
      
        
        # Salva em bronze
        save_parquet(df_consumer, "bronze", "df_consumer.parquet")
        save_parquet(df_restaurant, "bronze", "df_restaurant.parquet")
        save_parquet(df_ab, "bronze", "df_ab_test.parquet")
        
        # Verifica chabes
        check_key_uniqueness(df_consumer, ["customer_id","active","created_at"])
        check_key_uniqueness(df_restaurant, ["id"])
        check_key_uniqueness(df_ab, ["customer_id","is_target"])
       
        # Remove duplicidade from A/B
        df_ab_np = df_ab[~df_ab["customer_id"].isna()]
       
        # Define publico
        df_publico = merge_df(df_ab_np, df_consumer, ['customer_id'], 'outer')
        df_publico = df_publico.dropna(subset=['active'])
    
        # Salva base de publico em silver
        save_parquet(df_publico, "silver", "df_publico.parquet")
   
        # Load orders - exclusivamente para cleinte no test A?B
        URL_ORDERS = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"
        COLUMNS_TO_DROP = [
            'cpf','customer_name','delivery_address_city','delivery_address_country',
            'delivery_address_district','delivery_address_external_id',
            'delivery_address_latitude','delivery_address_longitude',
            'delivery_address_state','delivery_address_zip_code','items',
            'merchant_latitude','merchant_longitude','merchant_timezone',
            'order_scheduled','order_scheduled_date'
        ]
        
        customer_ids = df_ab["customer_id"].astype(str).unique()
        df_orders = load_orders(
            url=URL_ORDERS,
            customer_ids=customer_ids,
            columns_to_drop=COLUMNS_TO_DROP
        )
        
        # Duplicidade base de ordens
        print(check_key_uniqueness(df_orders, ["customer_id","merchant_id","order_id"]))
        print(check_key_uniqueness(df_orders, ["customer_id","merchant_id","order_id","order_created_at"]))
        
        # Publico + Ordens
        df_publico_orders = merge_df(df_publico, df_orders, ['customer_id'], 'inner')
        
        df_publico=process_orders_pandas(df_publico_orders)
        # Cria chave unica 
        save_parquet(df_publico, "silver", "df_publico_com_orders.parquet")
  
        
        # Cliente unico
        df_pub_un = df_publico[["customer_id",'origin_platform',"is_target", "order_created_month", 
                               "num_pedidos_mes", "num_pedidos_hist",'total_amount_mes','ticket_medio']].drop_duplicates().reset_index(drop=True)
     
        # Salva ambas bases de publico
        save_parquet(df_pub_un, "gold", "df_pub_un.parquet")
     
        files_to_check = [
            "dados/bronze/df_consumer.parquet",
            "dados/bronze/df_restaurant.parquet", 
            "dados/bronze/df_ab_test.parquet",
            "dados/silver/df_publico.parquet",
            "dados/silver/df_publico_com_orders.parquet",
            "dados/gold/df_pub_un.parquet"
        ]
        
        for file_path in files_to_check:
            if os.path.exists(file_path):
                print(f"Salvo em {file_path}")
            else:
                print(f"Nao salvou {file_path}")
        
    except Exception as e:
        print(f": {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()