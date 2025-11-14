import pandas as pd
import requests
import gzip
import json
import io
from datetime import date
from typing import List, Dict, Set, Optional
from pathlib import Path  # Importado para suportar Path

def execute_orders_etl(
    url: str, 
    customer_ids: Set[str], 
    columns_to_drop: Set[str], 
    max_sample: int,
) -> Optional[pd.DataFrame]:
    """
    Executa um processo ETL (Extração, Transformação) para um arquivo JSON.GZ
    de ordens, aplicando filtragem por Customer ID e amostragem.

    Args:
        url: URL do arquivo JSON.GZ das ordens.
        customer_ids: Conjunto de Customer IDs para filtrar.
        columns_to_drop: Conjunto de colunas a serem removidas na transformação.
        max_sample: Número máximo de registros a serem coletados (amostra).

    Returns:
        O DataFrame Pandas processado ou None em caso de falha.
    """
    print(f"✅ IDs de Clientes para filtro: {len(customer_ids):,}")
    print(f"⬇️ Streaming e filtrando ordens (amostra máx. {max_sample:,})...")
    
    filtered_records: List[Dict] = []
    
    # ETAPA 1: EXTRAÇÃO (Streaming do arquivo JSON.GZ)
    try:
        # Abre a URL e trata a compactação GZ para streaming de texto
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
        gz = gzip.GzipFile(fileobj=io.BytesIO(resp.content))
        text_stream = io.TextIOWrapper(gz, encoding="utf-8")

        # ETAPA 2: TRANSFORMAÇÃO (Filtragem e Limpeza)
        for i, line in enumerate(text_stream, start=1):
            
            # Condição de parada (Amostragem)
            if len(filtered_records) >= max_sample:
                print(f"✅ Amostra de {max_sample:,} registros atingida. Interrompendo leitura.")
                break
            
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            customer_id = record.get("customer_id")

            # FILTRAGEM: Apenas clientes de interesse
            if customer_id in customer_ids:
                # TRANSFORMAÇÃO: Remoção de colunas
                for col in columns_to_drop:
                    record.pop(col, None)
                
                filtered_records.append(record)
        
    except Exception as e:
        print(f"❌ ERRO FATAL: Falha na extração ou streaming: {e}")
        return None

    # Verifica se há dados para continuar
    if not filtered_records:
        print("\nProcesso concluído, mas nenhuma ordem correspondente encontrada.")
        return None

    print(f"\n✅ Coletado {len(filtered_records):,} ordens filtradas.")
    
    # ETAPA 3: TRANSFORMAÇÃO FINAL (Pandas)
    df_result = pd.DataFrame(filtered_records)
    # Adiciona a data de ingestão
    df_result['ingestion_date'] = date.today().isoformat()
    
    return df_result


def save_parquet(df: pd.DataFrame, folder_name: str, filename: str) -> Path:
    """
    Salva um DataFrame no formato Parquet, garantindo que o diretório exista.
    """
    folder = Path(folder_name)
    folder.mkdir(parents=True, exist_ok=True)

    path = folder / filename
    df.to_parquet(path, index=False)

    print(f"Arquivo Parquet salvo em: {path.resolve()}")
    return path


# ---------------------------------------------------
# EXEMPLO DE USO
# ---------------------------------------------------

if __name__ == "__main__":
    # 📌 CONFIGURAÇÃO DE ENTRADA
    URL_ORDENS = "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz"
    MAX_REGISTROS = 50000
    FOLDER_SAIDA = "03_output/orders_data" # Pasta de destino
    FILE_PARQUET_NAME = "orders_sample.parquet"
    
    # Simulação dos IDs de clientes 'ativos'
    IDS_ATIVOS: Set[str] = {
        "cus_e9f50f1469", "cus_887e2f5d94", "cus_61e1b203c9", 
        "cus_9c34f63c87", "cus_632a934fe3"
    }
    
    COLUNAS_PARA_REMOVER: Set[str] = {
        'cpf', 'customer_name', 'delivery_address_city', 'delivery_address_country',
        'delivery_address_district', 'delivery_address_external_id',
        'delivery_address_latitude', 'delivery_address_longitude',
        'delivery_address_state', 'delivery_address_zip_code', 'items',
        'merchant_latitude', 'merchant_longitude', 'merchant_timezone',
        'order_scheduled', 'order_scheduled_date', 'origin_platform'
    }

    # EXECUTA A FUNÇÃO ETL (E & T)
    df_final = execute_orders_etl(
        url=URL_ORDENS,
        customer_ids=IDS_ATIVOS,
        columns_to_drop=COLUNAS_PARA_REMOVER,
        max_sample=MAX_REGISTROS,
    )

    # ETAPA DE CARGA (L) usando a função save_parquet
    if df_final is not None:
        save_parquet(df_final, FOLDER_SAIDA, FILE_PARQUET_NAME)
        
        print("\n--- VISUALIZAÇÃO DO DATAFRAME RETORNADO ---")
        print(df_final.info())
        print(df_final.head(3))