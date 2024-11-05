import logging.config
from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta
from utils import (
    get_latest_file_name,
    get_manual_file_path,
    download_and_extract_file,
    remove_files,
    extract_data_from_parquet,
    clear_and_insert_data,
    clear_and_insert_distributors,
    clear_and_insert_products,
    clear_and_insert_stock,
    clear_and_insert_sales_force,
    clear_and_insert_customers
)

# Configuração de logging
logging.config.fileConfig('/app/logging.conf')

app = FastAPI()

# Mapeamento de tipos de dados para nomes de tabelas
def process_data(data_type, period=None, manual_trigger=False):
    """
    Processa os dados para um tipo específico.
    
    :param data_type: Tipo de dado a ser processado (sellout, distributors, products, stock, sfd, customer)
    :param period: Período específico para o processamento (aplicável a certos tipos de dados)
    :param manual_trigger: Indica se o trigger é manual    
    """
    try:
        table_name_map = {
            'sellout': 'sellout',
            'distributors': 'distribuidores',
            'products': 'produtos',
            'stock': 'estoque',
            'sfd': 'forca_vendas',
            'customer': 'clientes' 
        }
        if data_type not in table_name_map:
            logging.info(f"Mapa: {table_name_map} ");
            raise ValueError(f"Tipo de dado {data_type} não é válido.")

        table_name = table_name_map[data_type]        
        if manual_trigger:
            file_path = get_manual_file_path(data_type, period)
        else:
            file_path = get_latest_file_name(data_type, period)
        # parquet_file_path = download_and_extract_file(file_path)
        parquet_file_path, zip_file_path = download_and_extract_file(file_path)
        df = extract_data_from_parquet(parquet_file_path)

        if data_type == 'sellout':
            ano = period[:4]
            mes = period[4:6]
            clear_and_insert_data(table_name, df, ano, mes)
        elif data_type == 'distributors':
            clear_and_insert_distributors(df)
        elif data_type == 'products':
            clear_and_insert_products(df)
        elif data_type == 'stock':
            ano = period[:4]
            mes = period[4:6]
            clear_and_insert_stock(df, ano, mes)
        elif data_type == 'sfd':
            ano = period[:4]
            mes = period[4:6]
            clear_and_insert_sales_force(df, ano, mes)
        elif data_type == 'customer':
            clear_and_insert_customers(df)
        
        # Remover os arquivos após o processamento
        remove_files(parquet_file_path, zip_file_path)
        logging.info(f"Processamento de {data_type} concluído com sucesso.")

    except Exception as e:
        logging.error(f"Erro no processamento de {data_type}: {e}")
        raise

def main_process(period=None):
    """
    Processa todos os tipos de dados.
    """
    try:
        data_types = ['sellout', 'distributors', 'products', 'stock', 'sfd', 'customer']
        for data_type in data_types:
            process_data(data_type, period, manual_trigger=True)
    except Exception as e:
        logging.error(f"Erro no processo principal: {e}")

@app.post("/trigger-manual")
def trigger_manual(period: str = None):
    """
    Endpoint para disparar o processamento manual de todos os tipos de dados.
    
    :param period: Período específico para o processamento no formato YYYYMM ou YYYYMMDD000000
    :return: Mensagem de sucesso ou erro.
    """
    try:        
        if period is None:
            yesterday = datetime.now() - timedelta(days=1)
            period = yesterday.strftime("%Y%m%d000000")
        elif len(period) == 6:
            period += "01000000"  # Completa o período se for apenas YYYYMM
        elif len(period) != 14:
            raise ValueError("Período deve estar no formato YYYYMM ou YYYYMMDD000000")

        main_process(period, manual_trigger=True)
        return {"message": "Processamento realizado com sucesso."}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Erro ao iniciar processo manual")

@app.post("/trigger/{data_type}")
def trigger_specific(data_type: str, period: str = None):
    """
    Endpoint para disparar o processamento manual de um tipo específico de dado.
    
    :param data_type: Tipo de dado a ser processado (sellout, distributors, products, stock, sfd, customer)
    :param period: Período específico para o processamento no formato YYYYMM ou YYYYMMDD000000
    :return: Mensagem de sucesso ou erro.
    """
    try:
        if period is None:
            yesterday = datetime.now() - timedelta(days=1)
            period = yesterday.strftime("%Y%m%d000000")
        elif len(period) == 6:
            period += "01000000"  # Completa o período se for apenas YYYYMM
        process_data(data_type, period, manual_trigger=True)
        return {"message": f"Processamento de {data_type} realizado com sucesso."}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao iniciar processamento de {data_type}")



if __name__ == "__main__":
    yesterday = datetime.now() - timedelta(days=1)
    period = yesterday.strftime("%Y%m%d000000") 
    main_process(period)
