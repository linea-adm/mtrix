import logging
import time
import os
import zipfile
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from requests.exceptions import RequestException

# Configuração do banco de dados
engine = create_engine("mysql+pymysql://user:sellout2k24@db:3306/sellout_db?connect_timeout=180")

# URLs e credenciais da API
auth_url = 'https://drive.mtrix.com.br/auth'
base_url = 'https://drive.mtrix.com.br/2157'
download_url = 'https://drive.mtrix.com.br/download'
auth_payload = {
    'accessKey': 'API_TOKEN_LINEA-app',
    'secretKey': '5iylIEAsXhCnQMZIbnHsxiuJ9wje2Y1a'
}

def get_access_key():
    """
    Obtém o token de acesso para autenticação na API.
    
    :return: Access token.
    """
    try:
        response = requests.post(auth_url, data=auth_payload)
        response.raise_for_status()
        token = response.text.strip()
        logging.info("Access key obtida com sucesso.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao obter access key: {e}")
        raise

    

def get_latest_file_name(data_type, period):
    """
    Obtém o nome do arquivo mais recente para um tipo de dado específico.
    
    :param data_type: Tipo de dado (sellout, stock, sales_force, etc.)
    :param period: Período específico no formato YYYYMM (opcional).
    :return: Nome do arquivo mais recente.
    """
    token = get_access_key()
    headers = {"Authorization": f"Bearer {token}"}
    list_url = f"{base_url}/{data_type}/{period}"
    logging.info(f"Url da api de lista de arquivos: {list_url}")
    try:
        response = requests.get(list_url, headers=headers)
        response.raise_for_status()
        files = response.json()
        if not files:
            raise ValueError("Nenhum arquivo encontrado.")

        # Para o tipo customer, retorna o caminho específico conforme a documentação
        if data_type == 'customer':
            return "s3a://drive-linea/2157/DEFAULT_FILES/CUSTOMER/customer.parquet"
            
        latest_file = max(files, key=lambda x: x['dt_register'])
        logging.info(f"Último arquivo {data_type} obtido: {latest_file['file_path']}")
        return latest_file['file_path']
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao listar arquivos: {e}")
        raise
    except ValueError as e:
        logging.error(f"Erro ao processar a resposta da API de listagem: {e}")
        raise


def get_manual_file_path(data_type, period):
    """
    Constrói o caminho do arquivo para um tipo de dado específico e período manualmente informado.
    
    :param data_type: Tipo de dado (sellout, stock, sfd, etc.)
    :param period: Período específico no formato YYYYMM.
    :return: Caminho do arquivo.
    """
    # Extrair apenas os primeiros 6 dígitos do período
    period_str = str(period)
    year = period_str[:4]
    month = period_str[4:6]
    
    if data_type == 'sfd':
        file_path = f"s3a://drive-linea/2157/DEFAULT_FILES/SFD/{year}/{month}/sfd_{year}{month}.parquet"
    elif data_type == 'stock':
        file_path = f"s3a://drive-linea/2157/DEFAULT_FILES/STOCK/{year}/{month}/stock_{year}{month}.parquet"
    elif data_type == 'sellout':
        file_path = f"s3a://drive-linea/2157/DEFAULT_FILES/SELLOUT/{year}/{month}/sellout_{year}{month}.parquet"
    elif data_type == 'customer':
        file_path = f"s3a://drive-linea/2157/DEFAULT_FILES/CUSTOMER/customer.parquet"
    else:
        return get_latest_file_name(data_type, period) #raise ValueError(f"Tipo de dado {data_type} não é suportado.")
    
    logging.info(f"Caminho do arquivo {data_type} construído: {file_path}")
    return file_path



def download_and_extract_file(file_path, max_retries=5, base_delay=5):
    """
    Baixa e descomprime o arquivo especificado.
    
    :param file_path: Caminho do arquivo a ser baixado.
    :param max_retries: Número máximo de tentativas de download.
    :param base_delay: Tempo de espera inicial entre tentativas.
    :return: Caminho do arquivo Parquet extraído.
    """
    token = get_access_key()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fileName": file_path}

    attempt = 0
    while attempt < max_retries:
        try:
            # Baixa o arquivo .zip
            logging.info(f"Tentativa de download {attempt + 1} para o arquivo: {file_path}")
            response = requests.post(download_url, headers=headers, json=payload, timeout=1200)  # Aumentando o timeout para 1200 segundos
            response.raise_for_status()
            
            zip_file_path = os.path.basename(file_path) + ".zip"
            with open(zip_file_path, "wb") as file:
                file.write(response.content)
            logging.info(f"Arquivo {file_path} baixado com sucesso.")

            # Extrai o conteúdo do .zip
            extract_path = os.path.join("extracted_files", os.path.basename(file_path))
            if not os.path.exists(extract_path):
                os.makedirs(extract_path)

            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            logging.info(f"Arquivo {file_path} descomprimido com sucesso.")

            # Encontra o arquivo .parquet extraído
            parquet_files = [f for f in os.listdir(extract_path) if f.endswith('.parquet')]
            if not parquet_files:
                raise FileNotFoundError("Nenhum arquivo Parquet encontrado na pasta extraída.")
            
            parquet_file_path = os.path.join(extract_path, parquet_files[0])
            return parquet_file_path, zip_file_path

        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.error(f"Erro ao baixar o arquivo (tentativa {attempt}): {e}")
            if attempt >= max_retries:
                logging.error(f"Todas as {max_retries} tentativas de download falharam.")
                raise
            wait_time = base_delay * (2 ** (attempt - 1))  # Backoff exponencial
            logging.info(f"Aguardando {wait_time} segundos antes da próxima tentativa...")
            time.sleep(wait_time)
        except zipfile.BadZipFile as e:
            logging.error(f"Erro ao descomprimir o arquivo: {e}")
            raise

def download_and_extract_file2(file_path):
    """
    Baixa e descomprime o arquivo especificado.
    
    :param file_path: Caminho do arquivo a ser baixado.
    :return: Caminho do arquivo Parquet extraído.
    """
    token = get_access_key()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"fileName": file_path}
    try:

        # Baixa o arquivo .zip
        response = requests.post(download_url, headers=headers, json=payload, timeout=600)
        response.raise_for_status()
        zip_file_path = os.path.basename(file_path) + ".zip"
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Arquivo {file_path} baixado com sucesso.")

        # Extrai o conteúdo do .zip
        extract_path = os.path.join("extracted_files", os.path.basename(file_path))
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        logging.info(f"Arquivo {file_path} descomprimido com sucesso.")


        # Encontra o arquivo .parquet extraído
        parquet_files = [f for f in os.listdir(extract_path) if f.endswith('.parquet')]
        if not parquet_files:
            raise FileNotFoundError("Nenhum arquivo Parquet encontrado na pasta extraída.")
        
        parquet_file_path = os.path.join(extract_path, parquet_files[0])
        return parquet_file_path, zip_file_path

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao baixar o arquivo: {e}")
        raise
    except zipfile.BadZipFile as e:
        logging.error(f"Erro ao descomprimir o arquivo: {e}")
        raise

def remove_files(parquet_file_path, zip_file_path):
    """
    Remove os arquivos temporários após o processamento.
    
    :param parquet_file_path: Caminho do arquivo Parquet extraído.
    :param zip_file_path: Caminho do arquivo Zip baixado.
    """
    try:
        os.remove(parquet_file_path)
        logging.info(f"Arquivo Parquet {parquet_file_path} removido com sucesso.")
        os.remove(zip_file_path)
        logging.info(f"Arquivo Zip {zip_file_path} removido com sucesso.")
    except OSError as e:
        logging.error(f"Erro ao remover arquivos: {e}")
        raise
def extract_data_from_parquet(parquet_file_path):
    """
    Extrai dados de um arquivo Parquet e retorna um DataFrame.
    
    :param parquet_file_path: Caminho do arquivo Parquet.
    :return: DataFrame com os dados extraídos.
    """
    try:
        df = pd.read_parquet(parquet_file_path)
        logging.info(f"Dados extraídos do arquivo Parquet: {parquet_file_path}")
        return df
    except ValueError as e:
        logging.error(f"Erro ao ler arquivo Parquet: {e}")
        raise

def clear_and_insert_data(table_name, df, ano, mes):
    """
    Limpa dados antigos e insere novos dados na tabela especificada.
    
    :param table_name: Nome da tabela no banco de dados.
    :param df: DataFrame com os dados a serem inseridos.
    :param ano: Ano para o qual os dados serão deletados.
    :param mes: Mês para o qual os dados serão deletados.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        
        # Excluir registros existentes para o mês e ano especificados
        delete_query = f"DELETE FROM {table_name} WHERE DATE_FORMAT(SELLOUT_DATE, '%%Y-%%m') = %s"
        cursor.execute(delete_query, (f"{ano}-{str(mes).zfill(2)}",))
        logging.info(f"Dados antigos da tabela {table_name} deletados com sucesso.")
        
        # Inserir novos dados do DataFrame
        insert_query = f"""
            INSERT INTO {table_name} (
                DISTRIBUTOR_CODE, SELLOUT_DATE, CUSTOMER_ID, SELLOUT_TYPE, INVOICE_ID, PRODUCT_CODE,
                SALESREP_ID, QTY_UNIT, QTY_CONV1, QTY_CONV2, QTY_CONV3, QTY_CONV4, QTY_CONV5,
                QTY_CONV6, QTY_CONV7, QTY_CUSTOM1, QTY_CUSTOM2, SELLOUT_VALUE_LC, SELLOUT_CONV1
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela {table_name}: {e}")
        raise

def clear_and_insert_distributors(df):
    """
    Limpa dados antigos e insere novos dados na tabela de distribuidores.
    
    :param df: DataFrame com os dados a serem inseridos.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        
        delete_query = "DELETE FROM distribuidores WHERE DISTRIBUTOR_CODE = %s"
        for distributor_code in df['DISTRIBUTOR_CODE'].unique():
            cursor.execute(delete_query, (distributor_code,))
        logging.info("Dados antigos da tabela distribuidores deletados com sucesso.")
        
        insert_query = """
            INSERT INTO distribuidores (
                DISTRIBUTOR_CODE, DISTRIBUTOR_ID, DISTRIBUTOR_NAME, DISTRIBUTOR_GROUP_NAME, DISTRIBUTOR_FLAG, DISTRIBUTOR_CHANNEL, SF_LEVEL1, SF_LEVEL2, SF_LEVEL3, SF_LEVEL4, SF_LEVEL5
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela distribuidores com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela distribuidores: {e}")
        raise

def clear_and_insert_products(df):
    """
    Limpa dados antigos e insere novos dados na tabela de produtos.
    
    :param df: DataFrame com os dados a serem inseridos.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        
        delete_query = "DELETE FROM produtos WHERE PRODUCT_CODE = %s"
        for product_code in df['PRODUCT_CODE'].unique():
            cursor.execute(delete_query, f'{product_code}')
        logging.info("Dados antigos da tabela produtos deletados com sucesso.")
        
        insert_query = f"""
            INSERT INTO produtos (
                PRODUCT_CODE, PRODUCT_EAN_DUN_ID, PRODUCT_SKU_CODE, PRODUCT_NAME
            ) VALUES (%s, %s, %s, %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela produtos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela produtos: {e}")
        raise

def clear_and_insert_stock(df, ano, mes):
    """
    Limpa dados antigos e insere novos dados na tabela de estoque.
    
    :param df: DataFrame com os dados a serem inseridos.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        
        delete_query = f"DELETE FROM estoque WHERE DATE_FORMAT(STOCK_DATE, '%%Y-%%m') = %s"
        cursor.execute(delete_query, (f"{ano}-{str(mes).zfill(2)}",))

        logging.info("Dados antigos da tabela estoque deletados com sucesso.")
        
        insert_query = """
            INSERT IGNORE INTO estoque (
                DISTRIBUTOR_CODE, PRODUCT_CODE, STOCK_DATE, QTY_UNIT, QTY_CONV1, QTY_CONV2, QTY_CONV3, QTY_CONV4, QTY_CONV5, QTY_CONV6, QTY_CONV7, QTY_CUSTOM1, QTY_CUSTOM2
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela estoque com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela estoque: {e}")
        raise

def clear_and_insert_sales_force(df, ano, mes):
    """
    Limpa dados antigos e insere novos dados na tabela de força de vendas.
    
    :param df: DataFrame com os dados a serem inseridos.
    :param ano: Ano para o qual os dados serão deletados.
    :param mes: Mês para o qual os dados serão deletados.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Determina o período a ser excluído
        period = f"{ano}{str(mes).zfill(2)}"

        # Determina o tamanho do lote para exclusão
        batch_size = 1000

        # Executa a exclusão em lotes
        total_deleted = 0
        while True:
            delete_query = """
                DELETE FROM forca_vendas 
                WHERE SF_YEAR_MONTH = %s
                LIMIT %s
            """
            cursor.execute(delete_query, (period, batch_size))
            connection.commit()
            rows_deleted = cursor.rowcount
            total_deleted += rows_deleted            
            logging.info(f"Deletados {rows_deleted} registros da tabela forca_vendas.")
            if rows_deleted < batch_size:
                break

        logging.info(f"Total de {total_deleted} registros deletados da tabela forca_vendas.")

        # Insere os novos dados
        insert_query = """
            INSERT INTO forca_vendas (
                DISTRIBUTOR_CODE, SF_YEAR_MONTH, SALESREP_ID, SF_LEVEL2_ID, SF_LEVEL1_ID
            ) VALUES (%s, %s, %s, %s, %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, (row.DISTRIBUTOR_CODE, row.SF_YEAR_MONTH, row.SALESREP_ID, row.SF_LEVEL2_ID, row.SF_LEVEL1_ID))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela forca_vendas com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela forca_vendas: {e}")
        raise

def clear_and_insert_customers(df):
    """
    Limpa dados antigos e insere novos dados na tabela de clientes.
    
    :param df: DataFrame com os dados a serem inseridos.
    """
    try:
        connection = engine.raw_connection()
        cursor = connection.cursor()
        
        # Deletar registros antigos
        delete_query = "DELETE FROM clientes WHERE CUSTOMER_ID = %s"
        for customer_id in df['CUSTOMER_ID'].unique():
            cursor.execute(delete_query, (customer_id,))
        logging.info("Dados antigos da tabela clientes deletados com sucesso.")
        
        # Inserir novos dados
        insert_query = """
            INSERT INTO clientes (
                DISTRIBUTOR_CODE, CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_ADDRESS, CUSTOMER_NEIGHBORHOOD,
                CUSTOMER_CITY, CUSTOMER_UF, CUSTOMER_ZIPCODE, CUSTOMER_SEGMENTATION, CUSTOMER_FLAG
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, tuple(row))
        
        connection.commit()
        cursor.close()
        connection.close()
        logging.info("Novos dados inseridos na tabela clientes com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao limpar e inserir dados na tabela clientes: {e}")
        raise
