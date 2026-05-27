"""
sellout_pipeline.py

Pipeline de ingestão Drive Mtrix (Linea) -> MySQL (sellout_db).

Refatorações principais:
- Consolidação das duas funções de download em uma única, com retry exponencial.
- Fallback automático de gs:// para s3a:// quando a API responde 504.
- Cache do access token para evitar reautenticação a cada chamada.
- Função genérica clear_and_insert para reduzir duplicação.
"""

import logging
import os
import time
import zipfile
from typing import Optional, Tuple, Dict, Any

import pandas as pd
import requests
from requests.exceptions import HTTPError, RequestException
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# Banco de dados
engine = create_engine(
    "mysql+pymysql://user:sellout2k24@db:3306/sellout_db?connect_timeout=180"
)

# Endpoints da API Drive Mtrix
AUTH_URL = "https://drive.mtrix.com.br/auth"
BASE_URL = "https://drive.mtrix.com.br/2157"
DOWNLOAD_URL = "https://drive.mtrix.com.br/download"

AUTH_PAYLOAD = {
    "accessKey": "API_TOKEN_LINEA-app",
    "secretKey": "5iylIEAsXhCnQMZIbnHsxiuJ9wje2Y1a",
}

# Prefixos de storage — usamos S3A como fallback quando GS retorna 504
BASE_GS = "gs://mtx-drive-linea-prd/2157/DEFAULT_FILES"
BASE_S3A = "s3a://drive-linea/2157/DEFAULT_FILES"

# Mapa de subpastas/prefixos por tipo de dado para construção manual do path
DATA_TYPE_FOLDERS: Dict[str, str] = {
    "sellout": "SELLOUT",
    "stock": "STOCK",
    "sfd": "SFD",
    "customer": "CUSTOMER",
}

# Parâmetros de retry / timeout
DEFAULT_MAX_RETRIES = 5
DEFAULT_BASE_DELAY = 5
DEFAULT_TIMEOUT = 1200

# Cache simples de token em memória
_token_cache: Dict[str, Any] = {"value": None, "ts": 0}
_TOKEN_TTL = 60 * 30  # 30 min


# ---------------------------------------------------------------------------
# Autenticação
# ---------------------------------------------------------------------------

def get_access_key(force_refresh: bool = False) -> str:
    """Obtém o token de acesso, com cache em memória de 30 min."""
    now = time.time()
    if (
        not force_refresh
        and _token_cache["value"]
        and now - _token_cache["ts"] < _TOKEN_TTL
    ):
        return _token_cache["value"]

    try:
        response = requests.post(AUTH_URL, data=AUTH_PAYLOAD, timeout=60)
        response.raise_for_status()
        token = response.text.strip()
        _token_cache.update({"value": token, "ts": now})
        logger.info("Access key obtida com sucesso.")
        return token
    except RequestException as e:
        logger.error(f"Erro ao obter access key: {e}")
        raise


def _auth_headers(json_body: bool = False) -> Dict[str, str]:
    headers = {"Authorization": f"Bearer {get_access_key()}"}
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers


# ---------------------------------------------------------------------------
# Resolução de caminho do arquivo
# ---------------------------------------------------------------------------

def _swap_to_s3a(file_path: str) -> str:
    """Converte um path gs:// para o equivalente s3a:// usado como fallback."""
    if file_path.startswith(BASE_GS):
        return file_path.replace(BASE_GS, BASE_S3A, 1)
    # Se já vier com outro prefixo, retorna inalterado
    return file_path


def get_latest_file_name(data_type: str, period: str) -> str:
    """Obtém o nome do arquivo mais recente para um tipo de dado/período."""
    list_url = f"{BASE_URL}/{data_type}/{period}"
    logger.info(f"Listando arquivos em: {list_url}")

    try:
        response = requests.get(list_url, headers=_auth_headers(), timeout=120)
        response.raise_for_status()
        files = response.json()

        if not files:
            raise ValueError("Nenhum arquivo encontrado.")

        if data_type == "customer":
            return f"{BASE_GS}/CUSTOMER/customer.parquet"

        latest = max(files, key=lambda x: x["dt_register"])
        logger.info(f"Último arquivo {data_type}: {latest['file_path']}")
        return latest["file_path"]

    except RequestException as e:
        logger.error(f"Erro ao listar arquivos: {e}")
        raise


def get_manual_file_path(data_type: str, period) -> str:
    """Constrói manualmente o path do arquivo no storage."""
    period_str = str(period)
    year, month = period_str[:4], period_str[4:6]

    if data_type == "customer":
        return f"{BASE_GS}/CUSTOMER/customer.parquet"

    folder = DATA_TYPE_FOLDERS.get(data_type)
    if not folder:
        # data_type desconhecido -> cai no listing da API
        return get_latest_file_name(data_type, period)

    file_path = (
        f"{BASE_GS}/{folder}/{year}/{month}/{data_type}_{year}{month}.parquet"
    )
    logger.info(f"Caminho do arquivo {data_type} construído: {file_path}")
    return file_path


# ---------------------------------------------------------------------------
# Download + extração (com fallback gs:// -> s3a:// em caso de 504)
# ---------------------------------------------------------------------------

def _post_download(file_path: str, timeout: int) -> requests.Response:
    """Executa um POST /download para o file_path informado."""
    payload = {"fileName": file_path}
    response = requests.post(
        DOWNLOAD_URL,
        headers=_auth_headers(json_body=True),
        json=payload,
        timeout=timeout,
    )
    response.raise_for_status()
    return response


def _save_and_extract_zip(file_path: str, content: bytes) -> Tuple[str, str]:
    """Salva o conteúdo .zip em disco e extrai o .parquet de dentro."""
    zip_file_path = os.path.basename(file_path) + ".zip"
    with open(zip_file_path, "wb") as fh:
        fh.write(content)
    logger.info(f"Arquivo {file_path} baixado em {zip_file_path}.")

    extract_path = os.path.join("extracted_files", os.path.basename(file_path))
    os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)
    logger.info(f"Arquivo {file_path} descomprimido em {extract_path}.")

    parquet_files = [f for f in os.listdir(extract_path) if f.endswith(".parquet")]
    if not parquet_files:
        raise FileNotFoundError("Nenhum arquivo Parquet encontrado na pasta extraída.")

    parquet_file_path = os.path.join(extract_path, parquet_files[0])
    return parquet_file_path, zip_file_path


def download_and_extract_file(
    file_path: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: int = DEFAULT_BASE_DELAY,
    timeout: int = DEFAULT_TIMEOUT,
) -> Tuple[str, str]:
    """
    Baixa e extrai o arquivo informado.

    Retry com backoff exponencial. Se receber HTTP 504 e o path for gs://,
    tenta automaticamente o equivalente s3a://.
    """
    candidate_paths = [file_path]
    s3a_path = _swap_to_s3a(file_path)
    if s3a_path != file_path:
        candidate_paths.append(s3a_path)

    last_error: Optional[Exception] = None

    for current_path in candidate_paths:
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Download (path={current_path}, tentativa {attempt}/{max_retries})"
                )
                response = _post_download(current_path, timeout=timeout)
                return _save_and_extract_zip(current_path, response.content)

            except HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                last_error = e
                logger.error(f"HTTPError {status} em {current_path}: {e}")

                # 504: aborta retries deste path e tenta o próximo (s3a://)
                if status == 504 and current_path != candidate_paths[-1]:
                    logger.warning(
                        "Recebido 504 — trocando para fallback s3a:// imediatamente."
                    )
                    break

                # Outros HTTPError -> aplica backoff e tenta de novo
                if attempt >= max_retries:
                    break

            except RequestException as e:
                last_error = e
                logger.error(f"Erro de rede em {current_path} (tentativa {attempt}): {e}")
                if attempt >= max_retries:
                    break

            except zipfile.BadZipFile as e:
                logger.error(f"Erro ao descomprimir o arquivo {current_path}: {e}")
                raise

            # Backoff exponencial entre tentativas do mesmo path
            wait_time = base_delay * (2 ** (attempt - 1))
            logger.info(f"Aguardando {wait_time}s antes da próxima tentativa...")
            time.sleep(wait_time)

    logger.error(
        f"Falha definitiva ao baixar {file_path} após tentar gs:// e s3a://."
    )
    raise last_error if last_error else RuntimeError("Falha desconhecida no download.")


def remove_files(parquet_file_path: str, zip_file_path: str) -> None:
    """Remove os arquivos temporários após o processamento."""
    for path in (parquet_file_path, zip_file_path):
        try:
            os.remove(path)
            logger.info(f"Arquivo {path} removido com sucesso.")
        except OSError as e:
            logger.error(f"Erro ao remover {path}: {e}")
            raise


def extract_data_from_parquet(parquet_file_path: str) -> pd.DataFrame:
    """Lê um arquivo Parquet em DataFrame."""
    try:
        df = pd.read_parquet(parquet_file_path)
        logger.info(f"Dados extraídos do Parquet: {parquet_file_path}")
        return df
    except ValueError as e:
        logger.error(f"Erro ao ler Parquet: {e}")
        raise


# ---------------------------------------------------------------------------
# Persistência no MySQL — função genérica + wrappers por tabela
# ---------------------------------------------------------------------------

def _execute_clear_and_insert(
    table_name: str,
    df: pd.DataFrame,
    insert_columns: list,
    delete_sql: str,
    delete_params,
    fillna_empty: bool = True,
) -> None:
    """
    Estrutura comum: abre conexão, executa o DELETE, faz INSERT em massa
    a partir do DataFrame, commita e fecha.

    :param insert_columns: colunas (na ordem) que serão usadas no INSERT.
    :param delete_sql: SQL de DELETE (com placeholders %s).
    :param delete_params: pode ser uma tupla (1 execução) ou lista de tuplas
                          (uma execução por item).
    """
    if fillna_empty:
        df = df.fillna("")

    placeholders = ", ".join(["%s"] * len(insert_columns))
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(insert_columns)}) "
        f"VALUES ({placeholders})"
    )

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()

        # DELETE: pode ser 1 chamada ou várias (lote por chave)
        if isinstance(delete_params, list):
            for params in delete_params:
                cursor.execute(delete_sql, params)
        else:
            cursor.execute(delete_sql, delete_params)
        logger.info(f"Dados antigos de {table_name} deletados.")

        # INSERT em massa via executemany — mais rápido que itertuples + execute
        rows = [tuple(row) for row in df[insert_columns].itertuples(index=False)]
        cursor.executemany(insert_sql, rows)

        connection.commit()
        cursor.close()
        logger.info(f"{len(rows)} registros inseridos em {table_name}.")
    except Exception as e:
        connection.rollback()
        logger.error(f"Erro em clear_and_insert para {table_name}: {e}")
        raise
    finally:
        connection.close()


def clear_and_insert_data(table_name: str, df: pd.DataFrame, ano, mes) -> None:
    """Sellout: delete por SELLOUT_DATE (YYYY-MM)."""
    period = f"{ano}-{str(mes).zfill(2)}"
    _execute_clear_and_insert(
        table_name=table_name,
        df=df,
        insert_columns=[
            "DISTRIBUTOR_CODE", "SELLOUT_DATE", "CUSTOMER_ID", "SELLOUT_TYPE",
            "INVOICE_ID", "PRODUCT_CODE", "SALESREP_ID", "QTY_UNIT",
            "QTY_CONV1", "QTY_CONV2", "QTY_CONV3", "QTY_CONV4", "QTY_CONV5",
            "QTY_CONV6", "QTY_CONV7", "QTY_CUSTOM1", "QTY_CUSTOM2",
            "SELLOUT_VALUE_LC", "SELLOUT_CONV1",
        ],
        delete_sql=(
            f"DELETE FROM {table_name} "
            f"WHERE DATE_FORMAT(SELLOUT_DATE, '%%Y-%%m') = %s"
        ),
        delete_params=(period,),
    )


def clear_and_insert_distributors(df: pd.DataFrame) -> None:
    delete_params = [(code,) for code in df["DISTRIBUTOR_CODE"].unique()]
    _execute_clear_and_insert(
        table_name="distribuidores",
        df=df,
        insert_columns=[
            "DISTRIBUTOR_CODE", "DISTRIBUTOR_ID", "DISTRIBUTOR_NAME",
            "DISTRIBUTOR_GROUP_NAME", "DISTRIBUTOR_FLAG", "DISTRIBUTOR_CHANNEL",
            "SF_LEVEL1", "SF_LEVEL2", "SF_LEVEL3", "SF_LEVEL4", "SF_LEVEL5",
        ],
        delete_sql="DELETE FROM distribuidores WHERE DISTRIBUTOR_CODE = %s",
        delete_params=delete_params,
    )


def clear_and_insert_products(df: pd.DataFrame) -> None:
    delete_params = [(code,) for code in df["PRODUCT_CODE"].unique()]
    _execute_clear_and_insert(
        table_name="produtos",
        df=df,
        insert_columns=[
            "PRODUCT_CODE", "PRODUCT_EAN_DUN_ID", "PRODUCT_SKU_CODE", "PRODUCT_NAME",
        ],
        delete_sql="DELETE FROM produtos WHERE PRODUCT_CODE = %s",
        delete_params=delete_params,
        fillna_empty=False,
    )


def clear_and_insert_stock(df: pd.DataFrame, ano, mes) -> None:
    period = f"{ano}-{str(mes).zfill(2)}"
    _execute_clear_and_insert(
        table_name="estoque",
        df=df,
        insert_columns=[
            "DISTRIBUTOR_CODE", "PRODUCT_CODE", "STOCK_DATE", "QTY_UNIT",
            "QTY_CONV1", "QTY_CONV2", "QTY_CONV3", "QTY_CONV4", "QTY_CONV5",
            "QTY_CONV6", "QTY_CONV7", "QTY_CUSTOM1", "QTY_CUSTOM2",
        ],
        delete_sql=(
            "DELETE FROM estoque "
            "WHERE DATE_FORMAT(STOCK_DATE, '%%Y-%%m') = %s"
        ),
        delete_params=(period,),
        fillna_empty=False,
    )


def clear_and_insert_customers(df: pd.DataFrame) -> None:
    delete_params = [(cid,) for cid in df["CUSTOMER_ID"].unique()]
    _execute_clear_and_insert(
        table_name="clientes",
        df=df,
        insert_columns=[
            "DISTRIBUTOR_CODE", "CUSTOMER_ID", "CUSTOMER_NAME", "CUSTOMER_ADDRESS",
            "CUSTOMER_NEIGHBORHOOD", "CUSTOMER_CITY", "CUSTOMER_UF",
            "CUSTOMER_ZIPCODE", "CUSTOMER_SEGMENTATION", "CUSTOMER_FLAG",
        ],
        delete_sql="DELETE FROM clientes WHERE CUSTOMER_ID = %s",
        delete_params=delete_params,
    )


def clear_and_insert_sales_force(df: pd.DataFrame, ano, mes) -> None:
    """
    Caso especial: DELETE em lotes de 1000 (LIMIT) e INSERT específico,
    portanto não usa _execute_clear_and_insert.
    """
    period = f"{ano}{str(mes).zfill(2)}"
    batch_size = 1000

    connection = engine.raw_connection()
    try:
        cursor = connection.cursor()

        total_deleted = 0
        delete_query = (
            "DELETE FROM forca_vendas WHERE SF_YEAR_MONTH = %s LIMIT %s"
        )
        while True:
            cursor.execute(delete_query, (period, batch_size))
            connection.commit()
            rows_deleted = cursor.rowcount
            total_deleted += rows_deleted
            logger.info(f"Deletados {rows_deleted} registros de forca_vendas.")
            if rows_deleted < batch_size:
                break
        logger.info(f"Total deletado em forca_vendas: {total_deleted}.")

        insert_query = """
            INSERT INTO forca_vendas (
                DISTRIBUTOR_CODE, SF_YEAR_MONTH, SALESREP_ID,
                SF_LEVEL2_ID, SF_LEVEL1_ID
            ) VALUES (%s, %s, %s, %s, %s)
        """
        rows = [
            (r.DISTRIBUTOR_CODE, r.SF_YEAR_MONTH, r.SALESREP_ID,
             r.SF_LEVEL2_ID, r.SF_LEVEL1_ID)
            for r in df.itertuples(index=False)
        ]
        cursor.executemany(insert_query, rows)
        connection.commit()
        cursor.close()
        logger.info(f"{len(rows)} registros inseridos em forca_vendas.")
    except Exception as e:
        connection.rollback()
        logger.error(f"Erro em clear_and_insert_sales_force: {e}")
        raise
    finally:
        connection.close()
