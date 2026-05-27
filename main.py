"""
main.py

API FastAPI que dispara o pipeline de ingestão Drive Mtrix -> MySQL.
Depende das funções expostas em utils.py.
"""

import logging
import logging.config
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException
from requests.exceptions import HTTPError, RequestException

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
    clear_and_insert_customers,
)

# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
# Importante: o logging.conf precisa configurar o logger root (ou explicitamente
# o logger "utils") para que as mensagens emitidas por utils.py apareçam.
logging.config.fileConfig("/app/logging.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

app = FastAPI()

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

TABLE_NAME_MAP = {
    "sellout": "sellout",
    "distributors": "distribuidores",
    "products": "produtos",
    "stock": "estoque",
    "sfd": "forca_vendas",
    "customer": "clientes",
}

DATA_TYPES = list(TABLE_NAME_MAP.keys())

# Tipos de dados que exigem ano/mes nas funções de insert
PERIODIC_TYPES = {"sellout", "stock", "sfd"}


# ---------------------------------------------------------------------------
# Núcleo do processamento
# ---------------------------------------------------------------------------

def _split_year_month(period: str) -> tuple[str, str]:
    """Extrai (ano, mes) de um período no formato YYYYMM[DDhhmmss]."""
    if not period or len(period) < 6:
        raise ValueError("Período inválido — esperado YYYYMM ou YYYYMMDD000000.")
    return period[:4], period[4:6]


def process_data(data_type: str, period: str = None, manual_trigger: bool = False) -> None:
    """
    Processa os dados para um tipo específico.

    :param data_type: sellout, distributors, products, stock, sfd ou customer.
    :param period: período no formato YYYYMM ou YYYYMMDD000000.
    :param manual_trigger: se True, monta o path manualmente; caso contrário,
                           consulta a API de listagem para pegar o mais recente.
    """
    if data_type not in TABLE_NAME_MAP:
        logger.error(f"Tipo de dado inválido: {data_type}. Válidos: {DATA_TYPES}")
        raise ValueError(f"Tipo de dado {data_type} não é válido.")

    table_name = TABLE_NAME_MAP[data_type]
    parquet_file_path = zip_file_path = None

    try:
        # 1. Resolve o caminho do arquivo
        if manual_trigger:
            file_path = get_manual_file_path(data_type, period)
        else:
            file_path = get_latest_file_name(data_type, period)

        # 2. Baixa e extrai (com fallback gs:// -> s3a:// embutido no utils)
        parquet_file_path, zip_file_path = download_and_extract_file(file_path)

        # 3. Lê o Parquet
        df = extract_data_from_parquet(parquet_file_path)

        # 4. Persiste no MySQL conforme o tipo
        if data_type == "sellout":
            ano, mes = _split_year_month(period)
            clear_and_insert_data(table_name, df, ano, mes)
        elif data_type == "distributors":
            clear_and_insert_distributors(df)
        elif data_type == "products":
            clear_and_insert_products(df)
        elif data_type == "stock":
            ano, mes = _split_year_month(period)
            clear_and_insert_stock(df, ano, mes)
        elif data_type == "sfd":
            ano, mes = _split_year_month(period)
            clear_and_insert_sales_force(df, ano, mes)
        elif data_type == "customer":
            clear_and_insert_customers(df)

        logger.info(f"Processamento de {data_type} concluído com sucesso.")

    except Exception as e:
        logger.exception(f"Erro no processamento de {data_type}: {e}")
        raise

    finally:
        # 5. Limpeza dos arquivos temporários, mesmo em caso de falha tardia
        if parquet_file_path and zip_file_path:
            try:
                remove_files(parquet_file_path, zip_file_path)
            except OSError as cleanup_err:
                logger.warning(
                    f"Falha ao remover arquivos temporários de {data_type}: {cleanup_err}"
                )


def main_process(period: str = None, manual_trigger: bool = True) -> None:
    """Processa todos os tipos de dados na ordem definida em DATA_TYPES."""
    for data_type in DATA_TYPES:
        try:
            process_data(data_type, period, manual_trigger=manual_trigger)
        except Exception as e:
            # Loga e segue para o próximo tipo, evitando que uma falha
            # parcial aborte todo o pipeline.
            logger.error(f"Falha em {data_type} — seguindo com os demais. Erro: {e}")


# ---------------------------------------------------------------------------
# Helpers de período para os endpoints
# ---------------------------------------------------------------------------

def _normalize_period(period: str | None) -> str:
    """
    Normaliza o período recebido pelos endpoints.

    Aceita:
      - None  -> usa ontem no formato YYYYMMDD000000
      - YYYYMM -> completa com 01000000
      - YYYYMMDD000000 -> usa direto
    """
    if period is None:
        return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d000000")
    if len(period) == 6:
        return period
    if len(period) == 14:
        return period
    raise ValueError("Período deve estar no formato YYYYMM ou YYYYMMDD000000.")


def _http_error_to_response(err: HTTPError, contexto: str) -> HTTPException:
    """Mapeia HTTPError do utils para uma HTTPException adequada da API."""
    status = err.response.status_code if err.response is not None else 502
    logger.error(
        f"Falha HTTP {status} em {contexto} — fallback gs:// -> s3a:// também não resolveu."
    )
    detalhe = (
        f"Origem do arquivo indisponível (HTTP {status}) "
        f"mesmo após fallback gs:// -> s3a://."
    )
    return HTTPException(status_code=502, detail=detalhe)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/trigger-manual")
def trigger_manual(period: str = None):
    """
    Dispara o processamento manual de TODOS os tipos de dados.

    :param period: YYYYMM ou YYYYMMDD000000 (opcional — default: ontem).
    """
    try:
        period = _normalize_period(period)
        main_process(period, manual_trigger=True)
        return {"message": "Processamento realizado com sucesso.", "period": period}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPError as he:
        raise _http_error_to_response(he, contexto="trigger_manual")
    except RequestException as re_err:
        logger.exception("Erro de rede no trigger_manual")
        raise HTTPException(status_code=502, detail=f"Erro de rede: {re_err}")
    except Exception:
        logger.exception("Erro inesperado no trigger_manual")
        raise HTTPException(status_code=500, detail="Erro ao iniciar processo manual.")


@app.post("/trigger/{data_type}")
def trigger_specific(data_type: str, period: str = None):
    """
    Dispara o processamento manual de UM tipo específico de dado.

    :param data_type: sellout, distributors, products, stock, sfd ou customer.
    :param period: YYYYMM ou YYYYMMDD000000 (opcional — default: ontem).
    """
    try:
        if data_type not in TABLE_NAME_MAP:
            raise ValueError(
                f"Tipo de dado '{data_type}' inválido. Use um de: {DATA_TYPES}."
            )

        period = _normalize_period(period)
        process_data(data_type, period, manual_trigger=True)
        return {
            "message": f"Processamento de {data_type} realizado com sucesso.",
            "period": period,
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPError as he:
        raise _http_error_to_response(he, contexto=f"trigger_specific[{data_type}]")
    except RequestException as re_err:
        logger.exception(f"Erro de rede em trigger_specific[{data_type}]")
        raise HTTPException(status_code=502, detail=f"Erro de rede: {re_err}")
    except Exception:
        logger.exception(f"Erro inesperado em trigger_specific[{data_type}]")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao iniciar processamento de {data_type}.",
        )


# ---------------------------------------------------------------------------
# Execução standalone (cron / debug local)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    yesterday_period = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d000000")
    logger.info(f"Execução standalone — período: {yesterday_period}")
    main_process(yesterday_period, manual_trigger=True)
