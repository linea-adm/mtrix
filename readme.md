# MTRIX Data Processing API

## Descrição

Esta API foi desenvolvida para processar dados de diversos tipos (sellout, distributors, products, stock, sfd) a partir de arquivos Parquet obtidos da API MTRIX. A API permite disparar o processamento manualmente ou agendar o processamento automático utilizando cron jobs. 

## Funcionalidades

- Processamento manual de todos os tipos de dados.
- Processamento manual de tipos específicos de dados.
- Suporte a períodos específicos para tipos de dados que necessitam de segmentação por período.
- Integração com cron jobs para execução automática diária.

## Requisitos

- Python 3.11
- MySQL

## Dependências

As dependências necessárias para este projeto estão listadas no arquivo `requirements.txt`:

fastapi uvicorn requests pandas pyarrow fastparquet mysql-connector-python sqlalchemy pymysql


## Instalação

1. Clone o repositório:

    ```bash
    git clone https://github.com/seu_usuario/mtrix-data-processing-api.git
    cd mtrix-data-processing-api
    ```

2. Crie e ative um ambiente virtual (opcional, mas recomendado):

    ```bash
    python -m venv venv
    source venv/bin/activate  # No Windows: venv\\Scripts\\activate
    ```

3. Instale as dependências:

    ```bash
    pip install -r requirements.txt
    ```

## Configuração

Certifique-se de configurar as credenciais da API MTRIX e as configurações do banco de dados no arquivo `utils.py`:

```python
# Configuração do banco de dados
engine = create_engine("mysql+pymysql://user:senha@host:porta/nome_do_banco")

# URLs e credenciais da API
auth_url = 'https://drive.mtrix.com.br/auth'
list_url = 'https://drive.mtrix.com.br/list'
download_url = 'https://drive.mtrix.com.br/download'
auth_payload = {
    'accessKey': 'API_TOKEN',
    'secretKey': 'SECRET_KEY'
}
  Uso da API

Uso da API
==========

Iniciar a API
-------------

Para iniciar a API, execute o comando abaixo:

    uvicorn main:app --host 0.0.0.0 --port 8000

Endpoints
---------

### 1\. Disparar o processamento manual completo

*   **Descrição:** Dispara o processamento de todos os tipos de dados.
*   **Método HTTP:** POST
*   **URL:** `/trigger-manual`
*   **Resposta de Sucesso:**

    {
        "message": "Processo manual iniciado com sucesso."
    }

*   **Exemplo de Uso:**

    curl -X POST "http://localhost:8000/trigger-manual"

### 2\. Disparar o processamento de um tipo específico de dado

*   **Descrição:** Dispara o processamento de um tipo específico de dado.
*   **Método HTTP:** POST
*   **URL:** `/trigger/{data_type}`
*   **Parâmetro de URL:**

*   `data_type`: O tipo de dado a ser processado. Valores possíveis: `sellout`, `distributors`, `products`, `stock`, `sales_force`.

*   **Parâmetro de Consulta (Opcional):**

*   `period`: Período específico para o processamento no formato `YYYYMM` (aplicável a `sellout`, `stock` e `sales_force`).

*   **Resposta de Sucesso:**

    {
        "message": "Processamento de {data_type} iniciado com sucesso."
    }

*   **Exemplos de Uso:**

Disparar o processamento de `sellout` sem período específico:

    curl -X POST "http://localhost:8000/trigger/sellout"

Disparar o processamento de `sellout` com período específico:

    curl -X POST "http://localhost:8000/trigger/sellout?period=202312"

Disparar o processamento de `stock` com período específico:

    curl -X POST "http://localhost:8000/trigger/stock?period=202312"

Disparar o processamento de `sales_force` com período específico:

    curl -X POST "http://localhost:8000/trigger/sales_force?period=202312"

Disparar o processamento de `distributors` sem período específico:

    curl -X POST "http://localhost:8000/trigger/distributors"

Disparar o processamento de `products` sem período específico:

    curl -X POST "http://localhost:8000/trigger/products"

