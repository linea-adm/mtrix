FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Sao_Paulo

# Atualizar os pacotes e instalar dependências essenciais
RUN apt-get update && \
    apt-get install -y python3 python3-pip cron vim nano curl procps mysql-client tzdata && \
    apt-get clean

# Configurar o fuso horário manualmente
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Configurar diretório de trabalho
WORKDIR /app

# Copiar arquivos de requisitos e instalar dependências Python
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copiar arquivos da aplicação
COPY . .

# Script de inicialização para criar as tabelas
COPY init_db.sql /docker-entrypoint-initdb.d/

# Adicionar script de cron ao crontab
RUN echo "0 8 * * * /usr/bin/python3 /app/main.py >> /app/logs/cron.log 2>&1" > /etc/cron.d/mtrix-cron
RUN echo "29 11 * * * /usr/bin/python3 /app/main.py >> /app/logs/cron.log 2>&1" >> /etc/cron.d/mtrix-cron
# RUN echo "*/15 * * * * /usr/bin/python3 /app/main.py >> /app/logs/cron.log 2>&1" >> /etc/cron.d/mtrix-cron

# Adicionar permissões corretas ao arquivo cron
RUN chmod 0644 /etc/cron.d/mtrix-cron

# Aplicar crontab
RUN crontab /etc/cron.d/mtrix-cron

# Expor a porta para a API FastAPI (caso necessário para disparo manual)
EXPOSE 8005

# Script de entrada customizado para aguardar o MySQL estar pronto
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
