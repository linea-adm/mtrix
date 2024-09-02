#!/bin/bash

# Configurar o fuso horário manualmente
echo "America/Sao_Paulo" > /etc/timezone
dpkg-reconfigure -f noninteractive tzdata

# Esperar até que o MySQL esteja disponível
until mysql -u user -p'sellout2k24' -h db -P 3306 -e "SHOW DATABASES;" > /dev/null 2>&1; do
    echo "Aguardando o MySQL iniciar..."
    sleep 5
done

# Aplicar script SQL para criar tabelas
mysql -u user -p'sellout2k24' -h db -P 3306 sellout_db < /docker-entrypoint-initdb.d/init_db.sql

# Iniciar cron
cron

# Iniciar a aplicação FastAPI
uvicorn main:app --host 0.0.0.0 --port 8005
