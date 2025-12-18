FROM python:3.11-slim

WORKDIR /usr/app

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.10.9 \
    dbt-postgres==1.9.0

COPY . .

CMD ["dbt", "run"]