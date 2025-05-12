FROM python:3.11-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Значения по умолчанию, могут быть переопределены в docker-compose.yml
ENV BATCH_SIZE=20
ENV DESIRED_POOL_WORKERS=9
ENV NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY=4

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    # Зависимости для Playwright Chromium
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 libatspi2.0-0 \
    libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 \
    libxi6 libxrandr2 libxrender1 libxss1 libxtst6 libgbm1 libpango-1.0-0 libcairo2 \
    libasound2 libgdk-pixbuf2.0-0 \
    # curl был для docker-compose, который здесь не нужен
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Установка docker-compose внутри этого контейнера не нужна, если он только запускает скрипт
# ARG DOCKER_COMPOSE_VERSION=v2.24.6
# RUN curl -SL https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64 \
#     -o /usr/local/bin/docker-compose && \
#     chmod +x /usr/local/bin/docker-compose

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps chromium # Установка браузера для Playwright

# Копируем все необходимые скрипты Python
COPY csv_utils.py .
COPY main_worker.py .
COPY proxy_utils.py .
COPY run_parser.py .
COPY soundcloud_parser.py .
# COPY check_proxy_script.py . # Если скрипт проверки прокси отдельный и нужен

# Не копируем users_test.txt, так как он монтируется через volume
# COPY users_test.txt .
# working_proxies.txt также монтируется, можно не копировать или оставить для тестов без compose
# COPY working_proxies.txt .

CMD ["python", "run_parser.py"]