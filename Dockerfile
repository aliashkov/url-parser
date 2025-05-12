FROM python:3.11-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Значения по умолчанию, могут быть переопределены в docker-compose.yml или при запуске docker run -e
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
    # Зависимости, которые могут понадобиться для curl или других утилит
    ca-certificates curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --- Начало блока установки docker-compose ---
# ВНИМАНИЕ: Установка docker-compose ВНУТРИ этого контейнера обычно НЕ нужна,
# если этот контейнер просто запускает Python скрипт (run_parser.py).
# docker-compose используется на ХОСТЕ для управления контейнерами.
# Оставляю закомментированным, так как скорее всего это не требуется.
# Если он действительно нужен ВНУТРИ контейнера для какой-то специфической задачи, раскомментируйте:
# ARG DOCKER_COMPOSE_VERSION=v2.24.6
# RUN curl -SL https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64 \
#     -o /usr/local/bin/docker-compose && \
#     chmod +x /usr/local/bin/docker-compose
# --- Конец блока установки docker-compose ---

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Установка браузера для Playwright и его зависимостей
RUN playwright install --with-deps chromium

# Копируем все необходимые скрипты Python
COPY csv_utils.py .
COPY main_worker.py .
COPY proxy_utils.py .
COPY run_parser.py .
COPY soundcloud_parser.py .
# COPY check_proxy_script.py . # Раскомментируйте, если этот файл существует и нужен

# --- Копируем файлы данных ВНУТРЬ образа ---
# Убедитесь, что эти файлы существуют в том же каталоге, что и Dockerfile,
# или укажите правильный путь относительно контекста сборки.
COPY users_test.txt .
COPY working_proxies.txt .
# --- Конец копирования файлов данных ---

# output_files будут создаваться внутри контейнера.
# Если нужно сохранить их на хосте, используйте volume в docker-compose.yml
# для директории /app/output_files

CMD ["python", "run_parser.py"]