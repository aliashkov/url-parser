import requests
import concurrent.futures
import logging
import time
import os
from urllib.parse import urlparse

# --- Настройки ---
PROXY_FILE = "only_proxy2.txt"  
OUTPUT_WORKING_FILE = "working_proxies.txt" 
CHECK_URL = "https://soundcloud.com/martingarrix/" 
TIMEOUT_SECONDS = 10  
MAX_WORKERS = 50     
# -----------------

# Настройка логирования: Устанавливаем уровень INFO, чтобы DEBUG логи не отображались по умолчанию
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

# Отключаем излишние логи от urllib3, которые могут появляться при ошибках прокси
logging.getLogger("urllib3").setLevel(logging.WARNING)

def format_proxy_for_requests(proxy_string: str) -> dict | None:
    """
    Готовит словарь с прокси для библиотеки requests.
    """
    if not proxy_string:
        return None
    if '://' not in proxy_string:
        proxy_string = f"http://{proxy_string}"
    parsed = urlparse(proxy_string)
    if not parsed.scheme or not parsed.hostname:
         # Не будем логировать предупреждение, просто вернем None
         # logging.warning(f"Некорректный формат прокси после добавления схемы: '{proxy_string}'")
         return None
    return {'http': proxy_string, 'https': proxy_string}

def check_proxy(proxy_string_raw: str) -> str | None:
    """
    Проверяет один прокси, делая запрос к CHECK_URL (SoundCloud).
    Возвращает строку, если рабочий (получен ответ 2xx), иначе None.
    Логирует ТОЛЬКО успешные проверки.
    """
    proxy_dict = format_proxy_for_requests(proxy_string_raw)
    if not proxy_dict:
        return None

    proxy_for_log = proxy_dict.get('http') or proxy_dict.get('https', '')
    start_time = time.time()
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4430.93 Safari/537.36 ProxyChecker',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            # 'Upgrade-Insecure-Requests': '1', # Можно добавить
        }
        
        # Делаем запрос к SoundCloud
        response = requests.get(
            CHECK_URL, 
            proxies=proxy_dict, 
            timeout=TIMEOUT_SECONDS,
            headers=headers,
            allow_redirects=True # SoundCloud может редиректить
        )
        
        # Главная проверка - успешный статус код
        response.raise_for_status()  # Вызовет исключение для 4xx/5xx ошибок
        
        # Если исключения не было, прокси смог получить ответ от SoundCloud
        logging.info(f"РАБОЧИЙ (для {CHECK_URL}): {proxy_string_raw} (Статус: {response.status_code}, время: {time.time() - start_time:.2f}с)")
        return proxy_string_raw # Возвращаем исходную строку прокси

    # Обрабатываем ошибки, не логируя их (кроме неожиданных)
    except requests.exceptions.Timeout:
        return None
    except requests.exceptions.ProxyError:
        return None
    except requests.exceptions.RequestException as e: 
        # Можно залогировать статус код ошибки, если это не таймаут/прокси
        # if isinstance(e, requests.exceptions.HTTPError):
        #     logging.debug(f"НЕ РАБОЧИЙ (HTTP Error {e.response.status_code}): {proxy_string_raw}")
        return None
    except Exception as e:
        logging.error(f"НЕОЖИДАННАЯ ОШИБКА при проверке {proxy_string_raw}: {e}", exc_info=False)
        return None

def load_raw_proxies(filepath: str) -> list[str]:
    """Загружает 'сырые' строки прокси из файла."""
    proxies = []
    if not os.path.exists(filepath):
        logging.error(f"Файл с прокси не найден: {filepath}")
        return []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    proxies.append(line)
        logging.info(f"Загружено {len(proxies)} строк прокси из файла {filepath}")
        return proxies
    except Exception as e:
        logging.error(f"Ошибка при чтении файла прокси {filepath}: {e}")
        return []

if __name__ == "__main__":
    print(f"--- Проверка прокси из файла '{PROXY_FILE}' ---")
    print(f"URL для проверки: {CHECK_URL}")
    print(f"Таймаут: {TIMEOUT_SECONDS} сек")
    print(f"Потоков для проверки: {MAX_WORKERS}")
    print("Ожидайте, идет проверка (в лог выводятся только рабочие прокси)...")
    
    raw_proxies_to_check = load_raw_proxies(PROXY_FILE)
    
    if not raw_proxies_to_check:
        print("Список прокси для проверки пуст. Завершение.")
    else:
        working_proxies = []
        futures = []
        start_check_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(check_proxy, proxy_str) for proxy_str in raw_proxies_to_check]
            
            # Собираем результаты (не выводим прогресс, ждем завершения)
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result: 
                    working_proxies.append(result)
        
        end_check_time = time.time()
        total_checked = len(raw_proxies_to_check)
        total_working = len(working_proxies)
        total_failed = total_checked - total_working
        
        print("\n--- Результаты проверки ---")
        print(f"Всего проверено: {total_checked}")
        print(f"Рабочих: {total_working}")
        print(f"Нерабочих/Таймаут: {total_failed}")
        print(f"Время проверки: {end_check_time - start_check_time:.2f} сек")
        
        if working_proxies:
            try:
                # Сортируем для консистентности (опционально)
                working_proxies.sort() 
                with open(OUTPUT_WORKING_FILE, 'w') as f:
                    for proxy in working_proxies:
                        f.write(proxy + '\n')
                print(f"Список рабочих прокси сохранен в файл: '{OUTPUT_WORKING_FILE}'")
            except IOError as e:
                print(f"Ошибка записи в файл '{OUTPUT_WORKING_FILE}': {e}")
        else:
            print("Рабочих прокси не найдено.")
            if os.path.exists(OUTPUT_WORKING_FILE):
                 try:
                     os.remove(OUTPUT_WORKING_FILE)
                     print(f"Файл '{OUTPUT_WORKING_FILE}' удален.")
                 except OSError as e:
                     print(f"Не удалось удалить файл '{OUTPUT_WORKING_FILE}': {e}")