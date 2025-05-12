# proxy_utils.py
import logging
from urllib.parse import urlparse, unquote

def parse_proxy_string(proxy_string: str | None) -> dict | None:
    """
    Парсит строку прокси в словарь для Playwright.
    """
    if not proxy_string:
        return None
    
    try:
        parsed = urlparse(proxy_string)
        if not parsed.scheme or not parsed.hostname:
            logging.warning(f"Некорректный формат строки прокси: '{proxy_string}'. Отсутствует схема или хост.")
            return None
        server_url = f"{parsed.scheme}://{parsed.hostname}"
        if parsed.port:
            server_url += f":{parsed.port}"
        
        proxy_config = {"server": server_url}
        if parsed.username:
            proxy_config["username"] = unquote(parsed.username)
        if parsed.password:
            proxy_config["password"] = unquote(parsed.password)
        logging.debug(f"Прокси '{proxy_string}' успешно разобран: {proxy_config}")
        return proxy_config
    except Exception as e:
        logging.error(f"Критическая ошибка при парсинге строки прокси '{proxy_string}': {e}")
        return None

def load_proxies_from_file(filepath="proxies.txt") -> list[str | None]:
    """
    Загружает список прокси из файла.
    Каждая строка в файле должна быть в формате IP:PORT или user:pass@IP:PORT.
    Автоматически добавляет префикс 'http://', если схема не указана.
    """
    proxies = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '://' not in line:
                        proxies.append(f"http://{line}") # По умолчанию HTTP
                    else:
                        proxies.append(line)
        if proxies:
            logging.info(f"Загружено {len(proxies)} прокси из файла {filepath}")
            return proxies
        else:
            logging.warning(f"Файл прокси {filepath} пуст или содержит только комментарии.")
            return [None] 
    except FileNotFoundError:
        logging.warning(f"Файл прокси {filepath} не найден.")
        return [None] 
    except Exception as e:
        logging.error(f"Ошибка при чтении файла прокси {filepath}: {e}.")
        return [None]

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    # Тесты для parse_proxy_string
    test_proxies_strings = [
        "http://myuser:mypassword@proxy.example.com:8080",
        "socks5://anotheruser:anotherpass@socksproxy.example.com:1080",
        "http://192.168.1.100:3128", None, "invalid_proxy_string"
    ]
    for proxy_str in test_proxies_strings:
        print(f"Тест строки: '{proxy_str}' -> {parse_proxy_string(proxy_str)}")

    # Тест для load_proxies_from_file (создайте dummy proxies.txt для теста)
    # with open("proxies.txt", "w") as f:
    #     f.write("1.2.3.4:8080\n")
    #     f.write("# это комментарий\n")
    #     f.write("user:pass@5.6.7.8:1234\n")
    #     f.write("socks5://9.10.11.12:1080\n")
    
    # loaded = load_proxies_from_file("proxies.txt")
    # print(f"Загруженные прокси: {loaded}")
    # if os.path.exists("proxies.txt"): os.remove("proxies.txt")