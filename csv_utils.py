# csv_utils.py
import csv
import logging
import os
# threading Lock не нужен, если используем mp.Lock из run_parser.py
# import threading

def initialize_csv_file(filename: str, fieldnames: list, append_mode: bool = False):
    """
    Инициализирует CSV файл.
    Если append_mode=True и файл существует и не пустой, то заголовок НЕ ПИШЕТСЯ.
    В противном случае (новый файл, пустой файл, или append_mode=False),
    файл создается/перезаписывается и заголовок пишется.
    """
    # Убедимся, что директория для файла существует
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
    except OSError as e:
        logging.error(f"Не удалось создать директорию для CSV {os.path.dirname(filename)}: {e}")
        raise # Передаем ошибку выше, это критично

    file_exists_and_not_empty = os.path.isfile(filename) and os.path.getsize(filename) > 0
    
    write_header = not (append_mode and file_exists_and_not_empty)
    
    open_mode = 'w' 
    if append_mode and file_exists_and_not_empty:
        open_mode = 'a'
    
    try:
        with open(filename, mode=open_mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
                if open_mode == 'w' and not (append_mode and file_exists_and_not_empty):
                    logging.info(f"CSV файл '{filename}' инициализирован/перезаписан с заголовком.")
                # Если append_mode=True, но файл был пуст/не существовал, заголовок также пишется
            elif append_mode and file_exists_and_not_empty:
                logging.info(f"CSV файл '{filename}' открыт для дозаписи (заголовок не перезаписан).")

    except IOError as e:
        logging.error(f"Ошибка IOError при инициализации/открытии CSV файла {filename}: {e}")
        raise

def append_to_csv(data_item: dict, filename: str, fieldnames: list, lock = None): # lock может быть mp.Lock
    """
    Дописывает одну строку данных в CSV файл.
    Использует блокировку для безопасной записи из нескольких потоков/процессов.
    """
    if not isinstance(data_item, dict):
        logging.warning(f"Пропуск несловарных данных при дозаписи в CSV: {data_item}")
        return

    row_to_write = data_item.copy()
    if 'emails' in row_to_write and isinstance(row_to_write.get('emails'), list):
        row_to_write['emails'] = ', '.join(map(str, row_to_write['emails']))
    
    for key in fieldnames:
        if key not in row_to_write:
            row_to_write[key] = '' 

    acquired_lock = False
    if lock:
        if hasattr(lock, 'acquire') and hasattr(lock, 'release'):
            lock.acquire()
            acquired_lock = True
        else:
            logging.warning("Переданный объект lock не похож на объект блокировки. Запись без блокировки.")
    
    try:
        # Проверка на необходимость записи заголовка, если файл был создан в режиме 'a' и он пуст
        # Эта проверка гарантирует, что заголовок будет, даже если initialize_csv_file не смогла его создать
        # (например, если файл был удален между initialize и первым append).
        file_exists = os.path.isfile(filename)
        header_needed = not file_exists or os.path.getsize(filename) == 0

        with open(filename, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            if header_needed:
                writer.writeheader()
                logging.info(f"Заголовок записан в '{filename}' при дозаписи (файл был пуст/отсутствовал).")
            writer.writerow(row_to_write)
        logging.debug(f"Данные для URL '{data_item.get('url')}' дописаны в {filename}")
    except IOError as e:
        logging.error(f"Ошибка IOError при дозаписи в CSV файл {filename} для URL '{data_item.get('url')}': {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при дозаписи в CSV файл {filename} для URL '{data_item.get('url')}': {e}")
    finally:
        if acquired_lock and lock and hasattr(lock, 'release'):
            lock.release()

# Функция save_all_to_csv (если она вам нужна, оставьте ее без изменений или адаптируйте)
# ...

if __name__ == '__main__':
    # Тестовый код для csv_utils.py (можно расширить)
    logging.basicConfig(level=logging.DEBUG)
    test_filename = os.path.join("output_files_test", "test_append.csv") # Помещаем в тестовую поддиректорию
    fields = ['url', 'followers', 'website', 'emails', 'error', 'extra']
    
    # Создаем директорию, если ее нет
    os.makedirs(os.path.dirname(test_filename), exist_ok=True)

    if os.path.exists(test_filename): os.remove(test_filename)
    
    print("Тест 1: Инициализация нового файла")
    initialize_csv_file(test_filename, fields, append_mode=False) # Новый файл, пишем заголовок
    
    test_data1 = {'url': 'url1', 'followers': '10k', 'website': 'site1', 'emails': ['email1@ex.com'], 'error': '', 'extra': 'val1'}
    test_lock = mp.Manager().Lock() if 'multiprocessing' in dir() and hasattr(mp, 'Manager') else None # Для теста создадим, если возможно
    
    append_to_csv(test_data1, test_filename, fields, test_lock)
    
    print("Тест 2: Дозапись в существующий (append_mode=True)")
    initialize_csv_file(test_filename, fields, append_mode=True) # Существующий, не пустой, заголовок не пишем
    test_data2 = {'url': 'url2', 'followers': '200', 'website': 'site2', 'error': 'err2'}
    append_to_csv(test_data2, test_filename, fields, test_lock)
        
    print(f"Проверьте файл {test_filename}")
    # Почистить после теста
    # if os.path.exists(test_filename): os.remove(test_filename)
    # if os.path.exists(os.path.dirname(test_filename)): os.rmdir(os.path.dirname(test_filename))