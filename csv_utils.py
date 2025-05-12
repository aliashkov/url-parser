# csv_utils.py
import csv
import logging
import os
import threading # Для блокировки на уровне потоков, если используется в async
                  # Для multiprocessing нужна mp.Lock, передаваемая в воркер

def initialize_csv_file(filename: str, fieldnames: list):
    """Инициализирует CSV файл с заголовками, если он не существует или пуст."""
    file_exists = os.path.isfile(filename)
    is_empty = file_exists and os.path.getsize(filename) == 0

    if not file_exists or is_empty:
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
            logging.info(f"CSV файл '{filename}' инициализирован с заголовками: {fieldnames}")
        except IOError as e:
            logging.error(f"Ошибка IOError при инициализации CSV файла {filename}: {e}")

def append_to_csv(data_item: dict, filename: str, fieldnames: list, lock: threading.Lock = None): # threading.Lock здесь тип по умолчанию, но из mp придет mp.Lock
    """
    Дописывает одну строку данных в CSV файл.
    Использует блокировку для безопасной записи из нескольких потоков/процессов.
    """
    if not isinstance(data_item, dict):
        logging.warning(f"Пропуск несловарных данных при дозаписи в CSV: {data_item}")
        return

    row_to_write = data_item.copy()
    if 'emails' in row_to_write and isinstance(row_to_write.get('emails'), list):
        row_to_write['emails'] = ', '.join(row_to_write['emails'])
    
    for key in fieldnames:
        if key not in row_to_write:
            row_to_write[key] = '' # Убедимся, что все поля из fieldnames присутствуют

    acquired_lock = False
    if lock:
        lock.acquire()
        acquired_lock = True
    
    try:
        # Проверяем, существует ли файл и не пустой ли он (на случай удаления между инициализацией и записью)
        # Однако, initialize_csv_file должен вызываться один раз в начале главного процесса.
        # Если файл внезапно исчез, append создаст его, но без заголовка.
        # Для большей надежности, можно проверять наличие заголовка, но это усложнит.
        # Полагаемся, что initialize_csv_file отработал корректно.
        
        with open(filename, mode='a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            # Заголовок пишется только при инициализации.
            # Если файл был пуст (или только что создан 'a'), DictWriter сам не напишет заголовок.
            # Это задача initialize_csv_file.
            writer.writerow(row_to_write)
        logging.debug(f"Данные для URL '{data_item.get('url')}' дописаны в {filename}")
    except IOError as e:
        logging.error(f"Ошибка IOError при дозаписи в CSV файл {filename} для URL '{data_item.get('url')}': {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при дозаписи в CSV файл {filename} для URL '{data_item.get('url')}': {e}")
    finally:
        if acquired_lock and lock:
            lock.release()


def save_all_to_csv(data_list: list, filename: str):
    """
    Сохраняет весь список данных в CSV файл, перезаписывая его.
    (Эта функция остается для случая, когда мы хотим сохранить все сразу в конце)
    """
    if not data_list:
        logging.info(f"Нет данных для сохранения в файл {filename}.")
        return

    # Обновленный список полей по умолчанию, включая 'followers'
    default_fieldnames = ['url', 'followers', 'website', 'youtube', 'facebook', 'twitter', 'instagram',
                          'songkick', 'telegram', 'tiktok', 'linkedin', 'emails', 'error']
    
    fieldnames_to_use = default_fieldnames[:] 
    
    if data_list and isinstance(data_list[0], dict):
        for key in data_list[0].keys():
            if key not in fieldnames_to_use:
                fieldnames_to_use.append(key)
    
    initialize_csv_file(filename, fieldnames_to_use) 

    lock = threading.Lock() 
    for item in data_list:
        append_to_csv(item, filename, fieldnames_to_use, lock)

    logging.info(f"Все данные ({len(data_list)} строк) сохранены в {filename} (пакетная запись).")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    test_filename = "test_append.csv"
    # Обновленный список полей для теста
    fields = ['url', 'followers', 'website', 'emails', 'error', 'extra']
    
    if os.path.exists(test_filename): os.remove(test_filename)
    initialize_csv_file(test_filename, fields)
    
    test_data1 = {'url': 'url1', 'followers': '10k', 'website': 'site1', 'emails': ['email1@ex.com'], 'error': '', 'extra': 'val1'}
    test_data2 = {'url': 'url2', 'followers': '200', 'website': 'site2', 'error': 'err2'}
    test_data3 = {'url': 'url3', 'followers': '1.5M', 'emails': ['email3@ex.com', 'email4@ex.com']}

    test_lock = threading.Lock() 
    
    append_to_csv(test_data1, test_filename, fields, test_lock)
    append_to_csv(test_data2, test_filename, fields, test_lock)
    append_to_csv(test_data3, test_filename, fields, test_lock)
    
    print(f"Проверьте файл {test_filename}")

    # if os.path.exists("test_append.csv"): os.remove("test_append.csv")