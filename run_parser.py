# run_parser.py
import logging
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import time
import math
import queue # Для queue.Empty

# Убедитесь, что эти файлы существуют и доступны
from proxy_utils import load_proxies_from_file
from csv_utils import initialize_csv_file # Мы модифицируем эту функцию для append_mode
from main_worker import run_worker_task, DEFAULT_CSV_FIELDNAMES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s')

# --- Константы ---
DEFAULT_BATCH_SIZE = 100
DEFAULT_DESIRED_POOL_WORKERS = 19 # Это значение по умолчанию
DEFAULT_NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY = 4 # Это значение по умолчанию

BATCH_SIZE = int(os.environ.get('BATCH_SIZE', DEFAULT_BATCH_SIZE))
DESIRED_POOL_WORKERS = int(os.environ.get('DESIRED_POOL_WORKERS', DEFAULT_DESIRED_POOL_WORKERS))
NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY = int(os.environ.get('NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY', DEFAULT_NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY))

logging.info(f"Using BATCH_SIZE: {BATCH_SIZE}")
logging.info(f"Using DESIRED_POOL_WORKERS: {DESIRED_POOL_WORKERS}")
logging.info(f"Using NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY: {NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY}")

DIRECT_WORKER_FRACTION = 1/3
STOP_SIGNAL = None
URL_FILE = "users_test.txt"
PROXY_FILE = "working_proxies.txt" # Файл с рабочими прокси, генерируемый вашим скриптом проверки
OUTPUT_DATA_DIR = "output_files"
OUTPUT_CSV_FILENAME = os.path.join(OUTPUT_DATA_DIR, "soundcloud_profiles_batched.csv")
PROGRESS_FILE = os.path.join(OUTPUT_DATA_DIR, "processing_progress.txt")

# --- Функции для работы с прогрессом ---
def get_start_index_from_progress(prog_file: str) -> int:
    if os.path.exists(prog_file):
        try:
            with open(prog_file, 'r') as f:
                content = f.read().strip()
                if content:
                    start_index = int(content)
                    # Важно: PROGRESS_FILE хранит индекс НАЧАЛА СЛЕДУЮЩЕГО БАТЧА.
                    # Если BATCH_SIZE=20 и обработано 57, то в файле должно быть 40 (если батчи 0-19, 20-39 завершены).
                    # Если батч 40-59 упал, то при следующем запуске мы должны начать с 40.
                    # Поэтому здесь НЕ НУЖНО округлять start_index до ближайшего батча.
                    # Он УЖЕ должен быть началом батча.
                    logging.info(f"Файл прогресса найден. Возобновление с URL индекса: {start_index}")
                    return start_index
        except ValueError:
            logging.warning(f"Файл прогресса '{prog_file}' содержит некорректное значение. Начинаем с начала (индекс 0).")
        except Exception as e:
            logging.error(f"Ошибка чтения файла прогресса '{prog_file}': {e}. Начинаем с начала (индекс 0).")
    logging.info(f"Файл прогресса '{prog_file}' не найден или пуст. Начинаем с начала (индекс 0).")
    return 0

def save_progress_index(prog_file: str, next_batch_start_index: int):
    """Сохраняет индекс НАЧАЛА СЛЕДУЮЩЕГО БАТЧА, который должен быть обработан."""
    try:
        os.makedirs(os.path.dirname(prog_file), exist_ok=True)
        with open(prog_file, 'w') as f:
            f.write(str(next_batch_start_index))
        # Более информативное логгирование
        logging.info(f"Прогресс сохранен в '{prog_file}'. Следующий батч начнется с URL индекса: {next_batch_start_index}.")
    except Exception as e:
        logging.error(f"Ошибка сохранения прогресса в '{prog_file}': {e}")

# --- Функция чтения URL из файла (без изменений) ---
def load_urls_from_file(filepath: str) -> list[str]:
    urls = []
    if not os.path.exists(filepath):
        logging.error(f"Файл с URL не найден: {filepath}")
        return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):
                    urls.append(url)
        logging.info(f"Загружено {len(urls)} URL из файла {filepath}")
        return urls
    except Exception as e:
        logging.error(f"Ошибка при чтении файла URL {filepath}: {e}")
        return []

# --- Функция-цель для основного прямого воркера (без изменений) ---
def main_direct_worker_target(
    initial_urls: list,
    retry_queue: mp.Queue,
    csv_filename: str,
    csv_lock: mp.Lock
):
    worker_name = mp.current_process().name
    logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name} запущен.")

    if initial_urls:
        logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Обработка {len(initial_urls)} начальных URL...")
        processed_count = run_worker_task(initial_urls, None, csv_filename, csv_lock, None) # retry_queue=None
        logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Начальные задачи обработаны (успешно записано: {processed_count}).")

    logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Начало ожидания задач из очереди ретрая...")
    while True:
        try:
            url_to_retry = retry_queue.get(timeout=1.0)
            if url_to_retry is STOP_SIGNAL:
                logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Получен сигнал СТОП. Завершение.")
                break
            if url_to_retry:
                 logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Получен URL для ретрая: {url_to_retry}")
                 processed_count = run_worker_task([url_to_retry], None, csv_filename, csv_lock, None)
                 logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Ретрай для {url_to_retry} завершен (успешно записано: {processed_count > 0}).")
        except queue.Empty:
            continue
        except (EOFError, BrokenPipeError):
             logging.warning(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Очередь ретрая закрыта или повреждена. Завершение.")
             break
        except Exception as e:
            logging.error(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Ошибка в цикле обработки очереди: {e}", exc_info=True)
            time.sleep(1)
    logging.info(f"ОСНОВНОЙ ПРЯМОЙ ВОРКЕР {worker_name}: Завершил работу.")


# --- Основная функция запуска ---
def main_multiprocess_run():
    overall_start_time = time.time()

    if not os.path.exists(OUTPUT_DATA_DIR):
        try:
            os.makedirs(OUTPUT_DATA_DIR)
            logging.info(f"Создана директория для выходных файлов: {OUTPUT_DATA_DIR}")
        except OSError as e:
            logging.error(f"Не удалось создать директорию {OUTPUT_DATA_DIR}: {e}")
            return

    all_urls_full = load_urls_from_file(URL_FILE)
    if not all_urls_full:
        logging.error("Нет URL для обработки. Завершение.")
        return
    
    total_urls_in_file = len(all_urls_full)

    # --- Возобновление ---
    # start_index_for_this_run - это абсолютный индекс в all_urls_full, с которого начинаем
    start_index_for_this_run = get_start_index_from_progress(PROGRESS_FILE)

    if start_index_for_this_run > 0:
         # Проверка, что start_index не выходит за пределы
        if start_index_for_this_run >= total_urls_in_file:
            logging.info(f"Все {total_urls_in_file} URL уже были обработаны согласно файлу прогресса. Завершение.")
            print_final_csv_summary()
            return
        logging.info(f"Возобновление обработки. Пропускаются первые {start_index_for_this_run} URL.")
    
    # По вашему запросу: "обработалось 60345 ... стартуй уже с 60300"
    # Если get_start_index_from_progress возвращает то, что было сохранено (например, 60300, если предыдущий батч 60200-60299 завершился
    # и мы сохранили 60300 как начало следующего), то это уже правильный индекс.
    # Если скрипт упал на 60345, в файле прогресса должно быть 60300 (начало батча 60300-60399).
    # Поэтому дополнительная корректировка `start_index_for_this_run = (start_index_for_this_run // BATCH_SIZE) * BATCH_SIZE` не нужна,
    # если `save_progress_index` сохраняет именно начало следующего батча.

    if start_index_for_this_run > 0:
         logging.info(f"Возобновление обработки. Пропускаются первые {start_index_for_this_run} URL.")
    
    urls_to_process = all_urls_full[start_index_for_this_run:]

    if not urls_to_process:
        logging.info(f"Нет оставшихся URL для обработки после учета прогресса (все {total_urls_in_file} URL обработаны).")
        save_progress_index(PROGRESS_FILE, total_urls_in_file) 
        print_final_csv_summary()
        return
    
    logging.info(f"Всего URL для обработки в этом сеансе: {len(urls_to_process)} (из {total_urls_in_file} всего, начиная с абсолютного индекса {start_index_for_this_run}).")

    # Инициализация CSV: append_mode=True если start_index_for_this_run > 0
    initialize_csv_file(OUTPUT_CSV_FILENAME, DEFAULT_CSV_FIELDNAMES, append_mode=(start_index_for_this_run > 0))

    manager = mp.Manager()
    csv_file_lock = manager.Lock()

    proxies_list_raw = load_proxies_from_file(PROXY_FILE)
    logging.info(f"Загружено прокси: {len(proxies_list_raw) if proxies_list_raw and proxies_list_raw != [None] else 0} шт.")
    proxies_list = [p for p in proxies_list_raw if p] if proxies_list_raw and proxies_list_raw != [None] else []
    has_real_proxies = bool(proxies_list)
    num_cpu = os.cpu_count() or 1

    # `i` теперь является относительным индексом внутри `urls_to_process`
    for i in range(0, len(urls_to_process), BATCH_SIZE):
        batch_urls = urls_to_process[i : i + BATCH_SIZE]
        
        # Абсолютный индекс начала текущего батча в исходном файле users_test.txt
        current_absolute_start_index_of_batch = start_index_for_this_run + i
        
        # Номер батча и общее количество батчей относительно ПОЛНОГО списка URL
        batch_num_overall = (current_absolute_start_index_of_batch // BATCH_SIZE) + 1
        total_batches_overall = (total_urls_in_file + BATCH_SIZE - 1) // BATCH_SIZE

        logging.info(f"\n{'='*20} НАЧАЛО БАТЧА {batch_num_overall}/{total_batches_overall} ({len(batch_urls)} URL) {'='*20}")
        logging.info(f"(Обработка URL с абсолютного индекса {current_absolute_start_index_of_batch} по {current_absolute_start_index_of_batch + len(batch_urls) - 1})")
        
        # ВАЖНО: Перед началом обработки батча, мы НЕ обновляем файл прогресса здесь.
        # Обновление произойдет только ПОСЛЕ УСПЕШНОГО ЗАВЕРШЕНИЯ БАТЧА.
        # Если скрипт упадет во время этого батча, файл прогресса будет содержать
        # current_absolute_start_index_of_batch (или индекс начала предыдущего успешно завершенного батча),
        # и этот батч будет перезапущен.

        batch_start_time = time.time()

        retry_queue = manager.Queue()
        urls_for_main_direct_worker = []
        pool_worker_tasks = []

        direct_worker_url_count = math.ceil(len(batch_urls) * DIRECT_WORKER_FRACTION)
        urls_for_main_direct_worker = batch_urls[:direct_worker_url_count]
        remaining_urls_for_pool = batch_urls[direct_worker_url_count:]

        # ... (остальная логика распределения задач по воркерам, как в вашем оригинальном скрипте) ...
        num_pool_workers_to_launch = 0
        if remaining_urls_for_pool:
            max_pool_workers_cpu_limit = (num_cpu - 1) if num_cpu > 1 else 0 
            effective_desired_pool_workers = min(DESIRED_POOL_WORKERS, max_pool_workers_cpu_limit) if max_pool_workers_cpu_limit > 0 else 0
            num_pool_workers_to_launch = min(effective_desired_pool_workers, len(remaining_urls_for_pool))
            if num_pool_workers_to_launch < 0: num_pool_workers_to_launch = 0

        if num_pool_workers_to_launch == 0 and remaining_urls_for_pool:
            urls_for_main_direct_worker.extend(remaining_urls_for_pool)
            logging.info(f"Батч {batch_num_overall}: Пул воркеров не запускается. Все {len(remaining_urls_for_pool)} оставшихся URL переданы основному прямому воркеру.")
            remaining_urls_for_pool = []
        elif num_pool_workers_to_launch > 0 :
            pool_chunk_size = (len(remaining_urls_for_pool) + num_pool_workers_to_launch - 1) // num_pool_workers_to_launch
            url_chunks_for_pool = [
                remaining_urls_for_pool[k:k + pool_chunk_size]
                for k in range(0, len(remaining_urls_for_pool), pool_chunk_size)
            ][:num_pool_workers_to_launch]
            num_pool_workers_to_launch = len(url_chunks_for_pool)

            assigned_direct_in_pool = 0
            assigned_proxied_in_pool = 0
            current_proxy_idx = 0
            for k_chunk_idx in range(num_pool_workers_to_launch):
                chunk = url_chunks_for_pool[k_chunk_idx]
                if not chunk: continue
                proxy_to_assign = None
                if assigned_direct_in_pool < NUM_POOL_WORKERS_SPECIFICALLY_WITHOUT_PROXY:
                    proxy_to_assign = None
                    assigned_direct_in_pool += 1
                elif has_real_proxies:
                    proxy_to_assign = proxies_list[current_proxy_idx % len(proxies_list)]
                    current_proxy_idx += 1
                    assigned_proxied_in_pool += 1
                else:
                    proxy_to_assign = None
                    assigned_direct_in_pool += 1
                pool_worker_tasks.append({'chunk': chunk, 'proxy': proxy_to_assign})
        
        logging.info(f"Батч {batch_num_overall}: Основной прямой воркер: {len(urls_for_main_direct_worker)} URL.")
        if pool_worker_tasks:
             assigned_direct_in_pool_actual = sum(1 for task in pool_worker_tasks if task['proxy'] is None)
             assigned_proxied_in_pool_actual = sum(1 for task in pool_worker_tasks if task['proxy'] is not None)
             logging.info(f"Батч {batch_num_overall}: Пул: {len(pool_worker_tasks)} воркеров. "
                          f"Из них БЕЗ ПРОКСИ (в пуле): {assigned_direct_in_pool_actual}, "
                          f"С ПРОКСИ: {assigned_proxied_in_pool_actual}.")
        else:
            logging.info(f"Батч {batch_num_overall}: Пул воркеров не будет запущен для этого батча.")

        has_direct_worker_activity = bool(urls_for_main_direct_worker or pool_worker_tasks)
        total_workers_in_batch = (1 if has_direct_worker_activity else 0) + len(pool_worker_tasks)
        logging.info(f"Батч {batch_num_overall}: Всего будет запущено процессов (основной + пул): {total_workers_in_batch}")

        batch_processed_successfully_by_pool = 0
        main_direct_worker_process = None
        pool_worker_futures = []

        if has_direct_worker_activity:
            logging.info(f"Батч {batch_num_overall}: Запуск ОСНОВНОГО ПРЯМОГО воркера...")
            main_direct_worker_process = mp.Process(
                target=main_direct_worker_target,
                args=(urls_for_main_direct_worker, retry_queue, OUTPUT_CSV_FILENAME, csv_file_lock),
                name=f"MainDirectWorker-B{batch_num_overall}"
            )
            main_direct_worker_process.start()
        # ... (pool executor logic) ...
        if pool_worker_tasks:
            actual_pool_size = len(pool_worker_tasks)
            logging.info(f"Батч {batch_num_overall}: Запуск пула для {actual_pool_size} воркеров...")
            with ProcessPoolExecutor(max_workers=max(1, actual_pool_size)) as executor:
                for task_idx, task_info in enumerate(pool_worker_tasks):
                    chunk = task_info['chunk']
                    proxy_str = task_info['proxy']
                    worker_type_log = "БЕЗ ПРОКСИ (в пуле)" if proxy_str is None else f"С ПРОКСИ: {proxy_str}"
                    logging.info(f"Батч {batch_num_overall}: Отправка задачи ВОРКЕРУ ПУЛА {task_idx+1}/{actual_pool_size} ({worker_type_log}) для {len(chunk)} URL.")
                    future = executor.submit(
                        run_worker_task,
                        chunk,
                        proxy_str,
                        OUTPUT_CSV_FILENAME,
                        csv_file_lock,
                        retry_queue
                    )
                    pool_worker_futures.append(future)
                logging.info(f"Батч {batch_num_overall}: Ожидание завершения {len(pool_worker_futures)} воркеров пула...")
                for future in as_completed(pool_worker_futures):
                    try:
                        processed_count_by_future = future.result(timeout=None)
                        batch_processed_successfully_by_pool += processed_count_by_future
                    except Exception as e:
                        logging.error(f"Батч {batch_num_overall}: Ошибка при получении результата от воркера пула: {e}", exc_info=False)
                logging.info(f"Батч {batch_num_overall}: Все воркеры пула завершили работу. Успешно обработано пулом (первичные попытки): {batch_processed_successfully_by_pool}.")
        else:
             logging.info(f"Батч {batch_num_overall}: Воркеры пула не запускались в этом батче.")

        if main_direct_worker_process:
             logging.info(f"Батч {batch_num_overall}: Отправка сигнала СТОП основному прямому воркеру...")
             retry_queue.put(STOP_SIGNAL)
             logging.info(f"Батч {batch_num_overall}: Ожидание завершения основного прямого воркера...")
             main_direct_worker_process.join(timeout=180)
             if main_direct_worker_process.is_alive():
                 logging.warning(f"Батч {batch_num_overall}: Основной прямой воркер не завершился вовремя, принудительное завершение...")
                 main_direct_worker_process.terminate()
                 main_direct_worker_process.join(timeout=10)
             else:
                  logging.info(f"Батч {batch_num_overall}: Основной прямой воркер успешно завершен.")
        
        retry_queue._close()
        
        # ----- Обновление прогресса ПОСЛЕ успешной обработки батча -----
        next_batch_start_index_for_progress = current_absolute_start_index_of_batch + len(batch_urls)
        save_progress_index(PROGRESS_FILE, next_batch_start_index_for_progress)
        # ---------------------------------------------------------------

        batch_end_time = time.time()
        logging.info(f"======= ЗАВЕРШЕНИЕ БАТЧА {batch_num_overall}/{total_batches_overall} =======")
        logging.info(f"Время выполнения батча: {batch_end_time - batch_start_time:.2f} сек.")
        logging.info(f"Успешно обработано воркерами пула (первичные попытки): {batch_processed_successfully_by_pool}")
        logging.info(f"Прогресс обновлен. Следующий запуск начнется с URL с абсолютным индексом: {next_batch_start_index_for_progress}")

    logging.info(f"Все запланированные батчи для этого запуска обработаны.")
    final_processed_index = start_index_for_this_run + len(urls_to_process)
    save_progress_index(PROGRESS_FILE, final_processed_index) # Сохраняем финальный прогресс
    logging.info(f"Финальный прогресс сохранен: обработка завершена до абсолютного URL индекса {final_processed_index}.")
    
    logging.info(f"Результаты сохранены в {OUTPUT_CSV_FILENAME}")
    overall_end_time = time.time()
    logging.info(f"Общее время выполнения этого сеанса: {overall_end_time - overall_start_time:.2f} секунд.")

    print_final_csv_summary()

def print_final_csv_summary():
    """Печатает информацию о количестве строк в выходном CSV."""
    print(f"\nПарсинг завершен (или был завершен ранее).")
    print(f"Результаты (включая успешные и ошибки, если были) в файле: {OUTPUT_CSV_FILENAME}")
    try:
        import csv as csv_module # Локальный импорт, чтобы не конфликтовать с глобальным `queue`
        if os.path.exists(OUTPUT_CSV_FILENAME):
            if os.path.getsize(OUTPUT_CSV_FILENAME) > 0:
                with open(OUTPUT_CSV_FILENAME, 'r', encoding='utf-8', newline='') as f_csv:
                    reader = csv_module.reader(f_csv)
                    try:
                        header = next(reader) 
                        data_rows_count = sum(1 for _ in reader)
                        print(f"Всего строк данных в {OUTPUT_CSV_FILENAME}: {data_rows_count} (не считая заголовок)")
                    except StopIteration: 
                        print(f"Всего строк данных в {OUTPUT_CSV_FILENAME}: 0 (файл пуст или содержит только заголовок)")
            else:
                print(f"Выходной файл {OUTPUT_CSV_FILENAME} пуст.")
        else:
            print(f"Выходной файл {OUTPUT_CSV_FILENAME} не найден или не был создан.")
    except Exception as e:
        print(f"Не удалось посчитать строки в CSV: {e}")

if __name__ == "__main__":
    try:
        mp.set_start_method('spawn', force=True) 
        logging.info("Multiprocessing start method set to 'spawn'.")
    except RuntimeError:
        logging.info("Multiprocessing start method already set or cannot be changed. Proceeding.")
    main_multiprocess_run()