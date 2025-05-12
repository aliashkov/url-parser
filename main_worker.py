# main_worker.py
import asyncio
import logging
import multiprocessing as mp 
import random 
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

from proxy_utils import parse_proxy_string
from soundcloud_parser import parse_soundcloud_profile_html # Убедитесь, что обновленный парсер импортируется
from csv_utils import append_to_csv 

DEFAULT_CSV_FIELDNAMES = ['url', 'followers', 'website', 'youtube', 'facebook', 'twitter', 'instagram',
                          'songkick', 'telegram', 'tiktok', 'linkedin', 'emails', 'error'] 

MAX_GOTO_RETRIES = 3 
INITIAL_RETRY_DELAY = 3

async def process_single_url_in_worker(page, url: str) -> dict:
    data = {
        'url': url, 'followers': '', 'website': '', 'youtube': '', 'facebook': '', 'twitter': '',
        'instagram': '', 'songkick': '', 'telegram': '', 'tiktok': '', 'linkedin': '',
        'emails': [], 'error': ''
    }
    page_timeout = 180000 
    cookie_click_timeout = 10000
    content_selector_timeout = 15000

    goto_success = False 
    last_goto_error = None 

    for attempt in range(MAX_GOTO_RETRIES):
        try:
            logging.info(f"[{url}] Попытка {attempt + 1}/{MAX_GOTO_RETRIES}: Начало загрузки страницы...")
            await page.goto(url, wait_until="domcontentloaded", timeout=page_timeout)
            logging.info(f"[{url}] Попытка {attempt + 1}: Страница успешно загружена.")
            goto_success = True 
            last_goto_error = None 
            break 
        except (PlaywrightTimeoutError, PlaywrightError) as e: 
            last_goto_error = e 
            error_type = type(e).__name__
            error_message = str(e).split('\n')[0]
            logging.warning(f"[{url}] Попытка {attempt + 1}/{MAX_GOTO_RETRIES} не удалась ({error_type}): {error_message}")
            if attempt < MAX_GOTO_RETRIES - 1:
                retry_delay = (INITIAL_RETRY_DELAY * (2 ** attempt)) + random.uniform(0.5, 1.5)
                logging.info(f"[{url}] Пауза {retry_delay:.2f} сек перед повторной попыткой...")
                await asyncio.sleep(retry_delay)
            else:
                logging.error(f"[{url}] Превышено максимальное количество попыток ({MAX_GOTO_RETRIES}) для page.goto.", exc_info=False)
                data['error'] = f"Превышено {MAX_GOTO_RETRIES} попыток goto: {type(last_goto_error).__name__} - {str(last_goto_error).splitlines()[0]}"
        except Exception as e:
             last_goto_error = e
             logging.error(f"[{url}] НЕОЖИДАННАЯ ошибка при page.goto (попытка {attempt + 1}): {e}", exc_info=True)
             data['error'] = f"Неожиданная ошибка goto: {type(e).__name__} - {str(e)}"
             break

    if goto_success:
        try:
            cookie_button_selector = "#onetrust-accept-btn-handler"
            try:
                await page.locator(cookie_button_selector).click(timeout=cookie_click_timeout)
                logging.info(f"[{url}] Баннер с куки нажат.")
                await page.wait_for_timeout(1000) # Небольшая пауза после клика
            except PlaywrightTimeoutError:
                logging.info(f"[{url}] Баннер с куки не найден/кликабелен в течение {cookie_click_timeout/1000}с.")
            except Exception as e_cookie:
                logging.warning(f"[{url}] Ошибка при обработке баннера куки: {e_cookie}")

            # Ждем появления одного из ключевых блоков контента, чтобы страница "прогрузилась"
            content_selectors = "div.web-profiles, div.biographyText, div.truncatedUserDescription, a[href$='/followers']" # Добавлен селектор подписчиков
            try:
                await page.wait_for_selector(content_selectors, state='attached', timeout=content_selector_timeout) # Ждем, пока хотя бы один из них появится
                logging.info(f"[{url}] Ключевые элементы (или их часть) найдены.")
            except PlaywrightTimeoutError:
                logging.warning(f"[{url}] Ключевые элементы не загрузились в течение {content_selector_timeout/1000}с.")
                data['error'] = (data.get('error', '') + ";Ключевые элементы не найдены").strip(';')
            
            html_content = await page.content()
            parsed_specific_data = parse_soundcloud_profile_html(html_content, url)
            current_error = data.get('error', '') # Сохраняем предыдущие ошибки (если были)
            data.update(parsed_specific_data) # Обновляем data результатами парсинга
            if current_error: # Если были ошибки до парсинга, добавляем их обратно
                 data['error'] = (current_error + ";" + data.get('error', '')).strip(';')
            
            # Если парсер сам установил ошибку (например, не нашел ничего), она останется.
            # Если ошибок не было ни до, ни во время парсинга, data['error'] будет пустым.

            logging.info(f"[{url}] Успешно обработан и распарсен. Подписчики: '{data.get('followers', 'N/A')}'.")
        except Exception as e_process:
            logging.error(f"[{url}] Ошибка при обработке/парсинге страницы ПОСЛЕ goto: {e_process}", exc_info=True)
            data['error'] = (data.get('error', '') + f";Ошибка обработки/парсинга: {type(e_process).__name__}").strip(';')
    
    return data


async def playwright_tasks_for_worker(
        urls_chunk: list, 
        proxy_config: dict | None, 
        csv_filename: str, 
        csv_lock: mp.Lock,
        retry_queue: mp.Queue = None 
    ) -> int: 
    successful_count = 0
    worker_name = mp.current_process().name
    is_actually_using_proxy = bool(proxy_config) # Этот воркер использует прокси для текущего запуска

    async with async_playwright() as p:
        browser_launch_options = {"headless": True}
        if is_actually_using_proxy:
            browser_launch_options["proxy"] = proxy_config
            logging.info(f"Воркер {worker_name} (прокси: {proxy_config.get('server', 'N/A')}) запускается.")
        else:
            logging.info(f"Воркер {worker_name} (БЕЗ прокси) запускается.")
        
        browser = None
        try:
            browser = await p.chromium.launch(**browser_launch_options) 
            logging.info(f"Воркер {worker_name}: Браузер Chromium запущен.")
        except Exception as e:
            err_msg = f"Не удалось запустить браузер {'с прокси ' + proxy_config.get('server') if is_actually_using_proxy else 'без прокси'}: {e}"
            logging.error(err_msg)
            if retry_queue: # Если это воркер из пула (имеет очередь ретрая)
                 logging.warning(f"Воркер {worker_name}: Передача {len(urls_chunk)} URL в очередь ретрая (ошибка запуска браузера).")
                 for url_to_retry in urls_chunk: retry_queue.put(url_to_retry)
            return 0 # Возвращаем 0 успешных

        context = None
        try:
            context = await browser.new_context(
                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"
            )
            logging.info(f"Воркер {worker_name}: Контекст создан.")

            logging.info(f"Воркер {worker_name}: Начинаю обработку {len(urls_chunk)} URL.")
            for i, url_to_process in enumerate(urls_chunk):
                logging.info(f"Воркер {worker_name}: URL {i+1}/{len(urls_chunk)}: {url_to_process}")
                page = None
                result_data = None 
                process_error_occurred = False 
                try:
                    page = await context.new_page()
                    result_data = await process_single_url_in_worker(page, url_to_process) 
                    
                    if result_data and result_data.get('error'): 
                        process_error_occurred = True
                    elif result_data: # Успех, нет поля 'error' или оно пустое
                        successful_count += 1
                    else: # result_data is None - непредвиденная ситуация
                        process_error_occurred = True; 
                        result_data = {'url': url_to_process, 'error': 'Worker process_single_url_in_worker вернул None'}
                
                except Exception as page_err:
                    process_error_occurred = True
                    logging.error(f"[{url_to_process}] Критическая ошибка на уровне страницы/задачи в playwright_tasks_for_worker: {page_err}", exc_info=True)
                    if result_data is None: result_data = {'url': url_to_process, 'error': ''} # Инициализируем, если еще нет
                    result_data['error'] = (result_data.get('error', '') + f";Крит. ошибка page/task: {str(page_err)}").strip(';')
                finally:
                    if page and not page.is_closed():
                         try: await page.close()
                         except Exception as e: logging.warning(f"[{url_to_process}] Ошибка при закрытии страницы: {e}")

                # --- Логика записи и ретрая ---
                if result_data:
                    if not process_error_occurred: # Успех -> пишем в CSV
                        append_to_csv(result_data, csv_filename, DEFAULT_CSV_FIELDNAMES, csv_lock)
                    # Ошибка И этому воркеру была передана очередь ретрая (значит, он из пула)
                    elif retry_queue: 
                        log_msg_proxy_status = "с прокси" if is_actually_using_proxy else "без прокси (в пуле)"
                        logging.info(f"[{url_to_process}] Ошибка в воркере пула ({log_msg_proxy_status}), добавление в очередь ретрая. Ошибка: {result_data.get('error')}")
                        retry_queue.put(url_to_process)
                    else: # Ошибка, но это основной прямой воркер (нет retry_queue) или его ретрай-попытка
                         logging.warning(f"[{url_to_process}] Ошибка (основной прямой воркер или его ретрай), результат не записывается, в очередь не добавляется: {result_data.get('error')}")
                         # Не пишем в CSV, так как это ошибка, и она не для ретрая через этот механизм
                
                if i < len(urls_chunk) - 1: # Пауза между URL в чанке
                    delay = random.uniform(1, 3) if is_actually_using_proxy else random.uniform(0.5, 1.5)
                    logging.info(f"[{url_to_process}] Пауза {delay:.2f} сек перед следующим URL в чанке...")
                    await asyncio.sleep(delay)

            logging.info(f"Воркер {worker_name}: Обработка чанка из {len(urls_chunk)} URL завершена. Успешно: {successful_count}.")

        except Exception as context_err:
             logging.error(f"Воркер {worker_name}: Ошибка на уровне контекста браузера: {context_err}", exc_info=True)
             if retry_queue: # Если это воркер из пула
                 logging.warning(f"Воркер {worker_name}: Передача {len(urls_chunk)} URL в очередь ретрая (ошибка контекста).")
                 for url_to_retry in urls_chunk: retry_queue.put(url_to_retry)
        finally:
            if context: 
                 try: await context.close()
                 except Exception as e: logging.warning(f"Воркер {worker_name}: Ошибка при закрытии контекста: {e}")
            if browser: 
                 try: await browser.close()
                 except Exception as e: logging.warning(f"Воркер {worker_name}: Ошибка при закрытии браузера: {e}")
            logging.info(f"Воркер {worker_name}: Все ресурсы Playwright освобождены.")
            
    return successful_count


def run_worker_task(
        urls_chunk: list, 
        proxy_string: str | None, 
        csv_filename: str, 
        csv_lock: mp.Lock,
        retry_queue: mp.Queue = None 
    ) -> int:
    process_name = mp.current_process().name
    proxy_cfg = parse_proxy_string(proxy_string)
    successful_count = 0
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        successful_count = loop.run_until_complete(
            playwright_tasks_for_worker(urls_chunk, proxy_cfg, csv_filename, csv_lock, retry_queue) 
        )
    except Exception as e:
        logging.error(f"Критическая ошибка в цикле событий воркера {process_name} (run_worker_task): {e}", exc_info=True)
        if retry_queue: # Если этому воркеру была передана очередь (т.е. он из пула)
            logging.error(f"Воркер {process_name}: Критическая ошибка в run_worker_task, передача {len(urls_chunk)} URL в очередь ретрая.")
            for url_to_retry in urls_chunk: retry_queue.put(url_to_retry)
    finally:
        try:
            if not loop.is_closed():
                # Завершаем все активные задачи в цикле
                tasks = asyncio.all_tasks(loop=loop)
                for task in tasks:
                    if not task.done(): task.cancel()
                # Даем возможность задачам завершиться после отмены
                if tasks: 
                    # Оборачиваем gather в try-except, т.к. отмененные задачи могут вызвать CancelledError
                    try:
                        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                    except asyncio.CancelledError:
                        pass # Ожидаемо для отмененных задач
                loop.run_until_complete(asyncio.sleep(0.1)) # Небольшая пауза для очистки
                loop.close()
        except Exception as loop_close_err:
            logging.error(f"Воркер {process_name}: Ошибка при закрытии цикла событий: {loop_close_err}")
    return successful_count