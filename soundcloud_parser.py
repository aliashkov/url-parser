# soundcloud_parser.py
import re
import logging # Added for debug logging
from urllib.parse import urlparse, parse_qs, unquote
from bs4 import BeautifulSoup

def extract_url_from_gate_sc(gate_url: str) -> str:
    """Извлекает оригинальный URL из gate.sc ссылки."""
    if not gate_url:
        return ''
    if 'gate.sc?url=' in gate_url:
        parsed = urlparse(gate_url)
        query_params = parse_qs(parsed.query)
        target_url_encoded = query_params.get('url', [None])[0]
        if target_url_encoded:
            return unquote(target_url_encoded)
        return gate_url # Если 'url' нет, но есть gate.sc
    return gate_url

def parse_follower_count_to_int_str(text: str) -> str:
    """Пытается преобразовать текстовое представление количества подписчиков (напр. "90.2K", "1,234") в строку с целым числом."""
    if not text: return ''
    original_text = text
    text = text.lower().strip().replace(',', '') # "1,234" -> "1234", "90.2k" -> "90.2k"
    
    value = None
    try:
        if text.endswith('k'):
            value = float(text[:-1]) * 1000
        elif text.endswith('m'):
            value = float(text[:-1]) * 1_000_000
        else:
            value = float(text)
        return str(int(value))
    except ValueError:
        # logging.debug(f"Could not parse follower count from text: '{original_text}'")
        return '' # Возвращаем пустую строку, если не удалось распарсить

def parse_soundcloud_profile_html(html_content: str, profile_url: str) -> dict:
    """
    Парсит HTML-контент страницы профиля SoundCloud для извлечения ссылок и количества подписчиков.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {
        'url': profile_url, 'website': '', 'youtube': '', 'facebook': '', 'twitter': '',
        'instagram': '', 'songkick': '', 'telegram': '', 'tiktok': '', 'linkedin': '',
        'emails': [], 'followers': '', 'error': '' # Поле error будет заполняться в воркере
    }

    # 1. Извлечение ссылок из блока .web-profiles
    web_profiles_div = soup.select_one('div.web-profiles')
    if web_profiles_div:
        links = web_profiles_div.find_all('a', href=True)
        for link_tag in links:
            href = link_tag.get('href', '')
            original_url = extract_url_from_gate_sc(href)
            text_content = link_tag.get_text(strip=True).lower()

            if not original_url:
                continue

            if 'mailto:' in original_url:
                email = original_url.replace('mailto:', '')
                if email not in data['emails']:
                    data['emails'].append(email)
            elif any(domain in original_url for domain in ['instagram.com']):
                if not data['instagram']: data['instagram'] = original_url
            elif any(domain in original_url for domain in ['youtube.com', 'youtu.be']):
                if not data['youtube']: data['youtube'] = original_url
            elif any(domain in original_url for domain in ['facebook.com', 'fb.me']):
                if not data['facebook']: data['facebook'] = original_url
            elif any(domain in original_url for domain in ['twitter.com', 'x.com']):
                if not data['twitter']: data['twitter'] = original_url
            elif any(domain in original_url for domain in ['songkick.com']):
                if not data['songkick']: data['songkick'] = original_url
            elif any(domain in original_url for domain in ['t.me', 'telegram.me']):
                if not data['telegram']: data['telegram'] = original_url
            elif any(domain in original_url for domain in ['tiktok.com']):
                if not data['tiktok']: data['tiktok'] = original_url
            elif any(domain in original_url for domain in ['linkedin.com']):
                if not data['linkedin']: data['linkedin'] = original_url
            elif (original_url.startswith('http') and
                  not any(social_domain in original_url for social_domain in [
                      'instagram', 'youtube', 'facebook', 'twitter', 'songkick', 't.me', 'tiktok', 'linkedin',
                      'soundcloud.com', 'spotify.com', 'apple.com', 'bandcamp.com' # Добавил еще несколько, чтобы не считались за основной сайт
                  ])):
                if not data['website']:
                    data['website'] = original_url
                elif text_content == 'website' and not data['website']: # Если есть явная подпись "website"
                     data['website'] = original_url

    # 2. Извлечение email из описания (биографии)
    biography_text_element = soup.select_one('div.biographyText p, div.truncatedUserDescription q')
    if biography_text_element:
        biography_text = biography_text_element.get_text(separator=' ')
        for mailto_link in biography_text_element.find_all('a', href=re.compile(r'^mailto:')):
            email = mailto_link['href'].replace('mailto:', '')
            if email not in data['emails']:
                data['emails'].append(email)
        found_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', biography_text)
        for email in found_emails:
            if email not in data['emails']:
                data['emails'].append(email)
    
    data['emails'] = list(set(data['emails'])) # Убираем дубликаты

    # 3. Извлечение количества подписчиков
    followers_count_text = ''
    # Вариант 1: <a href=".../followers" ...><span data-testid="value">...</span></a>
    followers_stat_link = soup.find('a', href=lambda x: isinstance(x, str) and x.endswith('/followers'))
    if followers_stat_link:
        count_span = followers_stat_link.find('span', {'data-testid': 'value'})
        if count_span:
            followers_count_text = count_span.get_text(strip=True)
        else:
            # Вариант 2: <a href=".../followers" ...><meta itemprop="interactionCount" content="..."/></a>
            meta_tag = followers_stat_link.find('meta', itemprop='interactionCount')
            if meta_tag and meta_tag.get('content'):
                followers_count_text = meta_tag['content']
            else:
                # Вариант 3: Прямо в тексте ссылки, если другие не найдены
                # Пример: <a title="123,456 Followers" href="/username/followers">
                # Или просто текст внутри ссылки вида "100K followers"
                raw_link_text = followers_stat_link.get_text(strip=True)
                match = re.search(r'([\d\.,]+[kKmM]?)', raw_link_text) # Ищем числа с K/M
                if match:
                    followers_count_text = match.group(1)
                elif followers_stat_link.get('title') and "followers" in followers_stat_link.get('title','').lower():
                     title_match = re.search(r'([\d\.,]+[kKmM]?)', followers_stat_link.get('title',''))
                     if title_match:
                         followers_count_text = title_match.group(1)


    # Вариант 4 (более общий, если предыдущие не сработали): Ищем <meta property="soundcloud:follower_count" content="...">
    if not followers_count_text:
        meta_follower_tag = soup.find('meta', attrs={'property': 'soundcloud:follower_count', 'content': True})
        if meta_follower_tag:
            followers_count_text = meta_follower_tag['content']
            
    data['followers'] = parse_follower_count_to_int_str(followers_count_text)
    
    return data

if __name__ == '__main__':
    # Пример для тестирования парсера
    # Создайте dummy_profile.html с HTML-кодом страницы SoundCloud
    # try:
    #     with open("dummy_profile.html", "r", encoding="utf-8") as f:
    #         test_html = f.read()
    #     parsed_info = parse_soundcloud_profile_html(test_html, "http://example.com/testprofile")
    #     print("Результат парсинга:")
    #     for key, value in parsed_info.items():
    #         print(f"  {key}: {value}")
    # except FileNotFoundError:
    #     print("Файл dummy_profile.html не найден для теста.")
    
    # Тест parse_follower_count_to_int_str
    # print(f"'90.2K' -> '{parse_follower_count_to_int_str('90.2K')}'") # Ожидаем 90200
    # print(f"'1,234' -> '{parse_follower_count_to_int_str('1,234')}'") # Ожидаем 1234
    # print(f"'5M' -> '{parse_follower_count_to_int_str('5M')}'")     # Ожидаем 5000000
    # print(f"'123' -> '{parse_follower_count_to_int_str('123')}'")   # Ожидаем 123
    # print(f"'' -> '{parse_follower_count_to_int_str('')}'")       # Ожидаем ''
    # print(f"'abc' -> '{parse_follower_count_to_int_str('abc')}'")   # Ожидаем ''
    pass