import time
import json
import requests
from datetime import datetime, timezone
from clickhouse_connect import get_client

URL = "http://api.open-notify.org/astros.json"
MAX_ATTEMPTS = 5
BASE_DELAY = 1  # сек


def fetch_with_retry(url):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=10)
            print(f"Попытка {attempt}: статус {resp.status_code}")

            # Успех
            if resp.status_code == 200:
                return resp.json()

            # 429 Retry-After, иначе экспоненциальная задержка
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait = int(retry_after)
                else:
                    wait = BASE_DELAY * (2 ** (attempt - 1))
                print(f"429. Ждём {wait} сек...")
                time.sleep(wait)
                continue

            # 5xx — временные проблемы на сервере, пробуем ещё раз
            if 500 <= resp.status_code < 600:
                wait = BASE_DELAY * (2 ** (attempt - 1))
                print(f"5xx. Ждём {wait} сек...")
                time.sleep(wait)
                continue

            # Прочие 4xx считаем фатальными
            resp.raise_for_status()

        except requests.RequestException as e:
            # Сетевые ошибки (таймаут, соединение) — ретрай
            wait = BASE_DELAY * (2 ** (attempt - 1))
            print(f"Сетевая ошибка: {e}. Ждём {wait} сек...")
            time.sleep(wait)

    raise RuntimeError(f"Не удалось скачать данные после {MAX_ATTEMPTS} попыток")


def main():
    data = fetch_with_retry(URL)
    json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    client = get_client(host="localhost", port=8123, username="default", password="", database="space")
    inserted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    client.insert(
        "space.raw_data",
        [[json_str, inserted_at]],
        column_names=["raw_json", "_inserted_at"]
    )
    print("OK: данные вставлены в space.raw_data")


if __name__ == "__main__":
    main()
