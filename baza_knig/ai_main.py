import requests
from bs4 import BeautifulSoup
import lxml  # noqa: F401  # lxml парсер используется BeautifulSoup
import pandas as pd
import gspread
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# ====== СЕТЕВЫЕ НАСТРОЙКИ (НЕ ХАРДКОДИТЕ ЧУВСТВИТЕЛЬНОЕ) ======
COOKIES = {
    # Если cookies реально нужны сайту — передайте сюда.
    # Лучше загружать из .env / конфигов, а не хранить в коде.
}
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Referer": "https://baza-knig.top/fantastika-fenteze/",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
}


# ============================ ПАРСИНГ ============================

def normalize_str(value: object) -> str:
    """
    Приводит произвольное значение к «сравнимому» строковому виду:
    - преобразует в str,
    - тримит,
    - схлопывает повторяющиеся пробелы,
    - приводит к нижнему регистру.

    Это критично для корректного сравнения значений между источниками
    (интернет/Google Sheets), чтобы различия в регистре и пробелах
    не считались «изменениями».

    :param value: Любое значение (str, int, float, None)
    :return: Нормализованная строка.
    """
    s = "" if value is None else str(value)
    s = re.sub(r"\s+", " ", s).strip()
    return s.casefold()


def sanitize_genre(genre: str) -> str:
    """
    Нормализует поле «Жанр» по заданным паттернам:
    - пример: «Фантастика» -> «Фан», «фэнтези» -> «фэн»,
      замена запятых на '/', удаление лишних пробелов и т.п.

    ВНИМАНИЕ: Набор правил — пример. Подправляйте под свою задачу.

    :param genre: Исходная строка жанра.
    :return: Нормализованная строка жанра.
    """
    if not genre:
        return genre
    patterns = {r"Фантастика": "Фан", r"фэнтези": "фэн", r",": "/", r"\s+": ""}
    result = genre
    for pattern, repl in patterns.items():
        result = re.sub(pattern, repl, result, flags=re.IGNORECASE)
    return result


def extract_likes_dislikes(text_block: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Извлекает лайки/дизлайки из текстового блока.

    Так как точная разметка сайта неизвестна, применяем эвристику:
    - Ищем все целые числа в тексте.
    - Если одно число — трактуем как лайки (или общий рейтинг),
      дизлайки считаем None.
    - Если два и более — первые два трактуем как (likes, dislikes).
    - Если чисел нет — возвращаем (None, None).

    Рекомендация: при знании конкретных CSS-классов заменить на точечные селекторы.

    :param text_block: Текстовый фрагмент, содержащий метрики рейтинга.
    :return: (likes, dislikes), где значения — int или None.
    """
    nums = re.findall(r"-?\d+", text_block or "")
    if not nums:
        return None, None
    if len(nums) == 1:
        return int(nums[0]), None
    return int(nums[0]), int(nums[1])


def fetch_books_page(session: requests.Session, page: int) -> List[Dict]:
    """
    Парсит одну страницу каталога с книгами и возвращает список словарей с полями:
    - 'Название', 'Автор', 'Читает', 'Жанр', 'likes', 'dis'
    (часть полей может отсутствовать, тогда проставляются ' - ')

    МАКСИМАЛЬНАЯ Защита от падений:
    - Проверки существования блоков.
    - Таймауты и контроль статуса ответа.
    - «Мягкие» пропуски при ошибках в структуре.

    :param session: requests.Session с заданными заголовками/куками.
    :param page: Номер страницы (1..N).
    :return: Список словарей-строк каталога.
    """
    url = f"https://baza-knig.top/fantastika-fenteze/page/{page}/"
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    soup = BeautifulSoup(resp.text, "lxml")
    content = soup.find("div", id="dle-content")
    if not content:
        return []

    rows = []
    for short in content.find_all("div", class_="short"):
        row = {"Название": " - ", "Автор": " - ", "Читает": " - ", "Жанр": " - ", "likes": None, "dis": None}

        title_block = short.find("div", class_="short-title")
        if title_block:
            row["Название"] = title_block.get_text(strip=True)

        info_ul = short.find("ul", class_="reset short-items")
        if info_ul:
            for li in info_ul.find_all("li"):
                parts = li.get_text(strip=True).split(":", 1)
                if len(parts) == 2:
                    key, val = parts[0].strip(), parts[1].strip()
                    if key in ("Автор", "Читает", "Жанр"):
                        row[key] = val

        bottom = short.find("div", class_="short-bottom")
        if bottom:
            # На странице может быть один/несколько блоков с комментами/рейтингом,
            # соберём их тексты и попробуем извлечь числа.
            texts = [b.get_text(" ", strip=True) for b in bottom.find_all("div", class_="comments")]
            joined = " | ".join(texts)
            likes, dislikes = extract_likes_dislikes(joined)
            row["likes"] = likes
            row["dis"] = dislikes

        # Нормализуем жанр под твои правила:
        row["Жанр"] = sanitize_genre(row.get("Жанр") or " - ")

        rows.append(row)

    return rows


def fetch_books(max_pages: int = 1) -> pd.DataFrame:
    """
    Собирает данные по страницам каталога, агрегирует в pandas.DataFrame и добавляет timestamp.

    :param max_pages: Сколько страниц парсить. По умолчанию 1 (безопасно).
    :return: DataFrame с полями ['Название','Автор','Читает','Жанр','likes','dis','timestamp']
    """
    data: List[Dict] = []
    with requests.Session() as s:
        s.headers.update(HEADERS)
        if COOKIES:
            s.cookies.update(COOKIES)

        for page in range(1, max_pages + 1):
            try:
                page_rows = fetch_books_page(s, page)
                if not page_rows and page == 1:
                    # Если «пусто» уже на первой — вероятно, поменялась разметка/URL/доступ
                    break
                data.extend(page_rows)
            except Exception as e:
                # Логируйте e, при необходимости — ретраи
                # Здесь просто продолжаем.
                continue

    df = pd.DataFrame(data).fillna(" - ")
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Гарантируем порядок колонок:
    cols = ["Название", "Автор", "Читает", "Жанр", "likes", "dis", "timestamp"]
    for c in cols:
        if c not in df.columns:
            df[c] = " - "
    return df[cols]


# ============================ GSPREAD ============================

def get_gspread_client(sa_path: str = "config_gkey.json") -> gspread.Client:
    """
    Возвращает авторизованный gspread.Client через service account.

    ВАЖНО:
    - Файл ключа сервисного аккаунта (*.json) должен быть на диске.
    - Таблица Google должна быть расшарена на email сервисного аккаунта (Editor).

    :param sa_path: Путь до JSON ключа сервисного аккаунта.
    :return: gspread.Client
    """
    return gspread.service_account(filename=sa_path)


def open_spreadsheet(gc: gspread.Client, spreadsheet_title: str):
    """
    Открывает Google Spreadsheet по имени. Бросает исключение, если нет доступа/не найдено.

    :param gc: Авторизованный gspread.Client
    :param spreadsheet_title: Имя таблицы (как в интерфейсе Google Sheets)
    :return: gspread.Spreadsheet
    """
    return gc.open(spreadsheet_title)


def ensure_worksheet(ss, title: str, rows: int = 2000, cols: int = 20):
    """
    Возвращает worksheet по имени. Если нет — создаёт.

    :param ss: gspread.Spreadsheet
    :param title: имя листа
    :param rows: кол-во строк при создании
    :param cols: кол-во столбцов при создании
    :return: gspread.Worksheet
    """
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def write_df_to_sheet(ws, df: pd.DataFrame, clear: bool = True):
    """
    Пишет DataFrame на лист Google Sheets:
    - Опционально очищает лист,
    - Записывает шапку (имена колонок),
    - Записывает данные блоком (list-of-lists).

    :param ws: gspread.Worksheet
    :param df: pandas.DataFrame
    :param clear: Очистить лист перед записью
    """
    if clear:
        ws.clear()
    if df.empty:
        ws.update([["EMPTY"]])
        return

    values = [df.columns.tolist()] + df.astype(object).where(pd.notnull(df), "").values.tolist()
    ws.update(values)


def read_sheet_to_df(ws, header_row: int = 1) -> pd.DataFrame:
    """
    Считывает данные с указанного листа Google Sheets в pandas.DataFrame.

    Особенности:
    - Предполагается, что первая строка (header_row) — заголовок.
    - Пустые значения приводятся к пустым строкам.
    - Типы не форсируются специально, так как на листе часто всё в виде строк;
      при необходимости дальше приводите столбцы отдельно.

    :param ws: gspread.Worksheet
    :param header_row: Номер строки с заголовками (1-индексация).
    :return: pandas.DataFrame
    """
    all_values = ws.get_all_values()
    if not all_values:
        return pd.DataFrame()

    header_idx = header_row - 1
    if header_idx >= len(all_values):
        return pd.DataFrame()

    header = all_values[header_idx]
    data = all_values[header_idx + 1 :]
    df = pd.DataFrame(data, columns=header)
    # Нормализуем возможные незаполненные столбцы
    return df.fillna("")


# ============================ СРАВНЕНИЕ ============================

def build_key(df: pd.DataFrame, key_cols: List[str]) -> pd.Series:
    """
    Строит «сквозной ключ» для сопоставления записей из разных источников.
    Ключ = конкатенация нормализованных значений из key_cols через ' | '.

    :param df: DataFrame с данными
    :param key_cols: Колонки, образующие ключ (например, ['Название','Автор'])
    :return: pd.Series строковых ключей
    """
    parts = []
    for col in key_cols:
        if col not in df.columns:
            df[col] = ""
        parts.append(df[col].map(normalize_str))
    return pd.Series([" | ".join(vals) for vals in zip(*parts)], index=df.index)


def compare_web_vs_sheet(
    web_df: pd.DataFrame,
    sheet_df: pd.DataFrame,
    key_cols: List[str],
    compare_cols: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Сравнивает данные из веба (web_df) с данными, загруженными из Google Sheets (sheet_df).

    Логика:
    1) «added» — записи, которых нет в GS, но есть в вебе.
    2) «removed» — записи, которые были в GS, но не найдены в вебе.
    3) «changed» — записи, у которых совпал ключ, но значения в compare_cols различаются.

    Нормализация:
    - Для сравнения используем normalize_str, чтобы не считать различиями пробелы/регистр.

    :param web_df: DataFrame из парсинга
    :param sheet_df: DataFrame, полученный из Google Sheets
    :param key_cols: ['Название','Автор'] — рекомендуемая связка
    :param compare_cols: Явный список колонок для сравнения. Если None —
                         используем пересечение столбцов (кроме timestamp/служебных).
    :return: dict с DataFrame: {'added','removed','changed'}
    """
    w = web_df.copy()
    s = sheet_df.copy()

    # Построим ключи
    w["_key"] = build_key(w, key_cols)
    s["_key"] = build_key(s, key_cols)

    # Определим сравниваемые колонки
    if compare_cols is None:
        ignore = {"timestamp", "_key"}
        compare_cols = [c for c in set(w.columns).intersection(set(s.columns)) if c not in ignore]

    # added/removed
    added = w[~w["_key"].isin(s["_key"])].copy()
    removed = s[~s["_key"].isin(w["_key"])].copy()

    # changed: merge по ключу, затем сравнение по compare_cols
    merged = s[["_key"] + compare_cols].merge(
        w[["_key"] + compare_cols], on="_key", how="inner", suffixes=("_gs", "_web")
    )

    def row_changed(row) -> bool:
        for c in compare_cols:
            if normalize_str(row[f"{c}_gs"]) != normalize_str(row[f"{c}_web"]):
                return True
        return False

    changed_mask = merged.apply(row_changed, axis=1)
    changed_rows = merged[changed_mask].copy()

    # Для удобства покажем, какие столбцы различаются:
    diff_cols = []
    for c in compare_cols:
        diff = normalize_str(merged[f"{c}_gs"]) != normalize_str(merged[f"{c}_web"])
        diff_cols.append(diff.rename(f"diff_{c}"))
    if diff_cols:
        diff_flags = pd.concat(diff_cols, axis=1)
        changed_rows = pd.concat([changed_rows, diff_flags.loc[changed_rows.index]], axis=1)

    # Возвращаем аккуратные сеты
    return {
        "added": added.drop(columns=["_key"], errors="ignore"),
        "removed": removed.drop(columns=["_key"], errors="ignore"),
        "changed": changed_rows,  # оставляем с парами *_gs/*_web и флагами diff_*
    }


def save_diffs_to_sheets(ss, diffs: Dict[str, pd.DataFrame]):
    """
    Сохраняет результаты сравнения на отдельные листы:
    - 'diff_added', 'diff_removed', 'diff_changed'

    :param ss: gspread.Spreadsheet
    :param diffs: словарь {'added','removed','changed'}
    """
    for name, df in [("diff_added", diffs["added"]), ("diff_removed", diffs["removed"]), ("diff_changed", diffs["changed"])]:
        ws = ensure_worksheet(ss, name, rows=max(len(df) + 10, 50), cols=max(len(df.columns) + 2, 10))
        write_df_to_sheet(ws, df, clear=True)


# ============================ MAIN ============================

def main(
    max_pages: int = 1,
    sa_path: str = "config_gkey.json",
    spreadsheet_title: str = "web_parsing_baza_knig",
    target_sheet_for_web: str = "web_data",
    source_sheet_for_compare: str = "sheet_source",
    key_cols: Optional[List[str]] = None,
):
    """
    Главная «склейка»:
    1) Парсинг сайта (N страниц) -> web_df.
    2) Подключение к Google Sheets.
    3) Запись web_df на отдельный лист (target_sheet_for_web).
    4) Чтение данных из листа-источника (source_sheet_for_compare) — это «эталон» для сравнения.
       Если такого листа нет — он будет создан пустым.
    5) Сравнение web_df vs sheet_df (по умолчанию ключ = ['Название','Автор']).
    6) Выгрузка результатов сравнения на листы: diff_added / diff_removed / diff_changed.

    :param max_pages: Сколько страниц парсить с сайта.
    :param sa_path: Путь к JSON ключу сервисного аккаунта Google.
    :param spreadsheet_title: Название Google Spreadsheet.
    :param target_sheet_for_web: Лист, куда кладём свежие данные из веба.
    :param source_sheet_for_compare: Лист, откуда читаем данные для сравнения (может быть тем же).
    :param key_cols: Ключевые колонки для сопоставления (по умолчанию ['Название','Автор']).
    """
    if key_cols is None:
        key_cols = ["Название", "Автор"]

    # 1) Парсинг
    web_df = fetch_books(max_pages=max_pages)

    # 2) Google Sheets
    gc = get_gspread_client(sa_path)
    ss = open_spreadsheet(gc, spreadsheet_title)

    # 3) Пишем WEB данные на свой лист (для истории/аудита)
    ws_web = ensure_worksheet(ss, target_sheet_for_web, rows=max(len(web_df) + 10, 200), cols=max(len(web_df.columns) + 2, 10))
    write_df_to_sheet(ws_web, web_df, clear=True)

    # 4) Читаем исходник для сравнения
    ws_src = ensure_worksheet(ss, source_sheet_for_compare, rows=500, cols=20)
    sheet_df = read_sheet_to_df(ws_src, header_row=1)

    # Если источник пуст — аккуратно обработаем
    if sheet_df.empty:
        # Можно считать, что всё «added»
        diffs = {"added": web_df.copy(), "removed": pd.DataFrame(), "changed": pd.DataFrame()}
    else:
        # 5) Сравнение
        diffs = compare_web_vs_sheet(web_df, sheet_df, key_cols=key_cols, compare_cols=None)

    # 6) Сохраняем диффы
    save_diffs_to_sheets(ss, diffs)

    return web_df, diffs


if __name__ == "__main__":
    # Пример запуска:
    #   - парсим первые 2 страницы,
    #   - пишем веб-данные в лист 'web_data',
    #   - сравниваем с листом 'sheet_source'
    web_df, diffs = main(
        max_pages=3,
        sa_path="config_gkey.json",
        spreadsheet_title="web_parsing_baza_knig",
        target_sheet_for_web="web_data",
        source_sheet_for_compare="sheet_source",  # положи сюда «эталон»/прошлую версию
        key_cols=["Название", "Автор"],
    )
