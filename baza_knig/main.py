import requests
from bs4 import BeautifulSoup
import pandas as pd
import gspread
import re
from datetime import datetime

cookies = {
    "PHPSESSID": "6b0pk15p1i0nd3fumnlc49f8br",
    "_ga_04BJ8LCPGH": "deleted",
    "_ga": "GA1.1.1871535938.1755674660",
    "fid": "d69d87d6-fa85-44bd-bc7e-15b5531d1edd",
    "_ym_d": "1755674662",
    "_ym_uid": "1755674662823662447",
    "_ym_isad": "1",
    "_ac_oid": "f496ab89b7e3accc91562ec83d0c766f%3A1755896256103",
    "_ym_visorc": "b",
    "_ga_04BJ8LCPGH": "GS2.1.s1755949410$o11$g1$t1755951154$j60$l0$h0",
}

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "ru,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Referer": "https://baza-knig.top/fantastika-fenteze/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    # 'Cookie': 'PHPSESSID=6b0pk15p1i0nd3fumnlc49f8br; _ga_04BJ8LCPGH=deleted; _ga=GA1.1.1871535938.1755674660; fid=d69d87d6-fa85-44bd-bc7e-15b5531d1edd; _ym_d=1755674662; _ym_uid=1755674662823662447; _ym_isad=1; _ac_oid=f496ab89b7e3accc91562ec83d0c766f%3A1755896256103; _ym_visorc=b; _ga_04BJ8LCPGH=GS2.1.s1755949410$o11$g1$t1755951154$j60$l0$h0',
}


def main():
    page = 1
    max_page = 2
    data = []

    while page <= max_page:
        link = f"https://baza-knig.top/fantastika-fenteze/page/{page}/"

        respons = requests.get(link, headers=headers)
        respons.encoding = "utf-8"  # устанввливаем кодировку.

        soup = BeautifulSoup(respons.text, "lxml")
        content = soup.find("div", id="dle-content")

        for short in content.find_all("div", class_="short"):
            book_data = {
                "Название": " - ",
                "Автор": " - ",
                "Читает": " - ",
                "Длительность": " - ",
                "Цикл": " - ",
                "Жанр": " - ",
                "Добавлена": " - ",
                "likes": None,
                "dis": None,
            }

            # Извлекаем заголовок книги и названия
            title_bock = short.find("div", class_="short-title")
            if title_bock:
                book_data["Название"] = title_bock.get_text(strip=True)

            # Извлекаем Автор.Чтитает.Жанр....
            info_ul = short.find("ul", class_="reset short-items")
            if info_ul:
                for li in info_ul.find_all("li"):
                    # Делим по первому вхождению ":
                    parts = li.get_text(strip=True).split(":", 1)
                    if len(parts) == 2:
                        key, value = parts
                        book_data[key] = value

            # Извлекаем likes dislikes И обновляем словарь
            bottom = short.find("div", class_="short-bottom")
            btn = []
            for item in bottom.find_all("div", class_="comments"):
                btn.append(item.get_text(strip=True))
            book_data["likes"] = btn[0]
            book_data["dis"] = btn[1]

            data.append(book_data)

        page += 1

    # # Удаление ненужныз символов через re.sub.
    patterns = {r"Фантастика": "Фан", r"фэнтези": "фэн", r",": "/", r" ": ""}

    for item in data:
        elem = item["Жанр"]
        for pattern, replacement in patterns.items():
            elem = re.sub(pattern, replacement, elem, flags=re.IGNORECASE)
        item["Жанр"] = elem

    # pandas таблица
    df = pd.DataFrame(data).fillna(" - ")
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df


if __name__ == "__main__":
    df = main()

    # Подключение service_account
    gc = gspread.service_account(filename="config_key.json")
    wks = gc.open("web_parsing_baza_knig").sheet1
    wks.update([df.columns.values.tolist()] + df.values.tolist())

    # Update a range of cells using the top left corner address
    # wks.update([[1, 2], [3, 4]], 'A1')

    # Or update a single cell
    # wks.update_acell('B42', "it's down there somewhere, let me take another look.")

    # Format the header
    # wks.format('A1:B1', {'textFormat': {'bold': True}})
