import requests
from bs4 import BeautifulSoup
import lxml
import pandas as pd
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


# url = requests.get('https://baza-knig.top/fantastika-fenteze/')
# print(url)

def main():
    page = 1
    max_page = 3
    data = []
    
    while page <= max_page:
        
        link = (f"https://baza-knig.top/fantastika-fenteze/page/{page}/")

        respons = requests.get(link, headers=headers)
        respons.encoding = "utf-8"  # устанввливаем кодировку.

        soup = BeautifulSoup(respons.text, "lxml")
        block = soup.find("div", id="dle-content")

        # Находим все div блоке с короткой информ на странице..
        books = block.find_all("div", class_="short")

        
        for book in books:
            book_data = {}
            # Извлекаем заголовок книги и названия
            title = book.find("div", class_="short-title").text.strip()
            book_data["Название"] = title
            
            for li in book.find("ul", class_="reset short-items").find_all("li"):
                parts = li.text.strip().split(":", 1)  # Делим по первому вхождению ":
                if len(parts) == 2:
                    key, value = parts
                    book_data[key] = value
            data.append(book_data)
        page += 1
        
    # # Удаление ненужныз символов.
    patterns = {r"Фантастика": "Фан", r"фэнтези": "фэн", r",": "/", r" ": ""}

    for item in data:
        genre = item["Жанр"]
        for pattern, replacement in patterns.items():
            genre = re.sub(pattern, replacement, genre, flags=re.IGNORECASE)
        item["Жанр"] = genre
    
    # pandas 
    df = pd.DataFrame(data)
    df["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return len(data)


if __name__ == "__main__":
    print(main())
