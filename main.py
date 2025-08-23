import os
import sys
import requests

# Путь к интерпретатору
python_path = os.path.dirname(sys.executable)
# Версия python
python_version = sys.version


def main():
    return requests.get(url="https://google.com")


print("....")
print(f"Путь к интерпретатору. {python_path}.. \nPython Версия. {python_version}")

if __name__ == "__main__":
    print(main())
