import os
import requests
import zipfile
import shutil
import urllib3
import time
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

YEARS = [str(i) for i in range(2010, 2022)]

BASE_URL = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{}.zip"

DATA_DIR = "./data"

os.makedirs(DATA_DIR, exist_ok=True)


def download_file(url, path, retries=5):
    for attempt in range(retries):
        try:
            print(f"Downloading {url} (attempt {attempt+1})")

            with requests.get(url, stream=True, timeout=60, verify=False) as r:
                r.raise_for_status()

                with open(path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                        if chunk:
                            f.write(chunk)

            print("Download complete ✔")
            return

        except Exception as e:
            print(f"Failed attempt {attempt+1}: {e}")
            time.sleep(5)

    raise Exception("Download failed after multiple retries")


for year in YEARS:
    zip_path = os.path.join(DATA_DIR, f"{year}.zip")
    extract_path = os.path.join(DATA_DIR, year)

    url = BASE_URL.format(year)

    # 1. Download
    if os.path.exists(zip_path):
        print(f"{zip_path} already exists, skipping download")
        continue

    download_file(url, zip_path)

    # 2. Extract
    print(f"Extracting {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    # 3. Find CSV file
    csv_file = None
    for root, dirs, files in os.walk(extract_path):
        for file in files:
            if file.lower().endswith(".csv"):
                csv_file = os.path.join(root, file)
                break

    # 4. Move CSV to main folder
    if csv_file:
        final_path = os.path.join(DATA_DIR, f"{year}.csv")
        shutil.move(csv_file, final_path)
        print(f"Saved: {final_path}")

    # 5. Cleanup
    shutil.rmtree(extract_path)
    os.remove(zip_path)

print("DONE ✔")
