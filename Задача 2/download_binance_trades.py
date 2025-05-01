import argparse
import os
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import zipfile
import io

# URL de base
BASE_URL = "https://data.binance.vision/data/spot/daily/trades"

# Colonnes correctes à forcer
COLUMNS = ["trade_id", "price", "quantity", "quote_qty", "timestamp", "is_buyer_maker", "is_best_match"]

# Fonction pour télécharger un fichier zip
def download_file(pair: str, date: str, save_folder: str):
    url = f"{BASE_URL}/{pair.upper()}/{pair.upper()}-trades-{date}.zip"
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Succès: {url}")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith(".csv"):
                    with z.open(filename) as csvfile:
                        # Lire SANS header et appliquer manuellement les bons noms de colonnes
                        df = pd.read_csv(csvfile, header=None, names=COLUMNS)
                        table = pa.Table.from_pandas(df)
                        os.makedirs(save_folder, exist_ok=True)
                        pq.write_table(table, os.path.join(save_folder, f"{pair.lower()}-{date}.parquet"))
    else:
        print(f"Erreur ({response.status_code}): {url}")

# Fonction pour télécharger tous les fichiers pour une période donnée
def download_all(pair: str, threads: int, start_date: str, end_date: str, save_folder: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)]

    with ThreadPoolExecutor(max_workers=threads) as executor:
        for date in dates:
            executor.submit(download_file, pair, date, save_folder)

# Point d'entrée principal
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance Tick Data Downloader")
    parser.add_argument("pair", type=str, help="Trading pair, e.g., btcusdt")
    parser.add_argument("threads", type=int, help="Number of threads to use")
    parser.add_argument("--start_date", type=str, default="2025-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, default="2025-01-03", help="End date (YYYY-MM-DD)")
    parser.add_argument("--save_folder", type=str, default="data", help="Folder to save parquet files")
    args = parser.parse_args()

    download_all(args.pair, args.threads, args.start_date, args.end_date, args.save_folder)
