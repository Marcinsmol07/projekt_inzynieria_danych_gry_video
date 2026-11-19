import os
import json
import psycopg2
from google.cloud import bigquery
from google.oauth2 import service_account

# --- KONFIGURACJA (Krok 11 & Krok 15) ---
source_table = os.getenv("TABLE", "games")   # Nazwa tabeli źródłowej (np. 'games' lub z load.yml)
incremental_column = "updated_at"            # Kolumna służąca do śledzenia zmian (CDC)
load_mode = "full"                           # Tryb ładowania: 'full' (Krok 12) lub 'incremental' (Krok 15)

# Zmienne środowiskowe przekazywane z GitHub Actions (Secrets & Env)
NEON_DB_URL = os.getenv("NEON_DB_URL")
GCP_SA_KEY = os.getenv("GCP_SA_KEY")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")


def get_bigquery_client():
    credentials_info = json.loads(GCP_SA_KEY)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def run_etl():
    print(f"Start ładowania tabeli '{source_table}' w trybie '{load_mode}'...")
    bq_client = get_bigquery_client()
    destination_table = f"{PROJECT_ID}.{DATASET}.{source_table}"

    # 1. Połączenie z bazą źródłową Neon (PostgreSQL)
    conn = psycopg2.connect(NEON_DB_URL)
    cursor = conn.cursor()

    last_watermark = None

    # 2. Jeśli tryb to 'incremental', pobierz najwyższą wartość incremental_column z BigQuery
    if load_mode == "incremental":
        query_watermark = f"SELECT MAX({incremental_column}) AS max_val FROM `{destination_table}`"
        try:
            result = bq_client.query(query_watermark).result()
            for row in result:
                last_watermark = row.max_val
            print(f"Ostatni watermark ({incremental_column}) w BigQuery: {last_watermark}")
        except Exception as e:
            print(f"Nie znaleziono tabeli lub watermarku (wykonam pełny zrzut): {e}")

    # 3. Pobranie danych z NeonDB
    if load_mode == "incremental" and last_watermark is not None:
        sql = f"SELECT * FROM {source_table} WHERE {incremental_column} > %s"
        cursor.execute(sql, (last_watermark,))
    else:
        sql = f"SELECT * FROM {source_table}"
        cursor.execute(sql)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    if not rows:
        print(f"Brak nowych/zmienionych wierszy w tabeli '{source_table}'.")
        return

    # 4. Konwersja typów danych (daty, Decimal) do formatu akceptowanego przez BigQuery JSON Loader
    records = []
    for row in rows:
        record = {}
        for col_name, value in zip(column_names, row):
            if hasattr(value, "isoformat"):
                record[col_name] = value.isoformat()
            elif type(value).__name__ == "Decimal":
                record[col_name] = float(value)
            else:
                record[col_name] = value
        records.append(record)

    # 5. Konfiguracja i załadowanie danych do BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if load_mode == "full"
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
    )

    load_job = bq_client.load_table_from_json(records, destination_table, job_config=job_config)
    load_job.result()  # Oczekiwanie na zakończenie zadania

    print(f"Sukces! Załadowano {len(records)} wierszy do {destination_table}.")


if __name__ == "__main__":
    run_etl()
