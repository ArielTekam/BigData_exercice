import argparse
import polars as pl
import os

# Fonction pour construire les chandeliers
def build_candlesticks(input_files, interval_ms, output_file):
    dfs = []
    for file in input_files:
        df = pl.read_parquet(file)
        
        # Vérifier le timestamp
        if df["timestamp"].max() > 1e12:
            # Cas très improbable ici, mais bon à garder si timestamp en nanosecondes
            df = df.with_columns((pl.col("timestamp") // 1_000_000).alias("timestamp"))

        # Grouper dynamiquement
        df = df.group_by_dynamic(
            index_column="timestamp",
            every=f"{interval_ms}i",  # <<< CORRECTION IMPORTANTE : ajouter 'i' pour integer timestamp
            closed="left",
            start_by="datapoint"
        ).agg([
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("price").sample(1).alias("random"),
            pl.len().alias("num_trades"),  # <<< CORRECTION : remplacer pl.count() par pl.len()
            pl.col("quote_qty").sum().alias("volume")
        ])
        dfs.append(df)

    # Concaténer
    result = pl.concat(dfs)
    result.write_parquet(output_file)
    print(f"Chandeliers sauvegardés dans : {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build candlesticks from Binance trades")
    parser.add_argument("input_folder", type=str, help="Input folder containing parquet files")
    parser.add_argument("interval", type=str, help="Aggregation interval: 500ms, 1s, 1m")
    parser.add_argument("output_file", type=str, help="Output parquet file")
    args = parser.parse_args()

    # Convertir intervalle en millisecondes
    if args.interval == "500ms":
        interval_ms = 500
    elif args.interval == "1s":
        interval_ms = 1000
    elif args.interval == "1m":
        interval_ms = 60000
    else:
        raise ValueError("Interval must be one of: 500ms, 1s, 1m")

    # Chercher tous les fichiers parquet du dossier
    input_files = [os.path.join(args.input_folder, f) for f in os.listdir(args.input_folder) if f.endswith(".parquet")]

    build_candlesticks(input_files, interval_ms, args.output_file)
