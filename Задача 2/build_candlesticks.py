import argparse
import polars as pl
import os

def build_candlesticks(input_pattern, interval_ms, output_file):
    # Lire tous les fichiers correspondants en mode lazy
    df = pl.scan_parquet(input_pattern)

    # Correction de timestamp si besoin
    df = df.with_columns(
        (pl.col("timestamp") // 1_000_000).alias("timestamp")
        if df.select(pl.max("timestamp")).collect().item() > 1e12 else pl.col("timestamp")
    )

    # Grouper dynamiquement
    result = df.group_by_dynamic(
        index_column="timestamp",
        every=f"{interval_ms}i",
        closed="left",
        start_by="datapoint"
    ).agg([
        pl.col("price").max().alias("high"),
        pl.col("price").min().alias("low"),
        pl.col("price").last().alias("close"),
        pl.col("price").sample(1).alias("random"),
        pl.len().alias("num_trades"),
        pl.col("quote_qty").sum().alias("volume")
    ])

    # Collecter et sauvegarder
    result.collect().write_parquet(output_file)
    print(f"Chandeliers sauvegardés dans : {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build candlesticks from Binance trades")
    parser.add_argument("input_folder", type=str, help="Folder containing parquet files")
    parser.add_argument("pair", type=str, help="Trading pair, e.g., btcusdt")
    parser.add_argument("interval", type=str, help="Aggregation interval: 500ms, 1s, 1m")
    parser.add_argument("output_file", type=str, help="Output parquet file")
    args = parser.parse_args()

    if args.interval == "500ms":
        interval_ms = 500
    elif args.interval == "1s":
        interval_ms = 1000
    elif args.interval == "1m":
        interval_ms = 60000
    else:
        raise ValueError("Interval must be one of: 500ms, 1s, 1m")

    # Utiliser un motif wildcard pour charger tous les fichiers de la paire
    pattern = os.path.join(args.input_folder, f"{args.pair.lower()}-*.parquet")

    build_candlesticks(pattern, interval_ms, args.output_file)
