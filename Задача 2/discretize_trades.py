import argparse
import polars as pl
import os

def discretize_trades(input_pattern, interval_ms, output_file):
    # Lire tous les fichiers correspondants en mode lazy
    df = pl.scan_parquet(input_pattern)

    # Correction de timestamp si besoin
    df = df.with_columns(
        (pl.col("timestamp") // 1_000_000).alias("timestamp")
        if df.select(pl.max("timestamp")).collect().item() > 1e12 else pl.col("timestamp")
    )

    # Ajouter colonne trade_type
    df = df.with_columns(
        pl.col("is_buyer_maker").cast(pl.Int8).alias("trade_type")
    )

    # Discrétiser par trade_type
    result = df.group_by_dynamic(
        index_column="timestamp",
        every=f"{interval_ms}i",
        closed="left",
        start_by="datapoint",
        group_by="trade_type"
    ).agg([
        (pl.col("price") * pl.col("quantity")).sum().alias("weighted_price_sum"),
        pl.col("quantity").sum().alias("quantity"),
        pl.col("quote_qty").sum().alias("quote_qty"),
        pl.len().alias("num_trades")
    ])

    # Calculer le prix moyen pondéré
    result = result.with_columns(
        (pl.col("weighted_price_sum") / pl.col("quantity")).alias("price")
    ).drop("weighted_price_sum")

    # Collecter et sauvegarder
    result.collect().write_parquet(output_file)
    print(f"Trades discrétisés sauvegardés dans : {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discretize trades from Binance data")
    parser.add_argument("input_folder", type=str, help="Folder containing parquet files")
    parser.add_argument("pair", type=str, help="Trading pair, e.g., btcusdt")
    parser.add_argument("interval", type=str, help="Aggregation interval: 250ms, 500ms, 1s, 1h")
    parser.add_argument("output_file", type=str, help="Output parquet file")
    args = parser.parse_args()

    if args.interval == "250ms":
        interval_ms = 250
    elif args.interval == "500ms":
        interval_ms = 500
    elif args.interval == "1s":
        interval_ms = 1000
    elif args.interval == "1h":
        interval_ms = 3600 * 1000
    else:
        raise ValueError("Interval must be one of: 250ms, 500ms, 1s, 1h")

    pattern = os.path.join(args.input_folder, f"{args.pair.lower()}-*.parquet")

    discretize_trades(pattern, interval_ms, args.output_file)
