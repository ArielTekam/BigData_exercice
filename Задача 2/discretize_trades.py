import argparse
import polars as pl
import os

def discretize_trades(input_files, interval_ms, output_file):
    dfs = []
    for file in input_files:
        df = pl.read_parquet(file)

        # Correction timestamp si besoin
        if df["timestamp"].max() > 1e12:
            df = df.with_columns((pl.col("timestamp") // 1_000_000).alias("timestamp"))

        # Créer une colonne trade_type
        df = df.with_columns(
            pl.col("is_buyer_maker").cast(pl.Int8).alias("trade_type")
        )

        # Correction: utiliser group_by et correct multiplier
        df = df.group_by_dynamic(
            index_column="timestamp",
            every=f"{interval_ms}i",
            closed="left",
            start_by="datapoint",
            group_by="trade_type"  # <<< correction : group_by et plus by
        ).agg([
            (pl.col("price") * pl.col("quantity")).sum().alias("weighted_price_sum"),
            pl.col("quantity").sum().alias("quantity"),
            pl.col("quote_qty").sum().alias("quote_qty"),
            pl.len().alias("num_trades")
        ])

        # Calcul du prix moyen pondéré
        df = df.with_columns(
            (pl.col("weighted_price_sum") / pl.col("quantity")).alias("price")
        ).drop("weighted_price_sum")  # On enlève la colonne temporaire

        dfs.append(df)

    # Concaténer tous les résultats
    result = pl.concat(dfs)
    result.write_parquet(output_file)
    print(f"Trades discrétisés sauvegardés dans : {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discretize trades from Binance data")
    parser.add_argument("input_folder", type=str, help="Input folder containing parquet files")
    parser.add_argument("interval", type=str, help="Aggregation interval: 250ms, 500ms, 1s, 1h")
    parser.add_argument("output_file", type=str, help="Output parquet file")
    args = parser.parse_args()

    # Conversion de l'intervalle
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

    # Lire tous les fichiers parquet
    input_files = [os.path.join(args.input_folder, f) for f in os.listdir(args.input_folder) if f.endswith(".parquet")]

    discretize_trades(input_files, interval_ms, args.output_file)
