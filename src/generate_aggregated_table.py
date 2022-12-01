import os
import pathlib

import pandas as pd

import database


DB_URI = os.getenv("DB_URI")
RESULTS_DIR = os.getenv("RESULTS_DIR")

engine = database.create_database_engine(DB_URI)


def generate_dataframe() -> pd.DataFrame:

    with database.create_session(engine) as session:
        df = database.get_aggregated_stats_per_batch(session)

    return df


def save_to_file(df: pd.DataFrame, outdir: pathlib.Path) -> None:
    outfile = outdir / "aggregated_stats_per_batch.csv"
    df.to_csv(outfile, index=False)


if __name__ == "__main__":
    outdir = pathlib.Path(RESULTS_DIR)
    outdir /= "table"
    outdir.mkdir(parents=True, exist_ok=True)
    df = generate_dataframe()
    save_to_file(df, outdir)
