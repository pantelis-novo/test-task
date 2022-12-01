import pathlib
import os
from typing import Dict, List
import datetime
import pydantic
import glob

import database
import pandas as pd

DB_URI = os.getenv("DB_URI")

engine = database.create_database_engine(DB_URI)


class SensorValues(pydantic.BaseModel):
    timestamp: datetime.datetime
    value: float


def read_sensor_values(filename: pathlib.Path) -> List[Dict]:
    df = pd.read_csv(filename, delimiter="\t")

    def parse(row) -> Dict:
        return dict(timestamp=row["timestamp"], value=row["value"])

    return [parse(row) for _, row in df.iterrows()]


def read_batch_info_values(filename: pathlib.Path) -> List[Dict]:
    df = pd.read_csv(filename, delimiter="\t")

    def parse(row) -> Dict:
        return {
            "start_date": row["StartDate"],
            "end_date": row["EndDate"],
            "batch_id": row["BatchID"],
        }

    return [parse(row) for _, row in df.iterrows()]


def read_batch_phase_values(filename: pathlib.Path) -> List[Dict]:
    df = pd.read_csv(filename, delimiter="\t")

    def parse(row) -> Dict:
        return {
            "start_date": row["StartDate"],
            "end_date": row["EndDate"],
            "batch_phase": row["BatchPhase"],
        }

    return [parse(row) for _, row in df.iterrows()]


def insert_sensor_data(data_dir) -> None:
    with database.create_session(engine) as session:
        for filename in glob.glob(str(data_dir / "400E*")):
            filename = pathlib.Path(filename)

            sensor = filename.stem.split("_")[-1]
            sensor_class = getattr(database, sensor)
            sensor_values = read_sensor_values(filename)

            session.bulk_insert_mappings(sensor_class, sensor_values)
            session.commit()


def insert_batch_info(data_dir) -> None:
    with database.create_session(engine) as session:
        filename = data_dir / "batch_info.csv"

        batch_info_values = read_batch_info_values(filename)

        session.bulk_insert_mappings(database.BatchInfo, batch_info_values)
        session.commit()


def insert_batch_phase(data_dir) -> None:
    with database.create_session(engine) as session:
        filename = data_dir / "batch_phase.csv"

        batch_phase_values = read_batch_phase_values(filename)

        session.bulk_insert_mappings(database.BatchPhase, batch_phase_values)
        session.commit()


if __name__ == "__main__":
    data_dir = pathlib.Path("/app/data")
    insert_sensor_data(data_dir)
    insert_batch_info(data_dir)
    insert_batch_phase(data_dir)
