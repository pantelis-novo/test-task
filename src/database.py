from typing import List
import datetime

import pandas as pd

import sqlalchemy as sqla
import sqlalchemy.orm as orm

Base = orm.declarative_base()


class PH1(Base):
    __tablename__ = "ph1"
    timestamp = sqla.Column(sqla.DateTime, primary_key=True)
    value = sqla.Column(sqla.Float, nullable=False)


class PH2(Base):
    __tablename__ = "ph2"
    timestamp = sqla.Column(sqla.DateTime, primary_key=True)
    value = sqla.Column(sqla.Float, nullable=False)


class Temp1(Base):
    __tablename__ = "temp1"
    timestamp = sqla.Column(sqla.DateTime, primary_key=True)
    value = sqla.Column(sqla.Float, nullable=False)


class Temp2(Base):
    __tablename__ = "temp2"
    timestamp = sqla.Column(sqla.DateTime, primary_key=True)
    value = sqla.Column(sqla.Float, nullable=False)


class BatchInfo(Base):
    __tablename__ = "batch_info"
    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    start_date = sqla.Column(sqla.DateTime)
    end_date = sqla.Column(sqla.DateTime)
    batch_id = sqla.Column(sqla.String(10), nullable=True)


class BatchPhase(Base):
    __tablename__ = "batch_phase"
    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    start_date = sqla.Column(sqla.DateTime)
    end_date = sqla.Column(sqla.DateTime)
    batch_phase = sqla.Column(sqla.String)


sensor_name_mapping = dict(ph1=PH1, ph2=PH2, temp1=Temp1, temp2=Temp2)


def create_database_engine(url) -> sqla.engine.Engine:
    # dialect[+driver]://user:password@host/dbname
    sqla_engine = sqla.create_engine(
        url,
        echo=False,
        pool_pre_ping=True,  # automatically re-spawn stale connections
    )

    Base.metadata.create_all(sqla_engine)

    return sqla_engine


def create_session(engine: sqla.engine.Engine) -> orm.Session:
    session = orm.Session(engine)
    try:
        return session
    finally:
        session.close()


def avg_per_batch(session: orm.Session, sensor: str) -> sqla.sql.expression.CTE:
    sensor_table = sensor_name_mapping[sensor]

    cte = (
        session.query(
            BatchInfo.batch_id,
            (BatchPhase.end_date - BatchPhase.start_date).label("batch_duration"),
            sqla.func.avg(sensor_table.value).label(f"avg_{sensor}"),
        )
        .join(
            BatchPhase,
            BatchPhase.start_date.between(BatchInfo.start_date, BatchInfo.end_date),
        )
        .join(
            sensor_table,
            sensor_table.timestamp.between(BatchInfo.start_date, BatchInfo.end_date),
        )
        .filter(BatchPhase.batch_phase == "cultivation")
        .filter(BatchInfo.batch_id != "NaN")
        .filter(BatchInfo.batch_id.notlike("TEST%"))
        .group_by(BatchInfo.batch_id, BatchPhase.end_date - BatchPhase.start_date)
        .cte()
    )
    return cte


def get_batches_ids(session: orm.Session) -> List[str]:
    q = (
        session.query(BatchInfo)
        .filter(BatchInfo.batch_id != "NaN")
        .filter(BatchInfo.batch_id.notlike("TEST%"))
    )
    return [row.batch_id for row in q.all()]


def get_aggregated_stats_per_batch(session: orm.Session) -> pd.DataFrame:

    avg_temp1 = avg_per_batch(session, "temp1")
    avg_temp2 = avg_per_batch(session, "temp2")
    avg_ph1 = avg_per_batch(session, "ph1")
    avg_ph2 = avg_per_batch(session, "ph2")

    query = (
        session.query(
            avg_temp1.c.batch_id,
            avg_temp1.c.batch_duration,
            avg_temp1.c.avg_temp1,
            avg_temp2.c.avg_temp2,
            avg_ph1.c.avg_ph1,
            avg_ph2.c.avg_ph2,
        )
        .join(
            avg_temp2,
            avg_temp1.c.batch_id == avg_temp2.c.batch_id,
        )
        .join(
            avg_ph1,
            avg_temp1.c.batch_id == avg_ph1.c.batch_id,
        )
        .join(
            avg_ph2,
            avg_temp1.c.batch_id == avg_ph2.c.batch_id,
        )
    )
    return pd.read_sql(query.statement, query.session.bind)


def _round_timestamp(timestamp):
    return timestamp + datetime.timedelta(seconds=30)


def get_batch_sensor_difference(
    session: orm.Session, batch_id: str, sensor_type: str
) -> pd.DataFrame:

    sensor_table_1 = sensor_name_mapping[f"{sensor_type}1"]
    sensor_table_2 = sensor_name_mapping[f"{sensor_type}2"]

    query = (
        session.query(
            BatchInfo.id,
            sqla.func.date_trunc(
                "minutes", _round_timestamp(sensor_table_2.timestamp)
            ).label("date"),
            (sensor_table_2.value - sensor_table_1.value).label(
                f"{sensor_type}_difference"
            ),
        )
        .join(
            BatchPhase,
            BatchPhase.start_date.between(BatchInfo.start_date, BatchInfo.end_date),
        )
        .join(
            sensor_table_2,
            sensor_table_2.timestamp.between(BatchPhase.start_date, BatchPhase.end_date),
        )
        .join(
            sensor_table_1,
            (
                sqla.func.date_trunc(
                    "minutes", _round_timestamp(sensor_table_1.timestamp)
                )
                == sqla.func.date_trunc(
                    "minutes", _round_timestamp(sensor_table_2.timestamp)
                )
            ),
        )
        .filter(BatchPhase.batch_phase == "cultivation")
        .filter(BatchInfo.batch_id == batch_id)
        .order_by("date")
    )

    return pd.read_sql(query.statement, query.session.bind)
