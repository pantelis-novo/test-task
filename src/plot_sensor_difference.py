import os
import pathlib

import pandas as pd
from sqlalchemy import orm
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import database

DB_URI = os.getenv('DB_URI')
RESULTS_DIR = os.getenv('RESULTS_DIR')


engine = database.create_database_engine(DB_URI)


def query_merge_sensor_difference(session: orm.Session, batch_id: str) -> pd.DataFrame:

    temp_diff = database.get_batch_sensor_difference(session, batch_id, 'temp')
    ph_diff = database.get_batch_sensor_difference(session, batch_id, 'ph')

    sensors_diff = pd.merge(temp_diff, ph_diff, how='inner', on='date')

    return sensors_diff


def make_plot(session: orm.Session, outdir: pathlib.Path) -> None:

    batch_ids = database.get_batches_ids(session)

    for idx, batch_id in enumerate(batch_ids):
        fig = make_subplots(
            rows=2,
            cols=1,
            subplot_titles=[f'{sensor}' for sensor in ['PH', 'Temperature']]
        )

        # Generate data
        sensors_diff = query_merge_sensor_difference(session, batch_id)

        # Make scatter lines
        fig.add_trace(
            go.Scatter(
                x=sensors_diff['date'],
                y=sensors_diff['ph_difference'],
                name='PH2 - PH1'
            ),
            row=1,
            col=1
        )
        fig.add_trace(
            go.Scatter(
                x=sensors_diff['date'],
                y=sensors_diff['temp_difference'],
                name='Temp2 - Temp1'
            ),
            row=2,
            col=1
        )

        # Figure Layout
        fig.update_layout(
            height=900,
            width=1300,
            title_text=f"Sensor difference for batch {batch_id}",
            title_x=0.5,
            showlegend=False
        )
        fig['layout']['yaxis']['title'] = 'PH2 - PH1'
        fig['layout']['yaxis2']['title'] = 'Temp2 - Temp1'
        fig['layout']['xaxis']['title'] = 'Datetime'
        fig['layout']['xaxis2']['title'] = 'Datetime'
        # Write to file
        outfile = outdir / f'{batch_id}_sensor_diff.png'
        fig.write_image(outfile)


if __name__ == '__main__':
    with database.create_session(engine) as session:
        outdir = pathlib.Path(RESULTS_DIR)
        outdir /= 'plots'
        outdir.mkdir(parents=True, exist_ok=True)
        make_plot(session, outdir)
