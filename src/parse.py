from pathlib import Path

import pandas as pd

from .meta import Stops, StopTimes


def _parse_stops(feed_dir: Path, feed_name: str):
    return (
        pd.read_csv(feed_dir / "stops.txt", encoding="utf-8")
        .loc[
            :,
            [
                Stops.stop_id,
                Stops.stop_name,
                Stops.stop_lon,
                Stops.stop_lat,
            ],
        ]
        .assign(
            **{
                Stops.stop_id: lambda _df: (
                    _df[Stops.stop_id].astype(str) + f"_{feed_name}"
                ),
            }
        )
        .set_index([Stops.stop_id])
    )


def _parse_stop_times(feed_dir: Path, feed_name: str):
    return (
        pd.read_csv(feed_dir / "stop_times.txt", encoding="utf-8")
        .rename(
            columns={
                "trip_id": StopTimes.trip_id.trip_id,
                "stop_id": StopTimes.stop_id.stop_id,
            }
        )
        .loc[
            :,
            (
                StopTimes.trip_id.trip_id,
                StopTimes.stop_id.stop_id,
                StopTimes.arrival_time,
                StopTimes.departure_time,
                StopTimes.stop_sequence,
            ),
        ]
        .assign(
            **{
                StopTimes.stop_id.stop_id: lambda _df: (
                    _df[StopTimes.stop_id.stop_id].astype(str) + f"_{feed_name}"
                ),
                StopTimes.trip_id.trip_id: lambda _df: (
                    _df[StopTimes.trip_id.trip_id].astype(str) + f"_{feed_name}"
                ),
            }
        )
        .set_index(
            [
                StopTimes.trip_id.trip_id,
                StopTimes.stop_id.stop_id,
            ]
        )
    )


def parse_feed(feed_dir: Path):
    stops_df = _parse_stops(
        feed_dir=feed_dir,
        feed_name=feed_dir.name,
    )

    stop_times_df = _parse_stop_times(
        feed_dir=feed_dir,
        feed_name=feed_dir.name,
    )
    return stops_df, stop_times_df
