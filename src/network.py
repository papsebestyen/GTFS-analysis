from typing import TYPE_CHECKING
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from parquetranger import TableRepo
from sklearn.neighbors import BallTree
from tqdm import tqdm

from .config import EARTH_RADIUS, MAX_WALK, PREFERRED_WALKING_SPEED
from .meta import Stops, StopTimes
from .naming import DATA_DIR
from .utils import _parse_time

if TYPE_CHECKING:
    from pathlib import Path


def _get_transit_edges(stop_times: pd.DataFrame, prune: bool = False):
    transit_edges = (
        stop_times.reset_index()
        .assign(
            **{
                "distance": lambda _df: (
                    _df[StopTimes.arrival_time].pipe(_parse_time).diff()
                ),
                Stops.stop_id
                + "_stop": lambda _df: (_df[StopTimes.stop_id.stop_id].shift(1)),
                "departure_trip_id": lambda _df: (
                    _df[StopTimes.trip_id.trip_id].shift(1)
                ),
            }
        )
        .rename(columns={StopTimes.stop_id.stop_id: Stops.stop_id + "_start"})
        .loc[
            lambda _df: (_df[StopTimes.trip_id.trip_id] == _df["departure_trip_id"]),
            [
                Stops.stop_id + "_stop",
                Stops.stop_id + "_start",
                "distance",
            ],
        ]
        .astype({"distance": int})
    )
    if prune:
        transit_edges = transit_edges.groupby(
            [Stops.stop_id + "_start", Stops.stop_id + "_stop"]
        ).min()
    else:
        transit_edges = transit_edges.set_index(
            [Stops.stop_id + "_start", Stops.stop_id + "_stop"]
        )
    return transit_edges


def _parse_nearest_indicies(
    i, stops: pd.DataFrame, indicies: np.array, distances: np.array
):
    return pd.concat(
        [
            (
                stops.iloc[[i] * indicies[i].shape[0], :]
                .reset_index()
                .rename(lambda x: x + "_start", axis=1)
            ),
            (
                stops.iloc[indicies[i], :]
                .reset_index()
                .rename(lambda x: x + "_stop", axis=1)
                .assign(distance=distances[i] * EARTH_RADIUS)
            ),
        ],
        axis=1,
    )


def _get_walk_edges(stops: pd.DataFrame):

    stop_locations = stops[[Stops.stop_lat, Stops.stop_lon]].applymap(np.deg2rad)
    geo_tree = BallTree(stop_locations, metric="haversine")

    indicies, distances = geo_tree.query_radius(
        stop_locations, r=MAX_WALK / EARTH_RADIUS, return_distance=True
    )

    return (
        pd.concat(
            [
                _parse_nearest_indicies(
                    i, stops=stops, indicies=indicies, distances=distances
                )
                for i in tqdm(range(indicies.shape[0]), total=indicies.shape[0])
            ],
            axis=0,
        )[lambda _df: _df[Stops.stop_id + "_start"] != _df[Stops.stop_id + "_stop"]]
        .assign(distance=lambda _df: _df["distance"] / PREFERRED_WALKING_SPEED)
        .set_index([Stops.stop_id + "_start", Stops.stop_id + "_stop"])[["distance"]]
    )


def _get_edge_recs(df: pd.DataFrame):
    return [
        *zip(
            df.index.get_level_values(Stops.stop_id + "_start"),
            df.index.get_level_values(Stops.stop_id + "_stop"),
            df.to_dict(orient="records"),
        )
    ]


def _get_node_recs(df: pd.DataFrame):
    return [*zip(df.index, df[["stop_lat", "stop_lon"]].to_dict("records"))]


def load_network(fp: "Path" = None, overwrite: bool = False, prune: bool = True):
    fp = fp or DATA_DIR / "tree.pickle"
    if fp.exists() and not overwrite:
        return nx.read_gpickle(fp)

    stops_trepo = TableRepo(root_path=DATA_DIR / "stops")
    stop_times_trepo = TableRepo(root_path=DATA_DIR / "stop_times")

    stops_df = (
        stops_trepo.get_full_df()
        .pipe(
            lambda _df: gpd.GeoDataFrame(
                _df,
                geometry=gpd.points_from_xy(_df[Stops.stop_lon], _df[Stops.stop_lat]),
            )
        )
        .assign(publisher=lambda _df: _df.index.str.split("_").str[-1])
    )
    stop_times_df = stop_times_trepo.get_full_df()

    graph = nx.MultiDiGraph()
    graph.add_nodes_from(_get_node_recs(stops_df))
    graph.add_edges_from(
        _get_edge_recs(_get_transit_edges(stop_times=stop_times_df, prune=prune))
    )
    graph.add_edges_from(_get_edge_recs(_get_walk_edges(stops=stops_df)))

    nx.write_gpickle(graph, fp)
    return graph
