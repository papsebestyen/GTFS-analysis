from typing import TYPE_CHECKING

from .naming import HOUR, MINUTE, SECOND
from functools import partial
from atqo import parallel_map
import networkx as nx
import numpy as np

if TYPE_CHECKING:
    import pandas as pd


def _parse_time(_s: "pd.Series"):
    return (
        _s.str.extract(r"(\d\d):(\d\d):(\d\d)")
        .astype(int)
        .pipe(lambda _df: _df[0] * HOUR + _df[1] * MINUTE + _df[2] * SECOND)
    )


def _get_dist(graph: nx.MultiDiGraph, stop_pairs):
    return nx.dijkstra_path_length(graph, *stop_pairs, weight="distance") / HOUR


def get_travel_time(
    graph: nx.MultiDiGraph,
    stop_pairs: np.array,
    workers: int = 8,
    pbar: bool = False,
    **kwargs
):
    return parallel_map(
        partial(_get_dist, graph),
        iterable=stop_pairs,
        workers=workers,
        pbar=pbar,
        **kwargs
    )
