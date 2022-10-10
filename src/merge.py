from pathlib import Path
from shutil import rmtree
from zipfile import ZipFile

from parquetranger import TableRepo
from tqdm import tqdm

from .naming import DATA_DIR, EXTRACT_DIR, FEED_FILES, RAW_DIR
from .parse import parse_feed


def _extract_feed(feed_fp: Path, feed_name: str, extract_dir: Path = EXTRACT_DIR):
    with ZipFile(feed_fp) as file:
        file.extractall(extract_dir / feed_name)


def _extract_all_feed(feeds: list):
    """feeds: Iterable containing name and path pairs. [('a', '~/GTFS/a.zip')]"""
    [_extract_feed(feed_fp=RAW_DIR / fname, feed_name=name) for name, fname in feeds]


def merge_feeds(feeds: list = None):
    feeds = feeds or FEED_FILES.items()
    _extract_all_feed(feeds=feeds)

    stops_trepo = TableRepo(root_path=DATA_DIR / "stops")
    stop_times_trepo = TableRepo(root_path=DATA_DIR / "stop_times")

    stops_trepo.purge()
    stop_times_trepo.purge()

    for feed_name in tqdm(FEED_FILES.keys()):
        stops_df, stop_times_df = parse_feed(feed_dir=EXTRACT_DIR / feed_name)
        stops_trepo.extend(stops_df)
        stop_times_trepo.extend(stop_times_df)

    rmtree(EXTRACT_DIR)
