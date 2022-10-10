from typing import TYPE_CHECKING

from .naming import HOUR, MINUTE, SECOND

if TYPE_CHECKING:
    import pandas as pd


def _parse_time(_s: "pd.Series"):
    return (
        _s.str.extract(r"(\d\d):(\d\d):(\d\d)")
        .astype(int)
        .pipe(lambda _df: _df[0] * HOUR + _df[1] * MINUTE + _df[2] * SECOND)
    )
