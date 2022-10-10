import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

FEED_FILES = json.loads(os.getenv("GTFS_FILES"))

DATA_DIR = Path(os.getenv("GTFS_DATA"))
EXTRACT_DIR = DATA_DIR / "extracted"
RAW_DIR = DATA_DIR / "raw"

EXTRACT_DIR.mkdir(exist_ok=True)

SECOND = 1
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
