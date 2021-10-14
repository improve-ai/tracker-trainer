# Built-in imports
from pathlib import Path

# Local imports
from history_record import _load_records
from history_record import HistoryRecord

def test__load_records():
    """
    Missing:
    - If a bad Gzip is passed, make sure a file in /unrecoverable is written out.
    - config.stats is incremented by one
    """
    
    synthetic_data = Path("./tests/test_data/dummy_incoming/incoming").glob("*.jsonl.gz")

    for f in synthetic_data:
        records = _load_records(f, set())
        assert isinstance(records, list)
        for r in records:
            assert isinstance(r, HistoryRecord)


