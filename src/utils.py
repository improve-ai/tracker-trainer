# Built-in imports
from datetime import datetime
import logging
import json

# Local imports
from config import DATETIME_FORMAT


def load_sorted_records(filename):
    """
    Load jsonlines files and sort it first based on the type of its records
    and then by its timestamp.

    Args:
        filename: name of the input jsonlines file to load
    
    Returns:
        A list of sorted records, first by type, then by timestamp.
    """

    with open(filename, encoding="utf8") as f:
        records = []
        for line in f.readlines():
            try:
                records.append(json.loads(line))
            except json.decoder.JSONDecodeError as e:
                logging.error(
                    f"Error reading a record in file '{filename}', skipping."
                    f"{e}")

    # Sort records by record type and timestamp
    # decision > event > rewards
    records.sort(
        key=lambda x: (
            x['type'],
            datetime.strptime(x['timestamp'], DATETIME_FORMAT),
    ))

    return records