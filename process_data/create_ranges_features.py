"""
Creates the HeartRateRange and SpeedRange features

HeartRateRange is between 0 and 1, where 0 is 0% of maximum heart rate, and 1 is 100%
HeartRateQuotient is the percentage of heart rates that are below the current value.

SpeedRange is between 0 and 1, where 0 is 0% of maximum heart rate, and 1 is 100%
SpeedQuotient is the percentage of heart rates that are below the current value.
"""

from pathlib import Path
from bisect import bisect_left
from csv import DictReader, DictWriter

from typing import Any, Dict, List


DATAPATH = Path("processed_data/")
INFILE = DATAPATH / "cleaned.csv"
OUTFILE = DATAPATH / "with_ranges_features.csv"


def add_range_features(data: List[Dict[str, Any]], name: str) -> List[Dict[str, Any]]:
    # TODO: We need a better measure of the highest value.
    # We currently probably get some outlier, which is not what we want.
    sort = sorted([float(point[name]) for point in data])
    return [
        p
        | {
            f"{name}Range": float(p[name]) / sort[-1],
            f"{name}Quotient": (bisect_left(sort, float(p[name])) / (len(sort) - 1)),
        }
        for p in data
    ]


def main():
    with INFILE.open("r") as f:
        points = list(DictReader(f))
    points = add_range_features(add_range_features(points, "HeartRate"), "Speed")
    assert all(points[0].keys() == point.keys() for point in points)
    with OUTFILE.open("w") as f:
        writer = DictWriter(f, fieldnames=points[0].keys())
        writer.writeheader()
        writer.writerows(points)


if __name__ == "__main__":
    main()
