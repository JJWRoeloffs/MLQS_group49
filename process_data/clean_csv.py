#!/usr/bin/env python3
"""
Cleans the data as it comes out raw from the csv.

Removes unreasonable values:
    Distance over 50km (Might be reasonable for others, but we know the subject.)
    Heart Rate over 220
    Heart Rate under 20
    Speed over 30km/h (8 m/s)
Removes runs that are incomplete:
    Less than 500m
    Less than 5min
    Less than 300 data points (which is 5min if it is all complete).
Removes first and last 10 vals of each run:
    This is reasonable, as they are usually starting/stopping strava and all that.
"""
from pathlib import Path
from datetime import datetime
from itertools import groupby
from csv import DictReader, DictWriter

from typing import Any, Dict, Iterable, List

DATAPATH = Path("processed_data/")
INFILE = DATAPATH / "filled_in.csv"
OUTFILE = DATAPATH / "cleaned.csv"


def get_time(point: Dict[str, Any]) -> datetime:
    "Get the time in python datetime format from a point dict"
    return datetime.fromisoformat(point["Time"])


def unclean_runs(data: List[Dict[str, Any]]) -> Iterable[str]:
    for runid, run in groupby(data, lambda x: x["RunID"]):
        run = sorted(run, key=get_time)
        if len(run) < 300:
            yield runid
            continue
        if (get_time(run[-1]) - get_time(run[0])).total_seconds() < 300:
            yield runid
            continue
        if max(float(point["Distance"]) for point in run) < 500:
            yield runid
            continue


def remove_unclean_runs(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    "Remove the runs that fit the criteria for a useless run"
    unclean_run_ids = set(unclean_runs(data))
    print(f"Removing {len(unclean_run_ids)} runs because they are unusable.")
    return [point for point in data if point["RunID"] not in unclean_run_ids]


def remove_trailing(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    "Remove the first and last 10 points of each run."
    return [
        point
        for _, run in groupby(data, lambda x: x["RunID"])
        for point in sorted(run, key=get_time)[10:-10]
    ]


def remove_unreasonable_values(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ret = [
        point
        for point in data
        if float(point["HeartRate"]) < 220
        if float(point["HeartRate"]) > 20
        if float(point["Distance"]) < 50_000
        if float(point["Speed"]) < 8
    ]
    print(f"Removed {len(data) - len(ret)} out of {len(data)} points")
    print(f"{((len(data) - len(ret))/len(data))*100}%, because they are unreasonable")
    return ret


def main():
    with INFILE.open("r") as f:
        points = list(DictReader(f))
    points = remove_trailing(remove_unclean_runs(remove_unreasonable_values(points)))
    assert all(points[0].keys() == point.keys() for point in points)
    with OUTFILE.open("w") as f:
        writer = DictWriter(f, fieldnames=points[0].keys())
        writer.writeheader()
        writer.writerows(points)


if __name__ == "__main__":
    main()
