#!/usr/bin/env python3
"""
If Distance does not exist, try to build it from Latitude and Longitude or Speed
If Speed does not exist, try to build it from Distance.
-> Since all must have one of the three, the resulting always has Speed.

The only sometimes missing value here is Elevation, which I'm willing to leave for now.
"""
from pathlib import Path
from datetime import datetime
from itertools import groupby, tee
from csv import DictReader, DictWriter

from geopy.distance import distance

from typing import Any, Dict, Iterable, List, Tuple, TypeVar

DATAPATH = Path("processed_data/")
INFILE = DATAPATH / "first_pass.csv"
OUTFILE = DATAPATH / "filled_in.csv"

T = TypeVar("T")


# From https://docs.python.org/3.8/library/itertools.html#itertools-recipes
def pairwise(iterable: Iterable[T]) -> Iterable[Tuple[T, T]]:
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def get_loc(point: Dict[str, Any]) -> Tuple[float, float]:
    "Get the location in geopy format from a point dict."
    return (float(point["Longitude"]), float(point["Latitude"]))


def get_time(point: Dict[str, Any]) -> datetime:
    "Get the time in python datetime format from a point dict"
    return datetime.fromisoformat(point["Time"])


def fill_in_distance(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fill in all the distances / speeds where there are none.

    A cleaner version of this code is a state machine that runs over each run.
    However, I'm willing to accept 20 lines of garbage if I don't need to write a state machine
    """
    with_distance = []
    for runid, group in groupby(data, lambda x: x["RunID"]):
        deleted = 0
        group = sorted(group, key=get_time)
        if group[0]["Distance"]:
            prev_dist = float(group[0]["Distance"])
        else:
            prev_dist = 0.0
        with_distance.append(group[0] | {"Distance": prev_dist})
        prev_time = get_time(group[0])
        for x, y in pairwise(group):
            if y["Distance"]:
                new_dist = float(y["Distance"])
            elif y["Speed"]:
                elapsed = (get_time(y) - prev_time).total_seconds()
                new_dist = prev_dist + (float(y["Speed"]) * elapsed)
            elif x["Latitude"] and x["Longitude"] and y["Latitude"] and y["Longitude"]:
                new_dist = prev_dist + distance(get_loc(x), get_loc(y)).meters
            else:
                deleted += 1
                continue
            with_distance.append(y | {"Distance": new_dist})
            prev_dist = new_dist
            prev_time = get_time(y)
        if deleted:
            print(f"Delted {deleted} points in {runid}, no way to calculate Distance")
    return with_distance


def fill_in_speed(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Fills in the speed value of all points where possible based off previous distance
    Does not allow negative speed
    Does some smoothening: all new calculated values are not raw,
    but instead 50% raw, and 50% from previous, smoothening out harmonically

    A cleaner version of this code is a state machine that runs over each run.
    However, I'm willing to accept 20 lines of garbage if I don't need to write a state machine
    """
    with_speed = []
    for runid, group in groupby(data, lambda x: x["RunID"]):
        deleted = 0
        group = sorted(group, key=get_time)
        if group[0]["Speed"]:
            prev_speed = float(group[0]["Speed"])
        else:
            prev_speed = 0.0
        with_speed.append(group[0] | {"Speed": prev_speed})
        for x, y in pairwise(group):
            if y["Speed"]:
                new_speed = float(y["Speed"])
            elif x["Distance"] < y["Distance"]:
                time_elapsed = (get_time(y) - get_time(x)).total_seconds()
                dist_elapsed = y["Distance"] - x["Distance"]
                new_speed = (prev_speed + (dist_elapsed / time_elapsed)) / 2
            else:
                deleted += 1
                continue
            with_speed.append(y | {"Speed": new_speed})
            prev_speed = new_speed
        if deleted:
            print(f"Delted {deleted} points in {runid}, negative speed")
    return with_speed


def main():
    with INFILE.open("r") as f:
        points = list(DictReader(f))
    points = fill_in_speed(fill_in_distance(points))
    assert all(points[0].keys() == point.keys() for point in points)
    with OUTFILE.open("w") as f:
        writer = DictWriter(f, fieldnames=points[0].keys())
        writer.writeheader()
        writer.writerows(points)


if __name__ == "__main__":
    main()
