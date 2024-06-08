#!/usr/bin/env python3
"""
Get the data out in a CSV. First retrieving all possibly useful points,
Then merging them together (as both formats tend to have points at the same time,
where one has only the heart rate, and the other only the speed,) and doing a first
pass to remove some useless points (Not at least some distance and heart rate metric),
the latter mostly to remove entire *files* that don't have any of either.

It gets the following Columns:

RunID:
    Some random but unique ID of the run the point is from
Time:
    Time of the point in iso format.
Latitude:
    Latitude in degrees
Longitude:
    Longitude in degrees
Elevation:
    Elevation in meters
Distance:
    Distance run so far in meters
HeartRate:
    HeartRate at this point in BPM
Cadence:
    Cadence at this point in BPM
Speed:
    Speed at this point in m/s
"""

from pathlib import Path
from csv import DictWriter
from functools import reduce
from itertools import groupby

from tcxreader import TCXReader
from fitdecode import FitReader, FIT_FRAME_DATA

from typing import Any, Dict, List, Optional

# In .gitmodules, to be imported with access to the repo
INPATH = Path("data/")
# In .gitignore, to save the data to.
OUTPATH = Path("processed_data/")
OUTPATH.mkdir(exist_ok=True)
OUTFILE = OUTPATH / "first_pass.csv"


def merge_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merges points at the same time of the same run together,
    making one point with as little as possible missing values.
    This is especially useful for .fit files, but might be generally so."""
    return [
        reduce(lambda first, second: {k: first[k] or second[k] for k in first}, group)
        for _, group in groupby(data, lambda x: x["RunID"] + x["Time"])
    ]


def remove_useless(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove any points that don't have some metric of speed and heartrate"""
    return [
        point
        for point in data
        if point.get("Speed") is not None
        or point.get("HeartRate") is not None
    ]


def points_from_fit(file: Path) -> List[Dict[str, Any]]:
    points = []
    with file.open("rb") as f:
        fitfile = FitReader(f)
        for frame in fitfile:
            if frame.frame_type == FIT_FRAME_DATA:
                points.append({
                    "RunID": file.stem,
                    "Time": frame.get_value("timestamp").isoformat(),
                    "Latitude": frame.get_value("position_lat", fallback=None),
                    "Longitude": frame.get_value("position_long", fallback=None),
                    "Elevation": frame.get_value("altitude", fallback=None),
                    "Distance": frame.get_value("distance", fallback=None),
                    "HeartRate": frame.get_value("heart_rate", fallback=None),
                    "Cadence": frame.get_value("cadence", fallback=None),
                    "Speed": frame.get_value("speed", fallback=None),
                })
    return points


def points_from_tcx(file: Path) -> List[Dict[str, Any]]:
    # The tcxreader API does not appear to allow to do this in-place
    file.write_text(file.read_text().strip())

    return [
        {
            "RunID": file.stem,
            "Time": trackpoint.time.isoformat(),
            "Latitude": getattr(trackpoint, "latitude", None),
            "Longitude": getattr(trackpoint, "longitude", None),
            "Elevation": getattr(trackpoint, "elevation", None),
            "Distance": getattr(trackpoint, "distance", None),
            "HeartRate": getattr(trackpoint, "hr_value", None),
            "Cadence": getattr(trackpoint, "cadence", None),
            "Speed": getattr(trackpoint, "tpx_ext", dict()).get("Speed", None),
        }
        for lap in TCXReader().read(str(file)).laps
        for trackpoint in lap.trackpoints
        if getattr(trackpoint, "hr_value", None) is not None
        or getattr(trackpoint, "longitude", None) is not None
        or getattr(trackpoint, "latitude", None) is not None
    ]


def points_from_file(file: Path) -> List[Dict[str, Any]]:
    if file.suffix == ".tcx":
        points = points_from_tcx(file)
    elif file.suffix == ".fit":
        points = points_from_fit(file)
    else:
        points = []

    return remove_useless(merge_data(points))


def main():
    points = [point for file in INPATH.iterdir() for point in points_from_file(file)]
    assert all(points[0].keys() == point.keys() for point in points)
    count = len({point["RunID"] for point in points})
    print(f"Retrieved points from {count} files, with {len(points)/count} points per")
    with OUTFILE.open("w") as f:
        writer = DictWriter(f, fieldnames=points[0].keys())
        writer.writeheader()
        writer.writerows(points)


if __name__ == "__main__":
    main()