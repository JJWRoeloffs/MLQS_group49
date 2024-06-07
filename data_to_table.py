"""
Get the data out in a CSV with the following Collumns:

RunID:
    Some random but unique ID of the run the point is from
Time:
    Time of the point in POSIX time.
Latitude:
    Latitude as a literal float
Longitude:
    Longitude as a literal float
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

from csv import DictWriter
from pathlib import Path
from typing import Any, Dict, List, Optional

from tcxreader import TCXReader
from fitdecode import FitReader, FIT_FRAME_DATA

# In .gitmodules, to be imported with access to the repo
INPATH = Path("data/")
# In .gitignore, to save the data to.
OUTPATH = Path("processed_data/")
OUTPATH.mkdir(exist_ok=True)


# Fit format multiplies coordinates with this constant to not lose any data.
FIT_RANGE = 2**32 / 360


def fit_to_float(coordinate: Optional[int]) -> Optional[float]:
    if coordinate is not None:
        return coordinate / FIT_RANGE


def points_from_tcx(file: Path) -> List[Dict[str, Any]]:
    # The tcxreader API does not appear to allow to do this in-place
    file.write_text(file.read_text().strip())

    return [
        {
            "RunID": file.stem,
            "Time": trackpoint.time.timestamp(),
            "Latitude": getattr(trackpoint, "latitude", None),
            "Longitude": getattr(trackpoint, "longitude", None),
            "Elevation": getattr(trackpoint, "elevation", None),
            "Distance": getattr(trackpoint, "distance", None),
            "HeartRate": getattr(trackpoint, "hr_value", None),
            "Cadence": getattr(trackpoint, "cadence", None),
            "Speed": getattr(trackpoint, "tpx_ext", dict()).get("speed", None),
        }
        for lap in TCXReader().read(str(file)).laps
        for trackpoint in lap.trackpoints
        if getattr(trackpoint, "hr_value", None) is not None
    ]


def points_from_fit(file: Path) -> List[Dict[str, Any]]:
    with FitReader(str(file)) as fit:
        return [
            {
                "RunID": file.stem,
                "Time": frame.get_field("timestamp").value.timestamp(),
                "Latitude": fit_to_float(frame.get_field("position_lat").raw_value),
                "Longitude": fit_to_float(frame.get_field("position_long").raw_value),
                "Elevation": frame.get_field("enhanced_altitude").value,
                "Distance": frame.get_field("distance").value,
                "HeartRate": frame.get_field("heart_rate").value,
                "Cadence": frame.get_field("cadence").value,
                "Speed": frame.get_field("speed").value,
            }
            for frame in fit
            if frame.frame_type == FIT_FRAME_DATA
            # Quite a few points have *ONLY* timestamp. This filters those out.
            if frame.has_field("heart_rate")
            and frame.get_field("heart_rate").value is not None
        ]


def points_from_file(file: Path) -> List[Dict[str, Any]]:
    match file.suffix:
        case ".tcx":
            return points_from_tcx(file)
        case ".fit":
            return points_from_fit(file)
        case _:
            return []


def main():
    points = [point for file in INPATH.iterdir() for point in points_from_file(file)]
    assert all(points[0].keys() == point.keys() for point in points)
    with OUTPATH.joinpath("first_pass.csv").open("w") as f:
        fieldnames = points[0].keys()
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(points)


if __name__ == "__main__":
    main()
