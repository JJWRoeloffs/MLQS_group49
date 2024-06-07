import os
import pandas as pd

data_dir = "data/"
records = []

for filename in os.listdir(data_dir):
    if filename.endswith(".tcx"):
        file_path = os.path.join(data_dir, filename)

        # Read and clean the file
        with open(file_path, "r") as file:
            cleaned_content = file.read().strip()

        with open(file_path, "w") as file:
            file.write(cleaned_content)


import os
import pandas as pd
from tcxreader.tcxreader import TCXReader


def extract_trackpoints_from_tcx(file_path):
    # Read the TCX file
    tcx = TCXReader().read(file_path)

    trackpoints = []

    # Iterate over laps and trackpoints to extract data
    for lap in tcx.laps:
        for trackpoint in lap.trackpoints:
            trackpoints.append(
                {
                    "RunID": os.path.splitext(os.path.basename(file_path))[0],
                    "Time": trackpoint.time,
                    "Latitude": getattr(trackpoint, "latitude", None),
                    "Longitude": getattr(trackpoint, "longitude", None),
                    "Elevation": getattr(trackpoint, "elevation", None),
                    "Distance": getattr(trackpoint, "distance", None),
                    "Heart Rate": getattr(trackpoint, "hr_value", None),
                    "Cadence": getattr(trackpoint, "cadence", None),
                }
            )

    return trackpoints


def create_trackpoints_dataframe(directory):
    all_trackpoints = []

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".tcx"):
            file_path = os.path.join(directory, filename)
            trackpoints = extract_trackpoints_from_tcx(file_path)
            all_trackpoints.extend(trackpoints)

    # Create a DataFrame from the trackpoints list
    df = pd.DataFrame(all_trackpoints)
    return df


# Define the directory containing the TCX files
directory = "data/"

# Create the trackpoints DataFrame
df_trackpoints = create_trackpoints_dataframe(directory)

# Display the DataFrame
df_trackpoints

df_trackpoints.to_csv("processed_data/tcx_dataframe.csv", index=False)
