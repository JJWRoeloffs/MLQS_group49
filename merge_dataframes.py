import pandas as pd

tcx_df = pd.read_csv("processed_data/tcx_dataframe.csv")
# fit_df = ...

# Merge df's
# merged_df = ...
merged_df = tcx_df

display(tcx_df)

merged_df.to_csv("processed_data/merged_df.csv", index=False)
