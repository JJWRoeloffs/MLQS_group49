# MLQS_group49

Submissions for the course Machine Learning for the Quantified Self at vu Amsterdam

To pull the data from the submodule, use:

```shell
git pull && git submodule init && git submodule update && git submodule status
```

To run the preprocessing, use:

```shell
python process_data

# Or to run an individual step of the processing, e.g. raw to csv.
python process_data/rawdata_to_csv.py
```
