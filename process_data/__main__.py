from rawdata_to_csv import main as raw_to_csv
from complete_csv import main as complete
from clean_csv import main as clean
from create_ranges_features import main as add_ranges

raw_to_csv()
complete()
clean()
add_ranges()
