# import the pandas library to read csv files
import pandas as pd

# read files in a dataframe df
df = pd.read_csv("../data_modelling_test_tbl_telemetry.csv")
print(len(df))
old = 0
counter = 1
# spilt files in batches od 500K records. This will be helpful to load files incrementally

for i in range(500000, 5800000, 500000):
    new = i
    # files are placed in the data folder with counters
    df[old:new].to_csv("data/data_modelling_test_tbl_telemetry{counter}.csv".format(counter=counter))
    counter += 1
    old = new

# this logic helps save the remaining records
df[i:].to_csv("data/data_modelling_test_tbl_telemetry{counter}.csv".format(counter=counter))


