# import the pandas library to read csv files
import pandas as pd

# read files in a dataframe df
df = pd.read_csv("../data_modelling_test_tbl_states.csv")
print(len(df))

# spilt files in batches od 500K records. This will be helpful to load files incrementally
old = 0
counter = 1

for i in range(500000, 4910000, 500000):
    new = i
    # files are placed in the data folder with counters
    df[old:new].to_csv("data/data_modelling_test_tbl_states{counter}.csv".format(counter=counter))
    counter += 1
    old = new

# this logic helps to save the remaining records
df[i:].to_csv("data/data_modelling_test_tbl_states{counter}.csv".format(counter=counter))

