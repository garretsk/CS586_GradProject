import pandas as pd

input_file = "raw_data/transactions_data.csv"
output_file = "table_data/cities.csv"

cities = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        for _, row in chunk.iterrows():
            cities.add(row["merchant_city"])

dataframe = pd.DataFrame(columns=["Id", "City"])
for city in cities:
    dataframe.loc[len(dataframe)] = [len(dataframe), city]

dataframe.to_csv(output_file, index=False)