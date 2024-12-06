import pandas as pd

input_file = "raw_data/transactions_data.csv"
output_file = "table_data/states.csv"

states = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        for _, row in chunk.iterrows():
            states.add(row["merchant_state"])

dataframe = pd.DataFrame({"Id": range(len(states)), "State": list(states)})
dataframe.to_csv(output_file, index=False)