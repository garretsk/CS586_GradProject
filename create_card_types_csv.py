import pandas as pd

input_file = "raw_data/cards_data.csv"
output_file = "table_data/card_types.csv"

card_types = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        for _, row in chunk.iterrows():
            card_types.add(row["card_type"])

dataframe = pd.DataFrame(columns=["Id", "CardType"])
for card_type in card_types:
    dataframe.loc[len(dataframe)] = [len(dataframe), card_type]

dataframe.to_csv(output_file, index=False)