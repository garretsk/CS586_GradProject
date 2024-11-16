import pandas as pd

input_file = "raw_data/cards_data.csv"
output_file = "table_data/card_brands.csv"

card_brands = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        for _, row in chunk.iterrows():
            card_brands.add(row["card_brand"])

dataframe = pd.DataFrame(columns=["Id", "CardBrand"])
for card_type in card_brands:
    dataframe.loc[len(dataframe)] = [len(dataframe), card_type]

dataframe.to_csv(output_file, index=False)