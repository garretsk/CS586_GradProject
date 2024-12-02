import pandas as pd

input_file = "raw_data/transactions_data.csv"
output_file = "table_data/hastransaction.csv"

dataframe = pd.DataFrame(columns=["CardId", "MerchantId", "Date", "Amount", "UseChip", "Zip"])

chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        print("Reading in CSV - starting a new chunk")
        for _, row in chunk.iterrows():
            card_id = row["card_id"]
            merchant_id = row["merchant_id"]
            date = row["date"]
            amount = row["amount"]
            use_chip = row["use_chip"]
            zipcode = row["zip"]

            dataframe.loc[len(dataframe)] = [card_id, merchant_id, date, amount, use_chip, zipcode]
            
dataframe.to_csv(output_file, index=False)