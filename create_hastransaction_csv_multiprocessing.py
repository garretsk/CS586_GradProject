import pandas as pd
import multiprocessing

input_file = "raw_data/transactions_data.csv"
output_file = "table_data/hastransaction.csv"

shared_list = multiprocessing.Manager().list()

def process_chunk(chunk):
    print("Reading in CSV - " + multiprocessing.current_process().name + ", PID: " + multiprocessing.current_process().pid + " starting a new chunk")
    chunk_dataframe = pd.DataFrame(columns=["CardId", "MerchantId", "Date", "Amount", "UseChip", "Zip"])
    for _, row in chunk.iterrows():
        card_id = row["card_id"]
        merchant_id = row["merchant_id"]
        date = row["date"]
        amount = row["amount"]
        use_chip = row["use_chip"]
        zipcode = row["zip"]
        chunk_dataframe.loc[len(chunk_dataframe)] = [card_id, merchant_id, date, amount, use_chip, zipcode]
    shared_list.append(chunk_dataframe)


chunk_size = 100000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    with multiprocessing.Pool() as pool:
        pool.map(process_chunk, csv_reader)
        

dataframe = pd.concat(shared_list, ignore_index=True)
dataframe.to_csv(output_file, index=False)