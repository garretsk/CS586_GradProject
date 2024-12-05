import pandas as pd
import multiprocessing

input_file = "raw_data/transactions_data.csv"
output_file = "table_data/hastransaction.csv"

def process_chunk(chunk):
    print("Reading in CSV - " + multiprocessing.current_process().name + ", PID: " + str(multiprocessing.current_process().pid) + " starting a new chunk")
    rows = []
    for _, row in chunk.iterrows():
        card_id = row["card_id"]
        merchant_id = row["merchant_id"]
        date = row["date"]
        amount = row["amount"]
        use_chip = row["use_chip"]
        zipcode = row["zip"]
        rows.append([card_id, merchant_id, date, amount, use_chip, zipcode])
    chunk_dataframe = pd.DataFrame(rows, columns=["CardId", "MerchantId", "Date", "Amount", "UseChip", "Zip"])
    return chunk_dataframe


if __name__ == "__main__":
    chunk_size = 100000
    with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
        chunks = list(csv_reader)

    with multiprocessing.Pool() as pool:
        print("Beginning CSV processing")
        results = pool.map(process_chunk, chunks)

    dataframe = pd.concat(results, ignore_index=True)
    dataframe.to_csv(output_file, index=False)