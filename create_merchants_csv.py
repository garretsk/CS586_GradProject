import pandas as pd

input_file = "raw_data/transactions_data.csv"
input_file_2 = "table_data/states.csv"
input_file_3 = "table_data/cities.csv"
output_file = "table_data/merchants.csv"

states_reader = pd.read_csv(input_file_2)
cities_reader = pd.read_csv(input_file_3)

merchants = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        print("Reading in CSV - starting a new chunk")
        for _, row in chunk.iterrows():
            id = row["merchant_id"]
            merchant_category_id = row["mcc"]
            state = row["merchant_state"]
            city = row["merchant_city"]
            merchants.add(str(id) + "|" + str(merchant_category_id) + "|" + city + "|" + state)

print("Relevant fields extracted. Normalizing...\n")
dataframe = pd.DataFrame(columns=["Id", "MerchantCategoryId", "CityId"])
for merchant in merchants:
    mylist = merchant.split('|')

    city_id = None
    states_result = states_reader.loc[states_reader["State"] == mylist[3]]
    if not states_result.empty:
        state_id = states_result.iloc[0]["Id"]
        cities_result = cities_reader.loc[cities_reader["City"] == mylist[2]]
        if not cities_result.empty:
            for _, row in cities_result.iterrows():
                if row["StateId"] == state_id:
                    city_id = row["Id"]

    print("Creating merchant with ID: " + mylist[0])
    dataframe.loc[len(dataframe)] = [mylist[0], mylist[1], city_id]

print("Data normalized. Printing to output CSV file.")
dataframe.to_csv(output_file, index=False)