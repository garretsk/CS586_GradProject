import pandas as pd

input_file = "raw_data/transactions_data.csv"
input_file_2 = "table_data/states.csv"
output_file = "table_data/cities.csv"

states_reader = pd.read_csv(input_file_2)

cities = set()
chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        for _, row in chunk.iterrows():
            state = row["merchant_state"]
            city = row["merchant_city"]
            result = states_reader.loc[states_reader["State"] == state]
            if not result.empty:
                state_id = result.iloc[0]["Id"]
                cities.add(city + "|" + str(state_id))

cities = list(cities)
city_names = [city.split('|')[0] for city in cities]
state_ids = [city.split('|')[1] for city in cities]

dataframe = pd.DataFrame({"Id": range(len(cities)), "City": city_names, "StateId": state_ids})
dataframe.to_csv(output_file, index=False)