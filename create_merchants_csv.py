import pandas as pd

input_file = "raw_data/transactions_data.csv"
input_file_2 = "table_data/states.csv"
input_file_3 = "table_data/cities.csv"
output_file = "table_data/merchants.csv"

states_reader = pd.read_csv(input_file_2)
cities_reader = pd.read_csv(input_file_3)

class merchant:

    def __init__(self, id, merchant_category_id, state, city):
        self.id = id
        self.merchant_category_id = merchant_category_id
        self.state = state
        self.city = city
    
    def get_id(self):
        return self.id
    
    def get_merchant_category_id(self):
        return self.merchant_category_id
    
    def get_state(self):
        return self.state
    
    def get_city(self):
        return self.city
    
    def set_city_id(self, city_id):
        self.city_id = city_id
    
    def get_city_id(self):
        return self.city_id

    def __eq__(self, other):
        return self.id == other.id
    
    def __hash__(self):
        return hash(self.id)

merchants = set()
chunk_size = 100000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        print("Reading in CSV - starting a new chunk")
        for _, row in chunk.iterrows():
            merchants.add(merchant(row["merchant_id"], row["mcc"], row["merchant_state"], row["merchant_city"]))

for merchant in merchants:
    print("Setting city ID for merchant with ID: " + str(merchant.get_id()))
    merchant.set_city_id(None)
    states_result = states_reader.loc[states_reader["State"] == merchant.get_state()]
    if not states_result.empty:
        state_id = states_result.iloc[0]["Id"]
        cities_result = cities_reader.loc[cities_reader["City"] == merchant.get_city()]
        if not cities_result.empty:
            for _, row in cities_result.iterrows():
                if row["StateId"] == state_id:
                    merchant.set_city_id(int(row["Id"]))

print("Splitting into 3 lists for entry in dataframe")
merchants = list(merchants)
merchant_ids = [merchant.get_id() for merchant in merchants]
merchant_category_ids = [merchant.get_merchant_category_id() for merchant in merchants]
merchant_city_ids = [int(merchant.get_city_id()) if merchant.get_city_id() is not None else None for merchant in merchants]

dataframe = pd.DataFrame({"Id": merchant_ids, "MerchantCategoryId": merchant_category_ids, "CityId": merchant_city_ids})
dataframe = dataframe.astype({"Id": "int", "MerchantCategoryId": "int", "CityId": "Int64"})

print("Printing dataframe to output CSV file.")
dataframe.to_csv(output_file, index=False)