import pandas as pd

input_file = "raw_data/cards_data.csv"
input_file_2 = "table_data/card_brands.csv"
input_file_3 = "table_data/card_types.csv"
output_file = "table_data/cards.csv"

card_brands_reader = pd.read_csv(input_file_2)
card_types_reader = pd.read_csv(input_file_3)

dataframe = pd.DataFrame(columns=["Id", "UserId", "CardBrand", "CardType", "Expires", "HasChip", "CreditLimit", "AccountOpenDate", "YearPinLastChanged"])

chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        print("Reading in CSV - starting a new chunk")
        for _, row in chunk.iterrows():
            id = row["id"]
            user_id = row["client_id"]
            card_brand = row["card_brand"]
            card_type = row["card_type"]
            expires = row["expires"]
            has_chip = row["has_chip"]
            credit_limit = row["credit_limit"]
            account_open_date = row["acct_open_date"]
            year_pin_last_changed = row["year_pin_last_changed"]

            card_brand_id = None
            card_brands_result = card_brands_reader.loc[card_brands_reader["CardBrand"] == card_brand]
            if not card_brands_result.empty:
                card_brand_id = card_brands_result.iloc[0]["Id"]
            
            card_type_id = None
            card_types_result = card_types_reader.loc[card_types_reader["CardType"] == card_type]
            if not card_types_result.empty:
                card_type_id = card_types_result.iloc[0]["Id"]

            dataframe.loc[len(dataframe)] = [id, user_id, card_brand_id, card_type_id, expires, has_chip, credit_limit, account_open_date, year_pin_last_changed]


dataframe.to_csv(output_file, index=False)