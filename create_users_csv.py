import pandas as pd

input_file = "raw_data/users_data.csv"
output_file = "table_data/users.csv"

dataframe = pd.DataFrame(columns=["ID", "CurrentAge", "BirthYear", "BirthMonth", "Gender", "YearlyIncome", "TotalDebt", "CreditScore"])

chunk_size = 10000
with pd.read_csv(input_file, chunksize=chunk_size) as csv_reader:
    for chunk in csv_reader:
        print("Reading in CSV - starting a new chunk")
        for _, row in chunk.iterrows():
            id = row["id"]
            current_age = row["current_age"]
            birth_year = row["birth_year"]
            birth_month = row["birth_month"]
            gender = row["gender"]
            yearly_income = row["yearly_income"]
            total_debt = row["total_debt"]
            credit_score = row["credit_score"]

            dataframe.loc[len(dataframe)] = [id, current_age, birth_year, birth_month, gender, yearly_income, total_debt, credit_score]
            
dataframe.to_csv(output_file, index=False)