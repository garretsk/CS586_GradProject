import pandas as pd

input_file = "raw_data/mcc_codes.json"
output_file = "table_data/merchant_categories.csv"

data = open(input_file)

dataframe = pd.DataFrame(columns=["Id", "Category"])

for line in data:
    if not '{' in line and not '}' in line:
        parts = line.split(':')
        id = ''
        for char in parts[0]:
            if char != ',' and char != '"':
                id += char
        category = ''
        for char in parts[1]:
            if char != ',' and char != '"':
                category += char
        dataframe.loc[len(dataframe)] = [id.strip(), category.strip()]

dataframe.sort_values(by='Id', inplace=True)
dataframe.to_csv(output_file, index=False)